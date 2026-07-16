const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL;
const RABBITMQ_URL = process.env.RABBITMQ_URL;

const app = express();
app.use(express.json());

let dbPool;
let mqConnection;
let mqChannel;

const EXCHANGE_NAME = 'ecommerce_events';
const QUEUE_NAME = 'order_status_queue';

const statusRank = {
  'PENDING': 0,
  'PAID': 1,
  'INVENTORY_RESERVED': 2,
  'SHIPPED': 3
};

// Connect to Database with retries
async function connectDb() {
  dbPool = new Pool({ connectionString: DATABASE_URL });
  for (let i = 0; i < 15; i++) {
    try {
      await dbPool.query('SELECT 1');
      console.log('Successfully connected to PostgreSQL');
      return;
    } catch (err) {
      console.log(`Database connection failed, retrying in 2 seconds (${i + 1}/15)...`);
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }
  throw new Error('Could not connect to PostgreSQL');
}

// Connect to RabbitMQ with retries
async function connectMq() {
  for (let i = 0; i < 15; i++) {
    try {
      mqConnection = await amqp.connect(RABBITMQ_URL);
      mqChannel = await mqConnection.createChannel();
      console.log('Successfully connected to RabbitMQ');
      return;
    } catch (err) {
      console.log(`RabbitMQ connection failed, retrying in 2 seconds (${i + 1}/15)...`);
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }
  throw new Error('Could not connect to RabbitMQ');
}

// Setup RabbitMQ Topology
async function setupMq() {
  await mqChannel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });
  await mqChannel.assertQueue(QUEUE_NAME, { durable: true });

  // Bind status updates queue
  const statusBindings = ['payment.processed', 'inventory.reserved', 'shipment.created'];
  for (const binding of statusBindings) {
    await mqChannel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, binding);
  }

  // Consume status updates
  mqChannel.consume(QUEUE_NAME, async (msg) => {
    if (!msg) return;

    try {
      const event = JSON.parse(msg.content.toString());
      console.log(`Received status update event: ${event.eventType} for Order ID: ${event.aggregateId}`);

      let nextStatus;
      if (event.eventType === 'PaymentProcessed') {
        nextStatus = 'PAID';
      } else if (event.eventType === 'InventoryReserved') {
        nextStatus = 'INVENTORY_RESERVED';
      } else if (event.eventType === 'ShipmentCreated') {
        nextStatus = 'SHIPPED';
      }

      if (nextStatus) {
        // Idempotent/ordered status update
        const client = await dbPool.connect();
        try {
          await client.query('BEGIN');
          const res = await client.query('SELECT status FROM orders WHERE id = $1 FOR UPDATE', [event.aggregateId]);
          if (res.rowCount > 0) {
            const currentStatus = res.rows[0].status;
            const currentRank = statusRank[currentStatus] !== undefined ? statusRank[currentStatus] : -1;
            const nextRank = statusRank[nextStatus];

            if (nextRank > currentRank) {
              await client.query('UPDATE orders SET status = $1 WHERE id = $2', [nextStatus, event.aggregateId]);
              console.log(`Updated Order: ${event.aggregateId} status from ${currentStatus} to ${nextStatus}`);
            } else {
              console.log(`Ignoring status update to ${nextStatus} for Order: ${event.aggregateId} as current status is ${currentStatus}`);
            }
          } else {
            console.log(`Order ${event.aggregateId} not found in database for update.`);
          }
          await client.query('COMMIT');
        } catch (err) {
          await client.query('ROLLBACK');
          throw err;
        } finally {
          client.release();
        }
      }

      mqChannel.ack(msg);
    } catch (err) {
      console.error('Error processing status update event:', err);
      // Nack and requeue status updates for transient errors
      mqChannel.nack(msg, false, true);
    }
  });
}

// POST /api/orders
app.post('/api/orders', async (req, res) => {
  const { items, totalPrice } = req.body;

  if (!items || !Array.isArray(items) || items.length === 0) {
    return res.status(400).json({ error: 'Items must be a non-empty array' });
  }
  if (totalPrice === undefined || typeof totalPrice !== 'number') {
    return res.status(400).json({ error: 'totalPrice must be a number' });
  }

  const orderId = uuidv4();

  try {
    // 1. Save order to local database
    await dbPool.query(
      'INSERT INTO orders (id, items, total_price, status) VALUES ($1, $2, $3, $4)',
      [orderId, JSON.stringify(items), totalPrice, 'PENDING']
    );

    // 2. Build standardized event
    const event = {
      eventId: uuidv4(),
      eventType: 'OrderCreated',
      timestamp: new Date().toISOString(),
      aggregateId: orderId,
      payload: {
        items,
        totalPrice
      }
    };

    // 3. Publish OrderCreated
    mqChannel.publish(EXCHANGE_NAME, 'order.created', Buffer.from(JSON.stringify(event)), {
      persistent: true
    });

    console.log(`Published OrderCreated event for Order ID: ${orderId}`);

    // 4. Return 202 Accepted
    return res.status(202).json({ orderId });
  } catch (err) {
    console.error('Failed to create order:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// GET /api/orders/:orderId
app.get('/api/orders/:orderId', async (req, res) => {
  const { orderId } = req.params;

  try {
    const dbRes = await dbPool.query('SELECT * FROM orders WHERE id = $1', [orderId]);
    if (dbRes.rowCount === 0) {
      return res.status(404).json({ error: 'Order not found' });
    }

    const order = dbRes.rows[0];
    return res.status(200).json({
      orderId: order.id,
      status: order.status,
      items: typeof order.items === 'string' ? JSON.parse(order.items) : order.items,
      totalPrice: parseFloat(order.total_price),
      createdAt: order.created_at
    });
  } catch (err) {
    console.error('Failed to retrieve order:', err);
    return res.status(500).json({ error: 'Internal server error' });
  }
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'UP' });
});

async function start() {
  await connectDb();
  await connectMq();
  await setupMq();

  app.listen(PORT, () => {
    console.log(`Order Service is listening on port ${PORT}`);
  });
}

start().catch(err => {
  console.error('Failed to start Order Service:', err);
  process.exit(1);
});
