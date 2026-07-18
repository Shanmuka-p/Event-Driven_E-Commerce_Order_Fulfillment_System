const express = require('express');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
const ResilientRabbitMQ = require('./rabbitmq');
const OutboxWorker = require('./outbox');

const PORT = process.env.PORT || 3000;
const DATABASE_URL = process.env.DATABASE_URL;
const RABBITMQ_URL = process.env.RABBITMQ_URL;

const app = express();
app.use(express.json());

const dbPool = new Pool({ connectionString: DATABASE_URL });
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

const rabbitmq = new ResilientRabbitMQ(RABBITMQ_URL);
const outboxWorker = new OutboxWorker(dbPool, rabbitmq, EXCHANGE_NAME, 500);

rabbitmq.onConnect = async (channel) => {
  await channel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });
  await channel.assertQueue(QUEUE_NAME, { durable: true });
};

async function setupSubscriptions() {
  const statusBindings = ['payment.processed', 'inventory.reserved', 'shipment.created'];
  
  await rabbitmq.subscribe(QUEUE_NAME, statusBindings, { noAck: false }, async (msg, channel) => {
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

      channel.ack(msg);
    } catch (err) {
      console.error('Error processing status update event:', err.message);
      // Requeue status update for retry
      channel.nack(msg, false, true);
    }
  });
}

// POST /api/orders
app.post('/api/orders', async (req, res) => {
  const { items, totalPrice } = req.body;

  // Strict Request Validation
  if (!items || !Array.isArray(items) || items.length === 0) {
    return res.status(400).json({ error: 'items must be a non-empty array' });
  }
  if (totalPrice === undefined || typeof totalPrice !== 'number' || totalPrice <= 0) {
    return res.status(400).json({ error: 'totalPrice must be a positive number' });
  }

  let calculatedTotal = 0;
  for (const item of items) {
    if (!item.productId || typeof item.productId !== 'string' || item.productId.trim() === '') {
      return res.status(400).json({ error: 'Each item must have a valid productId' });
    }
    if (!item.quantity || !Number.isInteger(item.quantity) || item.quantity <= 0) {
      return res.status(400).json({ error: 'Each item must have a positive integer quantity' });
    }
    if (item.price === undefined || typeof item.price !== 'number' || item.price <= 0) {
      return res.status(400).json({ error: 'Each item must have a positive price' });
    }
    calculatedTotal += item.quantity * item.price;
  }

  // Prevent price tempering
  if (Math.abs(calculatedTotal - totalPrice) > 0.01) {
    return res.status(400).json({ 
      error: `totalPrice (${totalPrice}) does not match the sum of item totals (${calculatedTotal.toFixed(2)})` 
    });
  }

  const orderId = uuidv4();

  const client = await dbPool.connect();
  try {
    await client.query('BEGIN');

    // 1. Save order to local database
    await client.query(
      'INSERT INTO orders (id, items, total_price, status) VALUES ($1, $2, $3, $4)',
      [orderId, JSON.stringify(items), totalPrice, 'PENDING']
    );

    // 2. Insert event to Outbox table within the same transaction
    const eventId = uuidv4();
    const eventPayload = {
      items,
      totalPrice
    };

    await client.query(
      'INSERT INTO outbox (id, event_type, aggregate_id, payload, routing_key) VALUES ($1, $2, $3, $4, $5)',
      [eventId, 'OrderCreated', orderId, JSON.stringify(eventPayload), 'order.created']
    );

    await client.query('COMMIT');
    console.log(`Transaction committed for Order ID: ${orderId}. Saved to Outbox.`);
    
    // Return 202 Accepted immediately
    return res.status(202).json({ orderId });
  } catch (err) {
    console.error('Failed to create order:', err);
    await client.query('ROLLBACK');
    return res.status(500).json({ error: 'Internal server error' });
  } finally {
    client.release();
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
  
  rabbitmq.on('connected', () => {
    console.log('RabbitMQ connection established, starting Outbox worker...');
    outboxWorker.start();
  });
  
  rabbitmq.on('disconnected', () => {
    console.log('RabbitMQ disconnected, stopping Outbox worker...');
    outboxWorker.stop();
  });

  await rabbitmq.connect();
  await setupSubscriptions();

  const server = app.listen(PORT, () => {
    console.log(`Order Service is listening on port ${PORT}`);
  });

  // Graceful Shutdown Handler
  const shutdown = async (signal) => {
    console.log(`Received ${signal}. Starting graceful shutdown...`);
    server.close(() => {
      console.log('HTTP server closed.');
    });
    
    outboxWorker.stop();
    await rabbitmq.close();
    
    if (dbPool) {
      await dbPool.end();
      console.log('PostgreSQL connection pool closed.');
    }
    
    console.log('Graceful shutdown completed successfully. Exiting.');
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

start().catch(err => {
  console.error('Failed to start Order Service:', err);
  process.exit(1);
});
