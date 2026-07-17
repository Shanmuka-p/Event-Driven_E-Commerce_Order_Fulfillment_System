const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 3003;
const DATABASE_URL = process.env.DATABASE_URL;
const RABBITMQ_URL = process.env.RABBITMQ_URL;

const app = express();
app.use(express.json());

let dbPool;
let mqConnection;
let mqChannel;

const EXCHANGE_NAME = 'ecommerce_events';
const QUEUE_NAME = 'shipping_queue';

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
  await mqChannel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, 'inventory.reserved');

  // Consume InventoryReserved events
  mqChannel.consume(QUEUE_NAME, async (msg) => {
    if (!msg) return;

    const client = await dbPool.connect();
    try {
      const event = JSON.parse(msg.content.toString());
      console.log(`Received InventoryReserved event: ${event.eventId} for Order ID: ${event.aggregateId}`);

      await client.query('BEGIN');

      // 1. Check Idempotency
      const check = await client.query('SELECT 1 FROM processed_events WHERE event_id = $1', [event.eventId]);
      if (check.rowCount > 0) {
        console.log(`Event ${event.eventId} already processed. Skipping business logic.`);
        await client.query('ROLLBACK');
        mqChannel.ack(msg);
        return;
      }

      // 2. Execute Business Logic (Generate Shipment and Tracking)
      const shipmentId = uuidv4();
      const trackingNumber = 'TRK' + Math.floor(100000000 + Math.random() * 900000000);
      const status = 'SHIPPED';

      await client.query(
        'INSERT INTO shipments (id, order_id, tracking_number, status) VALUES ($1, $2, $3, $4)',
        [shipmentId, event.aggregateId, trackingNumber, status]
      );

      // 3. Mark Event as Processed (in the same transaction)
      await client.query('INSERT INTO processed_events (event_id) VALUES ($1)', [event.eventId]);

      await client.query('COMMIT');

      // 4. Publish next event (ShipmentCreated)
      const nextEvent = {
        eventId: uuidv4(),
        eventType: 'ShipmentCreated',
        timestamp: new Date().toISOString(),
        aggregateId: event.aggregateId, // orderId
        payload: {
          shipmentId,
          trackingNumber,
          status
        }
      };

      mqChannel.publish(EXCHANGE_NAME, 'shipment.created', Buffer.from(JSON.stringify(nextEvent)), {
        persistent: true
      });
      console.log(`Published ShipmentCreated event for Order ID: ${event.aggregateId}`);

      // 5. Acknowledge message
      mqChannel.ack(msg);
    } catch (err) {
      console.error('Error processing InventoryReserved event, rolling back:', err);
      await client.query('ROLLBACK');
      mqChannel.nack(msg, false, true);
    } finally {
      client.release();
    }
  });
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'UP' });
});

async function start() {
  await connectDb();
  await connectMq();
  await setupMq();

  app.listen(PORT, () => {
    console.log(`Shipping Service is listening on port ${PORT}`);
  });
}

start().catch(err => {
  console.error('Failed to start Shipping Service:', err);
  process.exit(1);
});
