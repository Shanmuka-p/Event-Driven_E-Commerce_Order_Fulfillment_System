const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 3002;
const DATABASE_URL = process.env.DATABASE_URL;
const RABBITMQ_URL = process.env.RABBITMQ_URL;

const app = express();
app.use(express.json());

let dbPool;
let mqConnection;
let mqChannel;

const EXCHANGE_NAME = 'ecommerce_events';
const QUEUE_NAME = 'inventory_queue';
const DLX_EXCHANGE = 'dlx_exchange';
const DLQ_NAME = 'dead_letter_queue';

// In-memory retry tracker for testing DLQ
const retryCounts = new Map();

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
  // Main Exchange
  await mqChannel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });

  // Dead Letter Exchange and Queue
  await mqChannel.assertExchange(DLX_EXCHANGE, 'topic', { durable: true });
  await mqChannel.assertQueue(DLQ_NAME, { durable: true });
  await mqChannel.bindQueue(DLQ_NAME, DLX_EXCHANGE, 'inventory.failed');

  // Main Queue with DLQ arguments
  await mqChannel.assertQueue(QUEUE_NAME, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': DLX_EXCHANGE,
      'x-dead-letter-routing-key': 'inventory.failed'
    }
  });

  await mqChannel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, 'payment.processed');

  // Consume PaymentProcessed events
  mqChannel.consume(QUEUE_NAME, async (msg) => {
    if (!msg) return;

    const event = JSON.parse(msg.content.toString());
    console.log(`Received PaymentProcessed event: ${event.eventId} for Order ID: ${event.aggregateId}`);

    // Check if the order contains a 'FAIL-ME' item to simulate failures
    const items = event.payload.items || [];
    const hasFailItem = items.some(item => item.productId === 'FAIL-ME');

    if (hasFailItem) {
      const attempt = (retryCounts.get(event.eventId) || 0) + 1;
      if (attempt < 3) {
        retryCounts.set(event.eventId, attempt);
        console.warn(`Simulating processing failure (attempt ${attempt}/3) for Order ID: ${event.aggregateId}. Requeueing...`);
        // Requeue: true to retry
        mqChannel.nack(msg, false, true);
        return;
      } else {
        retryCounts.delete(event.eventId);
        console.error(`Simulating processing failure (attempt ${attempt}/3) for Order ID: ${event.aggregateId}. Routing to DLQ...`);
        // Requeue: false to route to DLQ
        mqChannel.nack(msg, false, false);
        return;
      }
    }

    const client = await dbPool.connect();
    try {
      await client.query('BEGIN');

      // 1. Check Idempotency
      const check = await client.query('SELECT 1 FROM processed_events WHERE event_id = $1', [event.eventId]);
      if (check.rowCount > 0) {
        console.log(`Event ${event.eventId} already processed. Skipping business logic.`);
        await client.query('ROLLBACK');
        mqChannel.ack(msg);
        return;
      }

      // 2. Execute Business Logic (Deduct Inventory & Reserve)
      for (const item of items) {
        const updateRes = await client.query(
          'UPDATE inventory SET stock = stock - $1 WHERE product_id = $2 AND stock >= $1 RETURNING stock',
          [item.quantity, item.productId]
        );

        if (updateRes.rowCount === 0) {
          // Log a warning if stock is low, but create reservation anyway for fulfillment simulation
          console.warn(`Insufficient stock for product ${item.productId}, fulfilling anyway...`);
        }

        const reservationId = uuidv4();
        await client.query(
          'INSERT INTO reservations (id, order_id, product_id, quantity) VALUES ($1, $2, $3, $4)',
          [reservationId, event.aggregateId, item.productId, item.quantity]
        );
      }

      // 3. Mark Event as Processed (in the same transaction)
      await client.query('INSERT INTO processed_events (event_id) VALUES ($1)', [event.eventId]);

      await client.query('COMMIT');

      // 4. Publish next event (InventoryReserved)
      const nextEvent = {
        eventId: uuidv4(),
        eventType: 'InventoryReserved',
        timestamp: new Date().toISOString(),
        aggregateId: event.aggregateId, // orderId
        payload: {
          items
        }
      };

      mqChannel.publish(EXCHANGE_NAME, 'inventory.reserved', Buffer.from(JSON.stringify(nextEvent)), {
        persistent: true
      });
      console.log(`Published InventoryReserved event for Order ID: ${event.aggregateId}`);

      // 5. Acknowledge message
      mqChannel.ack(msg);
    } catch (err) {
      console.error('Error processing PaymentProcessed event, rolling back:', err);
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
    console.log(`Inventory Service is listening on port ${PORT}`);
  });
}

start().catch(err => {
  console.error('Failed to start Inventory Service:', err);
  process.exit(1);
});
