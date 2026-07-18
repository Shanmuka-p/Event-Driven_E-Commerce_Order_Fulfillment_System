const express = require('express');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
const ResilientRabbitMQ = require('./rabbitmq');
const OutboxWorker = require('./outbox');

const PORT = process.env.PORT || 3002;
const DATABASE_URL = process.env.DATABASE_URL;
const RABBITMQ_URL = process.env.RABBITMQ_URL;

const app = express();
app.use(express.json());

const dbPool = new Pool({ connectionString: DATABASE_URL });
const EXCHANGE_NAME = 'ecommerce_events';
const QUEUE_NAME = 'inventory_queue';
const DLX_EXCHANGE = 'dlx_exchange';
const DLQ_NAME = 'dead_letter_queue';

// In-memory retry tracker for testing DLQ
const retryCounts = new Map();

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
  // Assert exchanges
  await channel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });
  await channel.assertExchange(DLX_EXCHANGE, 'topic', { durable: true });
  
  // Assert dead letter queue and bind it
  await channel.assertQueue(DLQ_NAME, { durable: true });
  await channel.bindQueue(DLQ_NAME, DLX_EXCHANGE, 'inventory.failed');

  // Assert main queue with dead letter exchange settings
  await channel.assertQueue(QUEUE_NAME, {
    durable: true,
    arguments: {
      'x-dead-letter-exchange': DLX_EXCHANGE,
      'x-dead-letter-routing-key': 'inventory.failed'
    }
  });
};

async function setupSubscriptions() {
  await rabbitmq.subscribe(QUEUE_NAME, ['payment.processed'], { noAck: false }, async (msg, channel) => {
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
        channel.nack(msg, false, true);
        return;
      } else {
        retryCounts.delete(event.eventId);
        console.error(`Simulating processing failure (attempt ${attempt}/3) for Order ID: ${event.aggregateId}. Routing to DLQ...`);
        // Requeue: false to route to DLQ (causes dead-lettering by RabbitMQ)
        channel.nack(msg, false, false);
        return;
      }
    }

    const client = await dbPool.connect();
    try {
      await client.query('BEGIN');

      // 1. Check Idempotency
      const check = await client.query('SELECT 1 FROM processed_events WHERE event_id = $1 FOR UPDATE', [event.eventId]);
      if (check.rowCount > 0) {
        console.log(`Event ${event.eventId} already processed. Skipping business logic.`);
        await client.query('ROLLBACK');
        channel.ack(msg);
        return;
      }

      // 2. Execute Business Logic (Deduct Inventory & Reserve)
      for (const item of items) {
        const updateRes = await client.query(
          'UPDATE inventory SET stock = stock - $1 WHERE product_id = $2 AND stock >= $1 RETURNING stock',
          [item.quantity, item.productId]
        );

        if (updateRes.rowCount === 0) {
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

      // 4. Save InventoryReserved to Outbox in same transaction
      const nextEventId = uuidv4();
      const nextEventPayload = {
        items
      };

      await client.query(
        'INSERT INTO outbox (id, event_type, aggregate_id, payload, routing_key) VALUES ($1, $2, $3, $4, $5)',
        [
          nextEventId,
          'InventoryReserved',
          event.aggregateId,
          JSON.stringify(nextEventPayload),
          'inventory.reserved'
        ]
      );
      console.log(`Saved InventoryReserved event for Order: ${event.aggregateId} to outbox.`);

      await client.query('COMMIT');
      channel.ack(msg);
    } catch (err) {
      console.error('Error processing PaymentProcessed event, rolling back:', err.message);
      await client.query('ROLLBACK');
      channel.nack(msg, false, true);
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
    console.log(`Inventory Service is listening on port ${PORT}`);
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
  console.error('Failed to start Inventory Service:', err);
  process.exit(1);
});
