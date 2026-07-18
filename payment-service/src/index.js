const express = require('express');
const { Pool } = require('pg');
const { v4: uuidv4 } = require('uuid');
const ResilientRabbitMQ = require('./rabbitmq');
const OutboxWorker = require('./outbox');

const PORT = process.env.PORT || 3001;
const DATABASE_URL = process.env.DATABASE_URL;
const RABBITMQ_URL = process.env.RABBITMQ_URL;

const app = express();
app.use(express.json());

const dbPool = new Pool({ connectionString: DATABASE_URL });
const EXCHANGE_NAME = 'ecommerce_events';
const QUEUE_NAME = 'payment_queue';

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
  await rabbitmq.subscribe(QUEUE_NAME, ['order.created'], { noAck: false }, async (msg, channel) => {
    const client = await dbPool.connect();
    try {
      const event = JSON.parse(msg.content.toString());
      console.log(`Received OrderCreated event: ${event.eventId} for Order ID: ${event.aggregateId}`);

      await client.query('BEGIN');

      // 1. Check Idempotency
      const check = await client.query('SELECT 1 FROM processed_events WHERE event_id = $1 FOR UPDATE', [event.eventId]);
      if (check.rowCount > 0) {
        console.log(`Event ${event.eventId} already processed. Skipping business logic.`);
        await client.query('ROLLBACK');
        channel.ack(msg);
        return;
      }

      // 2. Execute Business Logic (Simulated Payment)
      const paymentId = uuidv4();
      const amount = event.payload.totalPrice;
      const status = amount > 0 ? 'SUCCESS' : 'FAILED';

      await client.query(
        'INSERT INTO payments (id, order_id, amount, status) VALUES ($1, $2, $3, $4)',
        [paymentId, event.aggregateId, amount, status]
      );

      // 3. Mark Event as Processed (in the same transaction)
      await client.query('INSERT INTO processed_events (event_id) VALUES ($1)', [event.eventId]);

      // 4. Save PaymentProcessed to Outbox if payment is successful
      if (status === 'SUCCESS') {
        const nextEventId = uuidv4();
        const nextEventPayload = {
          paymentId,
          amount,
          status,
          items: event.payload.items
        };

        await client.query(
          'INSERT INTO outbox (id, event_type, aggregate_id, payload, routing_key) VALUES ($1, $2, $3, $4, $5)',
          [
            nextEventId,
            'PaymentProcessed',
            event.aggregateId,
            JSON.stringify(nextEventPayload),
            'payment.processed'
          ]
        );
        console.log(`Saved PaymentProcessed event for Order: ${event.aggregateId} to outbox.`);
      } else {
        console.log(`Payment failed for Order ID: ${event.aggregateId}. Outbox entry not created.`);
      }

      await client.query('COMMIT');
      channel.ack(msg);
    } catch (err) {
      console.error('Error processing OrderCreated event, rolling back:', err.message);
      await client.query('ROLLBACK');
      // Requeue for retry
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
    console.log(`Payment Service is listening on port ${PORT}`);
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
  console.error('Failed to start Payment Service:', err);
  process.exit(1);
});
