const express = require('express');
const { Pool } = require('pg');
const amqp = require('amqplib');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 3001;
const DATABASE_URL = process.env.DATABASE_URL;
const RABBITMQ_URL = process.env.RABBITMQ_URL;

const app = express();
app.use(express.json());

let dbPool;
let mqConnection;
let mqChannel;

const EXCHANGE_NAME = 'ecommerce_events';
const QUEUE_NAME = 'payment_queue';

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
  await mqChannel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, 'order.created');

  // Consume OrderCreated events
  mqChannel.consume(QUEUE_NAME, async (msg) => {
    if (!msg) return;

    const client = await dbPool.connect();
    try {
      const event = JSON.parse(msg.content.toString());
      console.log(`Received OrderCreated event: ${event.eventId} for Order ID: ${event.aggregateId}`);

      await client.query('BEGIN');

      // 1. Check Idempotency
      const check = await client.query('SELECT 1 FROM processed_events WHERE event_id = $1', [event.eventId]);
      if (check.rowCount > 0) {
        console.log(`Event ${event.eventId} already processed. Skipping business logic.`);
        await client.query('ROLLBACK');
        mqChannel.ack(msg);
        return;
      }

      // 2. Execute Business Logic (Simulated Payment)
      const paymentId = uuidv4();
      const amount = event.payload.totalPrice;
      const status = amount > 0 ? 'SUCCESS' : 'FAILED'; // simulate success for positive amount

      await client.query(
        'INSERT INTO payments (id, order_id, amount, status) VALUES ($1, $2, $3, $4)',
        [paymentId, event.aggregateId, amount, status]
      );

      // 3. Mark Event as Processed (in the same transaction)
      await client.query('INSERT INTO processed_events (event_id) VALUES ($1)', [event.eventId]);

      await client.query('COMMIT');

      // 4. Publish next event (PaymentProcessed)
      if (status === 'SUCCESS') {
        const nextEvent = {
          eventId: uuidv4(),
          eventType: 'PaymentProcessed',
          timestamp: new Date().toISOString(),
          aggregateId: event.aggregateId, // orderId
          payload: {
            paymentId,
            amount,
            status,
            items: event.payload.items // pass items downstream for Inventory Service
          }
        };

        mqChannel.publish(EXCHANGE_NAME, 'payment.processed', Buffer.from(JSON.stringify(nextEvent)), {
          persistent: true
        });
        console.log(`Published PaymentProcessed event for Order ID: ${event.aggregateId}`);
      } else {
        console.log(`Payment failed for Order ID: ${event.aggregateId}. PaymentProcessed event not published.`);
      }

      // 5. Acknowledge message
      mqChannel.ack(msg);
    } catch (err) {
      console.error('Error processing OrderCreated event, rolling back:', err);
      await client.query('ROLLBACK');
      // Do not ack. Requeue so it can retry
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
    console.log(`Payment Service is listening on port ${PORT}`);
  });
}

start().catch(err => {
  console.error('Failed to start Payment Service:', err);
  process.exit(1);
});
