const express = require('express');
const amqp = require('amqplib');

const PORT = process.env.PORT || 3004;
const RABBITMQ_URL = process.env.RABBITMQ_URL;

const app = express();
app.use(express.json());

let mqConnection;
let mqChannel;

const EXCHANGE_NAME = 'ecommerce_events';
const QUEUE_NAME = 'notification_queue';

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

  const bindings = ['order.created', 'payment.processed', 'inventory.reserved', 'shipment.created'];
  for (const binding of bindings) {
    await mqChannel.bindQueue(QUEUE_NAME, EXCHANGE_NAME, binding);
  }

  // Consume events
  mqChannel.consume(QUEUE_NAME, (msg) => {
    if (!msg) return;

    try {
      const event = JSON.parse(msg.content.toString());
      console.log(`Notification sent for event: ${event.eventType}, Order ID: ${event.aggregateId}`);
      mqChannel.ack(msg);
    } catch (err) {
      console.error('Error processing notification event:', err);
      // Nack and requeue notifications
      mqChannel.nack(msg, false, true);
    }
  });
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'UP' });
});

async function start() {
  await connectMq();
  await setupMq();

  app.listen(PORT, () => {
    console.log(`Notification Service is listening on port ${PORT}`);
  });
}

start().catch(err => {
  console.error('Failed to start Notification Service:', err);
  process.exit(1);
});
