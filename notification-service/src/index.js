const express = require('express');
const ResilientRabbitMQ = require('./rabbitmq');

const PORT = process.env.PORT || 3004;
const RABBITMQ_URL = process.env.RABBITMQ_URL;

const app = express();
app.use(express.json());

const EXCHANGE_NAME = 'ecommerce_events';
const QUEUE_NAME = 'notification_queue';

const rabbitmq = new ResilientRabbitMQ(RABBITMQ_URL);

rabbitmq.onConnect = async (channel) => {
  await channel.assertExchange(EXCHANGE_NAME, 'topic', { durable: true });
  await channel.assertQueue(QUEUE_NAME, { durable: true });
};

async function setupSubscriptions() {
  const bindings = ['order.created', 'payment.processed', 'inventory.reserved', 'shipment.created'];
  
  await rabbitmq.subscribe(QUEUE_NAME, bindings, { noAck: false }, async (msg, channel) => {
    try {
      const event = JSON.parse(msg.content.toString());
      console.log(`Notification sent for event: ${event.eventType}, Order ID: ${event.aggregateId}`);
      channel.ack(msg);
    } catch (err) {
      console.error('Error processing notification event:', err.message);
      // Requeue notification event for retry
      channel.nack(msg, false, true);
    }
  });
}

// Health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'UP' });
});

async function start() {
  rabbitmq.on('connected', () => {
    console.log('RabbitMQ connection established for Notification Service');
  });

  await rabbitmq.connect();
  await setupSubscriptions();

  const server = app.listen(PORT, () => {
    console.log(`Notification Service is listening on port ${PORT}`);
  });

  // Graceful Shutdown Handler
  const shutdown = async (signal) => {
    console.log(`Received ${signal}. Starting graceful shutdown...`);
    server.close(() => {
      console.log('HTTP server closed.');
    });

    await rabbitmq.close();
    console.log('Graceful shutdown completed successfully. Exiting.');
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
}

start().catch(err => {
  console.error('Failed to start Notification Service:', err);
  process.exit(1);
});
