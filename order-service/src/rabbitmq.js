const amqp = require('amqplib');
const { EventEmitter } = require('events');

class ResilientRabbitMQ extends EventEmitter {
  constructor(url) {
    super();
    this.url = url;
    this.connection = null;
    this.channel = null;
    this.isClosing = false;
    this.subscriptions = [];
    this.reconnectAttempt = 0;
    this.maxReconnectDelay = 15000;
    this.initialReconnectDelay = 1000;
    this.onConnect = null;
  }

  async connect() {
    if (this.isClosing) return;
    try {
      console.log(`Connecting to RabbitMQ at ${this.url}...`);
      this.connection = await amqp.connect(this.url);
      
      this.connection.on('error', (err) => {
        console.error('RabbitMQ Connection error:', err.message);
      });

      this.connection.on('close', (err) => {
        console.warn('RabbitMQ Connection closed:', err ? err.message : 'no error');
        this.handleDisconnect();
      });

      this.channel = await this.connection.createChannel();
      
      this.channel.on('error', (err) => {
        console.error('RabbitMQ Channel error:', err.message);
      });

      this.channel.on('close', () => {
        console.warn('RabbitMQ Channel closed.');
        this.handleDisconnect();
      });

      console.log('Successfully connected to RabbitMQ and created channel');
      this.reconnectAttempt = 0;

      if (this.onConnect) {
        await this.onConnect(this.channel);
      }

      this.emit('connected');

      await this.restoreSubscriptions();
    } catch (err) {
      console.error('Failed to connect to RabbitMQ:', err.message);
      this.handleDisconnect();
    }
  }

  handleDisconnect() {
    if (this.isClosing) return;
    if (this.connection) {
      this.connection.removeAllListeners();
      this.connection = null;
    }
    if (this.channel) {
      this.channel.removeAllListeners();
      this.channel = null;
    }

    this.emit('disconnected');

    const delay = Math.min(
      this.initialReconnectDelay * Math.pow(2, this.reconnectAttempt),
      this.maxReconnectDelay
    );
    this.reconnectAttempt++;
    console.log(`Scheduling RabbitMQ reconnect in ${delay}ms (attempt ${this.reconnectAttempt})...`);
    setTimeout(() => this.connect(), delay);
  }

  async publish(exchange, routingKey, content, options = {}) {
    if (!this.channel) {
      throw new Error('Cannot publish, RabbitMQ channel is not open');
    }
    return this.channel.publish(exchange, routingKey, content, {
      persistent: true,
      ...options
    });
  }

  async subscribe(queueName, bindings, options, consumerFn) {
    const subscription = { queueName, bindings, options, consumerFn, consumerTag: null };
    this.subscriptions.push(subscription);

    if (this.channel) {
      await this._bindAndConsume(subscription);
    }
  }

  async _bindAndConsume(sub) {
    console.log(`Registering consumer for queue ${sub.queueName}`);
    if (sub.bindings && Array.isArray(sub.bindings)) {
      const exchange = sub.options.exchange || 'ecommerce_events';
      for (const binding of sub.bindings) {
        await this.channel.bindQueue(sub.queueName, exchange, binding);
      }
    }
    
    const { consumerTag } = await this.channel.consume(sub.queueName, async (msg) => {
      if (!msg) return;
      try {
        await sub.consumerFn(msg, this.channel);
      } catch (err) {
        console.error(`Error in consumer logic for queue ${sub.queueName}:`, err);
      }
    }, sub.options);

    sub.consumerTag = consumerTag;
  }

  async restoreSubscriptions() {
    if (this.subscriptions.length === 0) return;
    console.log(`Restoring ${this.subscriptions.length} subscriptions...`);
    for (const sub of this.subscriptions) {
      try {
        await this._bindAndConsume(sub);
      } catch (err) {
        console.error(`Failed to restore subscription for queue ${sub.queueName}:`, err);
      }
    }
  }

  async close() {
    this.isClosing = true;
    console.log('Closing RabbitMQ connection...');
    if (this.channel) {
      try {
        await this.channel.close();
      } catch (err) {
        // ignore if already closed
      }
      this.channel = null;
    }
    if (this.connection) {
      try {
        await this.connection.close();
      } catch (err) {
        // ignore
      }
      this.connection = null;
    }
  }
}

module.exports = ResilientRabbitMQ;
