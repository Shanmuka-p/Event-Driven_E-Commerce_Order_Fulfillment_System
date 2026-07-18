class OutboxWorker {
  constructor(dbPool, rabbitMQ, exchangeName, intervalMs = 1000) {
    this.dbPool = dbPool;
    this.rabbitmq = rabbitMQ;
    this.exchangeName = exchangeName;
    this.intervalMs = intervalMs;
    this.timer = null;
    this.isRunning = false;
  }

  start() {
    if (this.isRunning) return;
    this.isRunning = true;
    console.log(`Starting Outbox Worker...`);
    this.timer = setTimeout(() => this.poll(), this.intervalMs);
  }

  async poll() {
    if (!this.isRunning) return;

    if (!this.rabbitmq.channel) {
      this.timer = setTimeout(() => this.poll(), this.intervalMs);
      return;
    }

    let client;
    try {
      client = await this.dbPool.connect();
      await client.query('BEGIN');

      const res = await client.query(
        'SELECT * FROM outbox WHERE processed = FALSE ORDER BY created_at ASC LIMIT 10 FOR UPDATE SKIP LOCKED'
      );

      if (res.rows.length > 0) {
        console.log(`Outbox Worker found ${res.rows.length} unprocessed events.`);
        for (const row of res.rows) {
          const event = {
            eventId: row.id,
            eventType: row.event_type,
            timestamp: row.created_at.toISOString(),
            aggregateId: row.aggregate_id,
            payload: typeof row.payload === 'string' ? JSON.parse(row.payload) : row.payload
          };

          try {
            await this.rabbitmq.publish(
              this.exchangeName,
              row.routing_key,
              Buffer.from(JSON.stringify(event)),
              { persistent: true }
            );

            await client.query(
              'UPDATE outbox SET processed = TRUE, processed_at = NOW() WHERE id = $1',
              [row.id]
            );
            console.log(`Successfully published outbox event ${row.id} (${row.event_type}) for aggregate ${row.aggregate_id}`);
          } catch (pubErr) {
            console.error(`Failed to publish event ${row.id}:`, pubErr.message);
            throw pubErr;
          }
        }
      }
      await client.query('COMMIT');
    } catch (err) {
      console.error('Error during outbox polling:', err.message);
      if (client) {
        try {
          await client.query('ROLLBACK');
        } catch (rollbackErr) {
          // ignore
        }
      }
    } finally {
      if (client) {
        client.release();
      }
      if (this.isRunning) {
        this.timer = setTimeout(() => this.poll(), this.intervalMs);
      }
    }
  }

  stop() {
    this.isRunning = false;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    console.log('Outbox Worker stopped.');
  }
}

module.exports = OutboxWorker;
