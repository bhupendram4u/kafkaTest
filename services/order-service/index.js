import express from 'express';
import pg from 'pg';
import { producer, createConsumer, connectProducer } from './shared/kafkaClient/index.js';
import logger from './shared/logger/index.js';
import RetryHandler from './shared/retryHandler/index.js';

const { Pool } = pg;
const consumer = createConsumer('order-service-group');
const orderLogger = logger('order-service');
const app = express();
app.use(express.json());

const pool = new Pool({
  host: process.env.POSTGRES_HOST || 'localhost',
  port: process.env.POSTGRES_PORT || 5432,
  database: process.env.POSTGRES_DB || 'ecommerce',
  user: process.env.POSTGRES_USER || 'user',
  password: process.env.POSTGRES_PASSWORD || 'password'
});

const retryHandler = new RetryHandler();

// Topics
const TOPICS = {
  ORDER_CREATED: 'order_created',
  PAYMENT_SUCCESS: 'payment_success',
  PAYMENT_FAILED: 'payment_failed',
  INVENTORY_RESERVED: 'inventory_reserved',
  INVENTORY_FAILED: 'inventory_failed',
  ORDER_CONFIRMED: 'order_confirmed',
  ORDER_CANCELLED: 'order_cancelled'
};

// POST /orders
app.post('/orders', async (req, res) => {
  const { orderId, amount } = req.body;

  try {
    // Insert order and only create a new order if it does not exist yet
    const result = await pool.query(
      'INSERT INTO orders (id, amount, status) VALUES ($1, $2, $3) ON CONFLICT (id) DO NOTHING RETURNING id',
      [orderId, amount, 'pending']
    );

    if (result.rowCount === 0) {
      orderLogger.info({
        orderId,
        event: 'order_created',
        status: 'SKIPPED',
        message: 'Duplicate order request ignored'
      });
      return res.status(200).json({ message: 'Order already exists', orderId });
    }

    // Insert outbox event
    await pool.query(
      'INSERT INTO outbox_events (event_type, payload) VALUES ($1, $2)',
      [TOPICS.ORDER_CREATED, JSON.stringify({ orderId, amount })]
    );

    orderLogger.info({
      orderId,
      event: 'order_created',
      status: 'STARTED',
      message: 'Order created and event queued'
    });

    res.status(201).json({ message: 'Order created', orderId });
  } catch (error) {
    orderLogger.error({
      orderId,
      event: 'order_created',
      status: 'FAILED',
      reason: error.message
    });
    res.status(500).json({ error: error.message });
  }
});

// Outbox worker
async function processOutbox() {
  const client = await pool.connect();
  try {
    const result = await client.query(
      'SELECT * FROM outbox_events WHERE status = $1 ORDER BY created_at ASC LIMIT 10',
      ['pending']
    );

    for (const event of result.rows) {
      await producer.send({
        topic: event.event_type,
        messages: [{ key: event.payload.orderId, value: JSON.stringify(event.payload) }]
      });

      await client.query(
        'UPDATE outbox_events SET status = $1 WHERE id = $2',
        ['processed', event.id]
      );

      orderLogger.info({
        orderId: event.payload.orderId,
        event: event.event_type,
        status: 'PUBLISHED',
        message: 'Event published from outbox'
      });
    }
  } catch (error) {
    orderLogger.error({
      event: 'outbox_processing',
      status: 'FAILED',
      reason: error.message
    });
  } finally {
    client.release();
  }
}

// Consumer handlers
const handlePaymentSuccess = async (event) => {
  await pool.query('UPDATE orders SET status = $1 WHERE id = $2', ['paid', event.orderId]);
  // Inventory service will handle next step
};

const handlePaymentFailed = async (event) => {
  await pool.query('UPDATE orders SET status = $1 WHERE id = $2', ['cancelled', event.orderId]);
  await producer.send({
    topic: TOPICS.ORDER_CANCELLED,
    messages: [{ key: event.orderId, value: JSON.stringify(event) }]
  });
};

const handleInventoryReserved = async (event) => {
  await pool.query('UPDATE orders SET status = $1 WHERE id = $2', ['confirmed', event.orderId]);
  await producer.send({
    topic: TOPICS.ORDER_CONFIRMED,
    messages: [{ key: event.orderId, value: JSON.stringify(event) }]
  });
};

const handleInventoryFailed = async (event) => {
  await pool.query('UPDATE orders SET status = $1 WHERE id = $2', ['cancelled', event.orderId]);
  await producer.send({
    topic: TOPICS.ORDER_CANCELLED,
    messages: [{ key: event.orderId, value: JSON.stringify(event) }]
  });
};

// Start services
async function start() {
  await connectProducer();
  await consumer.connect();

  // Subscribe to topics
  await consumer.subscribe({ topic: TOPICS.PAYMENT_SUCCESS });
  await consumer.subscribe({ topic: TOPICS.PAYMENT_FAILED });
  await consumer.subscribe({ topic: TOPICS.INVENTORY_RESERVED });
  await consumer.subscribe({ topic: TOPICS.INVENTORY_FAILED });

  app.listen(3001, () => {
    orderLogger.info({ message: 'Order Service started on port 3001' });
  });

  // Start outbox worker
  setInterval(processOutbox, 5000);

  // Run consumer without blocking the rest of the startup flow
  consumer.run({
    eachMessage: async ({ topic, message }) => {
      console.log(`##### Received message on topic ${topic}: ${message.value.toString()}`);
      const event = JSON.parse(message.value.toString());
      event.event = topic;
      const operation = () => {
        switch (topic) {
          case TOPICS.PAYMENT_SUCCESS:
            return handlePaymentSuccess(event);
          case TOPICS.PAYMENT_FAILED:
            return handlePaymentFailed(event);
          case TOPICS.INVENTORY_RESERVED:
            return handleInventoryReserved(event);
          case TOPICS.INVENTORY_FAILED:
            return handleInventoryFailed(event);
        }
      };

      await retryHandler.executeWithRetry(operation, event, orderLogger);
    }
  }).catch((error) => {
    orderLogger.error({
      event: 'consumer_start',
      status: 'FAILED',
      reason: error.message
    });
  });
}

start().catch(console.error);