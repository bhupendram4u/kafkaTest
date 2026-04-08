import { producer, createConsumer, connectProducer } from './shared/kafkaClient/index.js';
import logger from './shared/logger/index.js';
import RetryHandler from './shared/retryHandler/index.js';
import redis from 'redis';

const consumer = createConsumer('payment-service-group');
const paymentLogger = logger('payment-service');
const client = redis.createClient({
  socket: {
    host: process.env.REDIS_HOST || 'localhost',
    port: Number(process.env.REDIS_PORT || 6379)
  }
});

const retryHandler = new RetryHandler();

const TOPICS = {
  ORDER_CREATED: 'order_created',
  PAYMENT_SUCCESS: 'payment_success',
  PAYMENT_FAILED: 'payment_failed'
};

async function processPayment(event) {
  const { orderId, amount } = event;

  // Idempotency check
  const exists = await client.get(`payment:${orderId}`);
  if (exists) {
    paymentLogger.info({
      orderId,
      event: 'payment_processing',
      status: 'SKIPPED',
      message: 'Payment already processed'
    });
    return;
  }

  // Simulate payment processing
  paymentLogger.info({
    orderId,
    event: 'payment_processing',
    status: 'STARTED',
    message: 'Processing payment'
  });

  if (orderId.startsWith('FAIL_RETRY')) {
    throw new Error('Simulated retryable payment gateway error');
  }

  // Business failure path
  if (orderId.startsWith('FAIL_PAYMENT')) {
    await producer.send({
      topic: TOPICS.PAYMENT_FAILED,
      messages: [{ key: orderId, value: JSON.stringify({ orderId, amount, reason: 'Simulated payment gateway error' }) }]
    });
    return;
  }

  if (orderId.startsWith('RANDOM_FAIL') && Math.random() <= 0.5) {
    throw new Error('Simulated random payment failure');
  }

  await client.set(`payment:${orderId}`, 'processed');
  await producer.send({
    topic: TOPICS.PAYMENT_SUCCESS,
    messages: [{ key: orderId, value: JSON.stringify({ orderId, amount }) }]
  });
}

async function start() {
  await connectProducer();
  await consumer.connect();
  await client.connect();

  await consumer.subscribe({ topic: TOPICS.ORDER_CREATED });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());
      event.event = TOPICS.ORDER_CREATED;
      await retryHandler.executeWithRetry(() => processPayment(event), event, paymentLogger);
    }
  });

  paymentLogger.info({ message: 'Payment Service started' });
}

start().catch(console.error);