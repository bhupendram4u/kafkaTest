const { createConsumer, connectProducer } = require('./shared/kafkaClient');
const consumer = createConsumer('notification-service-group');
const logger = require('./shared/logger')('notification-service');
const RetryHandler = require('./shared/retryHandler');

const retryHandler = new RetryHandler();

const TOPICS = {
  ORDER_CONFIRMED: 'order_confirmed',
  ORDER_CANCELLED: 'order_cancelled'
};

async function sendNotification(event, type) {
  const { orderId } = event;

  // Simulate sending email
  logger.info({
    orderId,
    event: type === 'confirmed' ? 'order_confirmed' : 'order_cancelled',
    status: 'SUCCESS',
    message: `${type === 'confirmed' ? 'Confirmation' : 'Cancellation'} email sent`
  });
}

async function start() {
  await connectProducer();
  await consumer.connect();

  await consumer.subscribe({ topic: TOPICS.ORDER_CONFIRMED });
  await consumer.subscribe({ topic: TOPICS.ORDER_CANCELLED });

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      const event = JSON.parse(message.value.toString());
      const type = topic === TOPICS.ORDER_CONFIRMED ? 'confirmed' : 'cancelled';
      await retryHandler.executeWithRetry(() => sendNotification(event, type), { ...event, event: topic }, logger);
    }
  });

  logger.info({ message: 'Notification Service started' });
}

start().catch(console.error);