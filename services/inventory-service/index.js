import { producer, createConsumer, connectProducer } from './shared/kafkaClient/index.js';
import logger from './shared/logger/index.js';
import RetryHandler from './shared/retryHandler/index.js';

const consumer = createConsumer('inventory-service-group');
const inventoryLogger = logger('inventory-service');
const retryHandler = new RetryHandler();

const TOPICS = {
  PAYMENT_SUCCESS: 'payment_success',
  INVENTORY_RESERVED: 'inventory_reserved',
  INVENTORY_FAILED: 'inventory_failed'
};

async function processInventory(event) {
  const { orderId, amount } = event;

  inventoryLogger.info({
    orderId,
    event: 'inventory_reserved',
    status: 'STARTED',
    message: 'Reserving inventory'
  });

  // Simulate inventory reservation
  if (orderId.startsWith('FAIL_INVENTORY')) {
    await producer.send({
      topic: TOPICS.INVENTORY_FAILED,
      messages: [{ key: orderId, value: JSON.stringify({ orderId, amount, reason: 'Inventory not available' }) }]
    });
    return;
  }

  if (orderId.startsWith('RANDOM_FAIL') && Math.random() <= 0.5) {
    throw new Error('Simulated random inventory failure');
  }

  await producer.send({
    topic: TOPICS.INVENTORY_RESERVED,
    messages: [{ key: orderId, value: JSON.stringify({ orderId, amount }) }]
  });
}

async function start() {
  await connectProducer();
  await consumer.connect();

  await consumer.subscribe({ topic: TOPICS.PAYMENT_SUCCESS });

  await consumer.run({
    eachMessage: async ({ message }) => {
      const event = JSON.parse(message.value.toString());
      event.event = TOPICS.PAYMENT_SUCCESS;
      await retryHandler.executeWithRetry(() => processInventory(event), event, inventoryLogger);
    }
  });

  inventoryLogger.info({ message: 'Inventory Service started' });
}

start().catch(console.error);