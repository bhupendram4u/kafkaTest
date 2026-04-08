const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'ecommerce-microservices',
  brokers: [process.env.KAFKA_BROKER || 'localhost:9092']
});

const producer = kafka.producer();

const createConsumer = (groupId) => kafka.consumer({ groupId });

const connectProducer = async () => {
  await producer.connect();
};

const disconnectProducer = async () => {
  await producer.disconnect();
};

module.exports = {
  kafka,
  producer,
  createConsumer,
  connectProducer,
  disconnectProducer
};