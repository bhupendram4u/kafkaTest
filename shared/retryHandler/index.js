const { producer } = require('../kafkaClient');

class RetryHandler {
  constructor(maxRetries = 3, deadLetterTopic = 'dead_letter') {
    this.maxRetries = maxRetries;
    this.deadLetterTopic = deadLetterTopic;
  }

  async executeWithRetry(operation, event, logger, retryCount = 0) {
    const eventName = event.event || event.topic || event.eventType || 'unknown';

    try {
      await operation();
      logger.info({
        orderId: event.orderId,
        event: eventName,
        status: 'SUCCESS',
        message: 'Event processed successfully'
      });
    } catch (error) {
      logger.error({
        orderId: event.orderId,
        event: eventName,
        status: 'FAILED',
        message: 'Event processing failed',
        reason: error.message,
        stack: error.stack,
        retryCount
      });

      if (retryCount < this.maxRetries) {
        // Retry after delay
        await new Promise((resolve) => setTimeout(resolve, 1000 * (retryCount + 1)));
        await this.executeWithRetry(operation, event, logger, retryCount + 1);
      } else {
        // Send to dead letter queue
        await producer.send({
          topic: this.deadLetterTopic,
          messages: [
            {
              key: event.orderId,
              value: JSON.stringify({
                ...event,
                failedAt: new Date().toISOString(),
                error: error.message,
                retryCount
              })
            }
          ]
        });
        logger.error({
          orderId: event.orderId,
          event: eventName,
          status: 'DEAD_LETTER',
          message: 'Event sent to dead letter queue after max retries'
        });
      }
    }
  }
}

module.exports = RetryHandler;