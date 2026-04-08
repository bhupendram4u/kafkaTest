# E-commerce Event-Driven Microservices Demo

This project demonstrates a production-style event-driven microservice system for an e-commerce payment workflow using Apache Kafka, implementing Saga Pattern, Outbox Pattern, Idempotent Consumer, Retry Pattern, Dead Letter Queue, and distributed logging.

## Architecture

The system consists of four microservices:

- **Order Service**: Handles order creation, outbox pattern for event publishing
- **Payment Service**: Processes payments with idempotency
- **Inventory Service**: Manages inventory reservation
- **Notification Service**: Sends confirmation/cancellation emails

## Technologies

- Node.js 18+
- Express.js
- KafkaJS
- PostgreSQL
- Redis
- Winston (logging)
- Docker & Docker Compose

## Prerequisites

- Docker and Docker Compose installed

## Quick Start

1. Clone the repository
2. Navigate to the project directory
3. Run `docker compose up --build`
4. Wait for all services to start (check logs for "started" messages)
5. Run the test script: `cd scripts && npm install && npm run test`

## Services

| Service | Port | Description |
|---------|------|-------------|
| Order Service | 3001 | Order management with outbox pattern |
| Payment Service | 3002 | Payment processing with idempotency |
| Inventory Service | 3003 | Inventory reservation |
| Notification Service | 3004 | Email notifications |
| Kafka | 9092 | Message broker |
| PostgreSQL | 5432 | Database |
| Redis | 6379 | Cache for idempotency |

## API Endpoints

### Create Order
```bash
POST http://localhost:3001/orders
Content-Type: application/json

{
  "orderId": "ORD1001",
  "amount": 500
}
```

## Test Cases

The system includes failure simulation based on order ID prefixes:

- `FAIL_PAYMENT_*`: Simulates payment failure
- `FAIL_INVENTORY_*`: Simulates inventory failure
- `RANDOM_FAIL_*`: Randomly fails processing
- `FAIL_RETRY_*`: Fails consistently to test retry + DLQ

## Expected Logs

### Successful Order (ORD1001)
```
Order Service → order_created ORD1001
Payment Service → payment_success ORD1001
Inventory Service → inventory_reserved ORD1001
Order Service → order_confirmed ORD1001
Notification Service → email sent ORD1001
```

### Payment Failure (FAIL_PAYMENT_001)
```
Order Service → order_created FAIL_PAYMENT_001
Payment Service → payment_failed FAIL_PAYMENT_001
Order Service → order_cancelled FAIL_PAYMENT_001
Notification Service → cancellation email sent
```

## Patterns Demonstrated

- **Event Driven Architecture**: Services communicate via Kafka events
- **Saga Pattern**: Orchestrates distributed transactions with compensation
- **Outbox Pattern**: Reliable event publishing from Order Service
- **Idempotent Consumer**: Payment Service avoids duplicate processing
- **Retry Pattern**: Failed events are retried up to 3 times
- **Dead Letter Queue**: Exhaustively failed events sent to `dead_letter` topic
- **Distributed Logging**: Structured logs with Winston across all services

## Monitoring

Check service logs in Docker Compose output for real-time monitoring of the saga workflow.

## Cleanup

Run `docker compose down -v` to stop services and remove volumes.