# Event-Driven E-Commerce Order Fulfillment System

This repository implements a highly resilient, industry-grade, multi-service order fulfillment system using **Event-Driven Architecture (EDA)** with **RabbitMQ** and **Postgres**.

The project is structured into five decoupled microservices with isolated databases and features advanced patterns to solve typical distributed systems challenges: the **Transactional Outbox Pattern**, **Idempotency Protection**, **Robust Broker Reconnections**, and **Dead Letter Queueing (DLQ)**.

---

## Architecture Overview

```mermaid
graph TD
    Client[API Client / Frontend] -->|1. POST /api/orders| OS[Order Service]
    Client -->|6. GET status| OS
    
    subgraph Isolated Databases
        OS_DB[(Order DB)]
        PS_DB[(Payment DB)]
        IS_DB[(Inventory DB)]
        SS_DB[(Shipping DB)]
    end

    subgraph Message Broker
        RMQ[RabbitMQ Exchange]
        O_Queue[Order Status Queue]
        P_Queue[Payment Queue]
        I_Queue[Inventory Queue]
        S_Queue[Shipping Queue]
        DLQ[Dead Letter Queue]
    end

    OS <-->|Outbox & Queries| OS_DB
    
    %% Event Flow
    OS -->|Publish OrderCreated| RMQ
    RMQ -->|Route| P_Queue
    P_Queue -->|Consume| PS[Payment Service]
    PS <-->|Outbox & Idempotency| PS_DB
    
    PS -->|Publish PaymentProcessed| RMQ
    RMQ -->|Route| I_Queue
    I_Queue -->|Consume| IS[Inventory Service]
    IS <-->|Outbox & Idempotency| IS_DB
    IS -->|Failures 3x| DLQ
    
    IS -->|Publish InventoryReserved| RMQ
    RMQ -->|Route| S_Queue
    S_Queue -->|Consume| SS[Shipping Service]
    SS <-->|Outbox & Idempotency| SS_DB
    
    SS -->|Publish ShipmentCreated| RMQ
    
    %% Status Updates
    RMQ -->|Route status updates| O_Queue
    O_Queue -->|Consume status updates| OS
    
    %% Fanout notifications
    N_Queue[Notification Queue]
    RMQ -->|Route all events| N_Queue
    N_Queue -->|Log notifications| NS[Notification Service]
```

### Key Industry-Grade Patterns Implemented

1. **Transactional Outbox Pattern (Dual Write Solution)**:
   Publishing an event to RabbitMQ and committing a transaction to Postgres are two separate operations. If RabbitMQ is down during an API call, a service might commit changes to its DB but fail to publish the event, leading to system inconsistency. 
   To solve this, each service saves events to an `outbox` table within the **same database transaction** as the business data. A background polling worker (`outbox.js`) queries the table using `SELECT ... FOR UPDATE SKIP LOCKED` inside a transaction, publishes events, and marks them as processed. This guarantees **at-least-once delivery** of events even if the message broker restarts or undergoes maintenance.

2. **Idempotency Protection**:
   In distributed systems, duplicate message deliveries are common (due to consumer crashes before ACKing, network partitions, etc.). Every consumer tracks processed events in a `processed_events` table. Before executing business logic, the message's `eventId` is queried under a `FOR UPDATE` lock. If it exists, the service ignores the duplicate and acknowledges the message.

3. **Resilient Connection & Auto-Reconnection**:
   Using `amqplib` directly without connection monitoring leads to crashed or silent consumers when RabbitMQ undergoes transient disconnects or restarts. We implement a custom connection manager (`rabbitmq.js`) which registers `'close'` and `'error'` events on connections and channels, executing a reconnect loop with an exponential backoff and automatically restoring topology bindings and consumer subscriptions.

4. **Graceful Shutdown Handling**:
   All services hook into `SIGINT` and `SIGTERM` signals to stop accepting new REST API requests, cancel RabbitMQ consumers to stop receiving new events, wait for in-flight handlers to finish, and cleanly terminate connection pools.

---

## Directory Structure

```text
Event-Driven_E-Commerce_Order_Fulfillment_System/
├── docker-compose.yml       # Orchestrates broker, databases, and microservices
├── .env.example             # Documented environment variables
├── init-db.sh               # SQL seeds setting up tables and databases
├── README.md                # System documentation
├── order-service/           # Receives API orders, publishes OrderCreated
├── payment-service/         # Processes payments, publishes PaymentProcessed
├── inventory-service/       # Deducts inventory, handles retries, routes to DLQ
├── shipping-service/        # Creates shipments, publishes ShipmentCreated
└── notification-service/    # stateless observers logging notifications to stdout
```

---

## Environment Variables (`.env.example`)

A `.env.example` file is included in the root directory. Copy it to `.env` if you want local non-docker configurations, though Docker Compose initializes all environment variables automatically.

```ini
PORT=3000
RABBITMQ_URL=amqp://guest:guest@localhost:5672
DATABASE_URL_ORDER=postgresql://postgres:postgres@localhost:5432/order_db
DATABASE_URL_PAYMENT=postgresql://postgres:postgres@localhost:5432/payment_db
DATABASE_URL_INVENTORY=postgresql://postgres:postgres@localhost:5432/inventory_db
DATABASE_URL_SHIPPING=postgresql://postgres:postgres@localhost:5432/shipping_db
```

---

## Setup & Running the System

You must have **Docker** and **Docker Compose** installed.

To build and start all databases, RabbitMQ, and microservices with a single command:

```bash
docker-compose up --build
```

Wait until all services show `healthy` in `docker-compose ps`.

---

## Postman Collection

For a streamlined testing experience, we have included a pre-configured Postman Collection:
- File: [Order_Fulfillment_System.postman_collection.json](file:///d:/Partnr/Main/week27/Event-Driven_E-Commerce_Order_Fulfillment_System/Order_Fulfillment_System.postman_collection.json)
- **Features**:
  - Automatically extracts and sets the global `orderId` variable upon placing a successful order, so you can call `Get Order Status` immediately without manual copy-pasting.
  - Automatically extracts the global `failOrderId` variable upon placing a failing (`FAIL-ME`) order.
  - Includes pre-configured Basic Authentication credentials for querying the RabbitMQ management API directly.
- **How to use**:
  1. Open Postman, click **Import**, and select the `Order_Fulfillment_System.postman_collection.json` file.
  2. Start running the requests sequentially!

---

## Verification and Testing

### 1. Normal E2E Checkout Flow

Send a `POST` request to the Order Service to place an order.

**Request:**
```bash
curl -X POST http://localhost:3000/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "items": [
      {"productId": "prod-123", "quantity": 2, "price": 10.50}
    ],
    "totalPrice": 21.00
  }'
```

**Success Response (HTTP 202 Accepted):**
```json
{
  "orderId": "47a3fd9f-4318-47bc-ad7e-07a9ef03b9b4"
}
```

Now, query the status of the order. The status evolves as events propagate through the queues:
`PENDING` $\rightarrow$ `PAID` $\rightarrow$ `INVENTORY_RESERVED` $\rightarrow$ `SHIPPED`.

**Query Status:**
```bash
curl http://localhost:3000/api/orders/47a3fd9f-4318-47bc-ad7e-07a9ef03b9b4
```

**Response (HTTP 200 OK):**
```json
{
  "orderId": "47a3fd9f-4318-47bc-ad7e-07a9ef03b9b4",
  "status": "SHIPPED",
  "items": [{"productId":"prod-123","quantity":2,"price":10.50}],
  "totalPrice": 21,
  "createdAt": "2026-07-18T09:30:11.000Z"
}
```

---

### 2. Price Tampering Prevention (Input Validation)

Attempt to place an order where the items' total price does not match the `totalPrice` field:

```bash
curl -i -X POST http://localhost:3000/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "items": [
      {"productId": "prod-123", "quantity": 2, "price": 10.50}
    ],
    "totalPrice": 100.00
  }'
```

**Response (HTTP 400 Bad Request):**
```json
{
  "error": "totalPrice (100) does not match the sum of item totals (21.00)"
}
```

---

### 3. Dead Letter Queue (DLQ) Routing

The Inventory Service is configured to intentionally fail processing if an order contains an item with `productId: "FAIL-ME"`. 

1. Create a failing order:
   ```bash
   curl -X POST http://localhost:3000/api/orders \
     -H "Content-Type: application/json" \
     -d '{
       "items": [
         {"productId": "FAIL-ME", "quantity": 1, "price": 10.00}
       ],
       "totalPrice": 10.00
     }'
   ```
2. The order will successfully process payment and transition to `PAID`.
3. When the `PaymentProcessed` event reaches the Inventory Service, the service will attempt to process it, fail, and `nack(requeue=true)` twice.
4. On the 3rd attempt, it will `nack(requeue=false)` the message.
5. RabbitMQ will route the message to the Dead Letter Exchange (`dlx_exchange`), which delivers it into the `dead_letter_queue`.
6. Query RabbitMQ's management API or broker logs to confirm the message is in the DLQ:
   ```bash
   curl -u guest:guest http://localhost:15672/api/queues/%2F/dead_letter_queue
   ```
   (Verify `messages` count is greater than 0).
7. Querying the order status will show that it got stuck at `PAID` and never reached `SHIPPED`:
   ```bash
   curl http://localhost:3000/api/orders/{orderId}
   ```

---

### 4. Broker Durability & Auto-Recovery Test

1. Create a normal order:
   ```bash
   curl -X POST http://localhost:3000/api/orders \
     -H "Content-Type: application/json" \
     -d '{
       "items": [
         {"productId": "prod-123", "quantity": 1, "price": 5.00}
       ],
       "totalPrice": 5.00
     }'
   ```
2. Immediately restart the message broker container:
   ```bash
   docker-compose restart broker
   ```
3. Wait 15-20 seconds for the broker to fully start up and healthy.
4. Query the order:
   ```bash
   curl http://localhost:3000/api/orders/{orderId}
   ```
5. Confirm that the order status successfully reaches `SHIPPED`. This demonstrates that:
   - The message was persisted durably on disk by RabbitMQ.
   - The microservices successfully reconnected to the broker after it came back online, restored their consumers, and finished processing the pipeline.
