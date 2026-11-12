# Banking System

A Java/Spring Boot microservices project for account management, money transfers, fraud checks, and payment processing. The services expose HTTP APIs and use Kafka events to coordinate asynchronous work. MySQL stores account, transaction, and payment data; Redis supports rate limiting and short-lived transaction verification data.

**This project is not production-ready.**

## Architecture

```text
Clients
   |
   v
 ______________________________________________
| API Gateway (:8080) -- Redis (rate limiting) |
|______________________________________________|
   |                |                   |
   v                v                   v
Account(:8081)  Transaction(:8082)   Payment (:8083)
   |                 |                  |
   +-------------+--------------+------------- MySQL
                 |
                 +---- Kafka events ----> Fraud Detection (:8084)
                                      +--> Notifications (:8085)
```

| Service | Responsibility |
| --- | --- |
| `api-gateway` | Routes account, transaction, and payment API traffic and applies Redis-backed request limits. |
| `account-service` | Creates and reads accounts, manages balances and account status, and persists accounts in MySQL. |
| `transaction-service` | Initiates transfers, records transaction state, manages OTP verification in Redis, and publishes transaction events. |
| `fraud-detection-service` | Consumes initiated-transfer events and evaluates transactions using configured limits and account data. |
| `payment-service` | Creates Razorpay orders, records payment state, and processes payment webhooks. |
| `notification-service` | Consumes Kafka events and sends account/payment/transaction notifications. |

### Transfer flow

1. A client submits a transfer through the gateway.
2. The transaction service asks the account service to debit the sender, records the transfer as processing, and publishes `transaction.initiated`.
3. Fraud detection evaluates the event. A clean result lets the transaction service complete the transfer; a verification request triggers an OTP stored in Redis with a five-minute expiry.
4. On completion, `transaction.completed` is published. The account service credits the receiver, and notification handling sends alerts. Failure/verification paths can publish refund or fraud events.

The services communicate through Kafka topics, so event processing is asynchronous. Allow a short time for transfer state and balances to update after an API response.

## Requirements

- Java 17
- Docker Desktop with Docker Compose
- Internet access on the first Maven build, to download dependencies

Each service is an independent Maven project with its own `mvnw.cmd`; there is no root Maven aggregator.

## Setup and run (Windows PowerShell)

1. Open PowerShell in the project root.
2. Start the supporting infrastructure:

   ```powershell
   docker compose up -d
   docker compose ps
   ```

   This starts MySQL, Redis, Kafka, and ZooKeeper. Wait until the containers are running before starting the services. The services expect these local ports: MySQL `3306`, Redis `6379`, and Kafka `9092`.

3. Configure MySQL credentials consistently. `docker-compose.yml` initializes the MySQL root password, while each database-backed service has a datasource password in its `src/main/resources/application.yml` or `application.yaml`. Make these values match for local development. Account, transaction, and payment databases are configured to be created automatically.

4. Start each application in a **separate PowerShell window**, from the project root. For example:

   ```powershell
   Set-Location .\account-service
   .\mvnw.cmd spring-boot:run
   ```

   Repeat for each service:

   ```powershell
   Set-Location .\transaction-service
   .\mvnw.cmd spring-boot:run
   ```

   ```powershell
   Set-Location .\fraud-detection-service
   .\mvnw.cmd spring-boot:run
   ```

   ```powershell
   Set-Location .\payment-service
   .\mvnw.cmd spring-boot:run
   ```

   ```powershell
   Set-Location .\notification-service
   .\mvnw.cmd spring-boot:run
   ```

   ```powershell
   Set-Location .\api-gateway
   .\mvnw.cmd spring-boot:run
   ```

   Start the account, transaction, and payment services before relying on gateway routes. Fraud and notification services should also be running for the full event-driven flow.

| Service | Local port |
| --- | ---: |
| API Gateway | 8080 |
| Account | 8081 |
| Transaction | 8082 |
| Payment | 8083 |
| Fraud Detection | 8084 |
| Notification | 8085 |

The gateway routes `/api/v1/accounts/**`, `/api/v1/transactions/**`, and `/api/v1/payments/**`. Direct service URLs use the same paths on their respective ports.

### External integrations

- Payment order creation requires valid Razorpay credentials; the checked-in values are placeholders. Configure valid **test-mode** credentials before trying payment processing.
- Email delivery requires a working mail-server configuration for the notification service.
- Service configuration currently contains local-development settings. Do not put production credentials in source control; use an appropriate secrets/configuration mechanism before deployment.

## Testing the whole project

### Automated tests

Start the infrastructure first, as application-context tests may initialize integrations that depend on it. Run the Maven verification phase for each independent service:

```powershell
$services = @(
  "account-service",
  "api-gateway",
  "fraud-detection-service",
  "notification-service",
  "payment-service",
  "transaction-service"
)

foreach ($service in $services) {
  Push-Location $service
  try {
    & .\mvnw.cmd clean verify
    if ($LASTEXITCODE -ne 0) {
      throw "Verification failed for $service (exit code $LASTEXITCODE)"
    }
  }
  finally {
    Pop-Location
  }
}
```

To run just one service's tests, for example:

```powershell
Set-Location .\transaction-service
.\mvnw.cmd test
```

Each service currently has a Spring application-context load test. These are useful build/startup checks, but they do **not** by themselves validate the complete transfer, fraud, payment-webhook, or notification flows.

### Postman API testing guide

Start the supporting containers and all six services first. In Postman, create an environment with these variables:

| Variable | Initial value | Use |
| --- | --- | --- |
| `baseUrl` | `http://localhost:8080` | API gateway |
| `senderAccountNumber` | *(leave blank)* | Set from the first account response |
| `receiverAccountNumber` | *(leave blank)* | Set from the second account response |
| `transactionId` | *(leave blank)* | Set from the transfer response |
| `otp` | *(leave blank)* | Set to the OTP delivered for a verification-required transfer |

For JSON requests, select **Body → raw → JSON** and set `Content-Type: application/json`. The account, transaction, and payment APIs below can be sent through the gateway.

#### 1. Check service health

Create a `GET` request for each URL:

```text
http://localhost:8080/actuator/health
http://localhost:8081/actuator/health
http://localhost:8082/actuator/health
http://localhost:8083/actuator/health
http://localhost:8084/actuator/health
http://localhost:8085/actuator/health
```

Expected: **200 OK**, with a body similar to:

```json
{ "status": "UP" }
```

#### 2. Create two accounts

Send `POST {{baseUrl}}/api/v1/accounts/create` with this body. Use a unique email for each account; change the holder details for the second request.

```json
{
  "accountHolderName": "Postman Test Sender",
  "email": "sender@example.test",
  "phone": "5550100100",
  "accountType": "SAVINGS",
  "initialDeposit": 1000.00
}
```

`accountType` accepts `SAVINGS`, `CURRENT`, or `FIXED_DEPOSIT`. Expected: **201 Created**, with a response shaped like:

```json
{
  "id": "<database-id>",
  "accountNumber": "<12-digit-account-number>",
  "accountHolderName": "Postman Test Sender",
  "email": "sender@example.test",
  "phone": "5550100100",
  "accountType": "SAVINGS",
  "accountStatus": "ACTIVE",
  "balance": 1000.00,
  "dailyTransactionLimit": 100000,
  "createdAt": "<timestamp>"
}
```

Save each returned `accountNumber` into the corresponding Postman environment variable.

#### 3. Read an account and balance

Send these `GET` requests:

```text
{{baseUrl}}/api/v1/accounts/{{senderAccountNumber}}
{{baseUrl}}/api/v1/accounts/{{senderAccountNumber}}/balance
```

Expected: **200 OK**. The first returns the account object; the balance endpoint returns a JSON number, for example:

```json
1000.00
```

#### 4. Submit a transfer and check its status

Send `POST {{baseUrl}}/api/v1/transactions/transfer`:

```json
{
  "senderAccountNumber": "{{senderAccountNumber}}",
  "receiverAccountNumber": "{{receiverAccountNumber}}",
  "amount": 10.00,
  "description": "Postman test transfer"
}
```

Expected: **201 Created**, with a transaction response like:

```json
{
  "id": "<transaction-id>",
  "senderAccountNumber": "<sender-account-number>",
  "receiverAccountNumber": "<receiver-account-number>",
  "amount": 10.00,
  "type": "TRANSFER",
  "status": "PROCESSING",
  "description": "Postman test transfer",
  "failureReason": null,
  "referenceNumber": "<reference>",
  "createdAt": "<timestamp>",
  "completedAt": null
}
```

Save `id` as `transactionId`. The transfer is processed asynchronously, so the initial status can be `PROCESSING`. Poll `GET {{baseUrl}}/api/v1/transactions/{{transactionId}}` until it reaches `COMPLETED` or `PENDING_VERIFICATION`. A completed transfer should eventually credit the receiver. Transaction statuses also include `FAILED` and `FLAGGED`.

To check sent transactions, use:

```text
GET {{baseUrl}}/api/v1/transactions/account/{{senderAccountNumber}}
```

Expected: **200 OK** with a JSON array of transaction responses.

#### 5. Verify an OTP when requested

If the transaction reaches `PENDING_VERIFICATION`, get the OTP from the configured notification channel, set the `otp` environment variable, then send:

```text
POST {{baseUrl}}/api/v1/transactions/{{transactionId}}/verify?otp={{otp}}
```

Expected for a valid, unexpired OTP: **200 OK** with the transaction response updated to `COMPLETED`. The OTP expires after five minutes. Do not use a wrong OTP for this smoke test; the current flow treats it as a fraud case, flags the transaction, and triggers a refund.

#### 6. Create a Razorpay test payment (optional)

Configure valid Razorpay **test-mode** credentials first. Send `POST {{baseUrl}}/api/v1/payments/create-order`:

```json
{
  "accountNumber": "{{senderAccountNumber}}",
  "amount": 10.00,
  "description": "Postman test payment"
}
```

Expected: **201 Created**, with a response shaped like:

```json
{
  "paymentId": "<database-payment-id>",
  "razorpayPaymentId": "<razorpay-order-id>",
  "amount": 10.00,
  "currency": "USD",
  "status": "CREATED",
  "razorpayKeyId": "<test-key-id>"
}
```

In the current response model, `razorpayPaymentId` contains the Razorpay **order ID**. Complete checkout using Razorpay's test flow. For an end-to-end webhook test, configure Razorpay to send its callback to a reachable development URL ending in `/api/v1/payments/webhook`; Razorpay cannot normally connect to your machine's `localhost`. The local service endpoint is `http://localhost:8083/api/v1/payments/webhook`.

To check the webhook handler directly in Postman, send `POST http://localhost:8083/api/v1/payments/webhook` with the actual order ID from the payment-order response and a payment ID from the corresponding Razorpay test payment:

```json
{
  "event": "payment.captured",
  "payload": {
    "payment": {
      "entity": {
        "order_id": "<razorpay-order-id>",
        "id": "<razorpay-payment-id>"
      }
    }
  }
}
```

For a matching order, the handler returns **200 OK** with `Webhook processed`; the payment should become `COMPLETED` and a Kafka notification event should be published. Verify processing in the service logs and notification consumer rather than relying on the HTTP response alone.

**Webhook caution:** the current webhook handler does not verify a Razorpay signature. Use this endpoint only in a local/test environment; do not expose it publicly.

Downstream event handling may take a moment. If state or balances do not update, check the relevant service logs and Kafka/container status. The automated tests are context-load checks, not end-to-end assertions of these API flows.

## Stop local infrastructure

From the project root:

```powershell
docker compose down
```

This stops and removes the containers but preserves the named MySQL data volume. To remove local database data as well, use `docker compose down -v` only when that data is no longer needed.