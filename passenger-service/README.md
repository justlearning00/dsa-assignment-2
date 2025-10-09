## Test 1: Register a New Passenger

```bash
curl -X POST http://localhost:8081/passenger/register \
  -H "Content-Type: application/json" \
  -d '{
    "userId": "PASS_TEST_001",
    "username": "testuser",
    "password": "test123",
    "name": "Test User",
    "email": "test@example.com",
    "phone": "+26481234567",
    "balance": 100.00
  }'
```

**Expected Response:**
```json
{
  "status": "Passenger registered successfully",
  "userId": "PASS_TEST_001"
}
```

**Verify in MongoDB:**
```bash
docker exec -it mongodb mongosh
use transport_db
db.users.find({userId: "PASS_TEST_001"}).pretty()
```

---

## Test 2: Login

```bash
curl -X POST http://localhost:8081/passenger/login \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser",
    "password": "test123"
  }'
```

**Expected Response:**
```json
{
  "status": "Login successful",
  "user": {
    "userId": "PASS_TEST_001",
    "username": "testuser",
    "name": "Test User",
    "email": "test@example.com",
    "phone": "+26481234567",
    "balance": 100.00
  }
}
```

**Test Failed Login:**
```bash
curl -X POST http://localhost:8081/passenger/login \
  -H "Content-Type: application/json" \
  -d '{
    "username": "testuser",
    "password": "wrongpassword"
  }'
```

Expected: `{"status": "Invalid credentials"}`

---

## Test 3: Update Account

```bash
curl -X PUT http://localhost:8081/passenger/account/PASS_TEST_001 \
  -H "Content-Type: application/json" \
  -d '{
    "phone": "+26481999999",
    "balance": 150.00
  }'
```

**Expected Response:**
```json
{
  "status": "Account updated",
  "matchedCount": 1,
  "modifiedCount": 1
}
```

---

## Test 4: Request a Trip (Kafka Producer)

```bash
curl -X POST http://localhost:8081/passenger/tripRequest \
  -H "Content-Type: application/json" \
  -d '{
    "passengerId": "PASS_TEST_001",
    "routeId": "BUS_101_WINDHOEK_SWAKOP",
    "preferredTime": "2025-10-09T10:00:00Z",
    "numberOfPassengers": 1
  }'
```

**Expected Response:**
```json
{
  "status": "REQUEST_SENT",
  "message": "Your trip request is being processed"
}
```

**Verify Kafka Message:**
```bash
# In another terminal, listen to the topic
docker exec -it kafka1 kafka-console-consumer \
  --topic passenger.trip.requests \
  --from-beginning \
  --bootstrap-server localhost:9092
```

You should see the trip request message appear.

---

## Test 5: Request a Ticket (Kafka Producer)

```bash
curl -X POST http://localhost:8081/passenger/ticketRequest \
  -H "Content-Type: application/json" \
  -d '{
    "passengerId": "PASS_TEST_001",
    "routeId": "BUS_101_WINDHOEK_SWAKOP",
    "tripId": "TRIP_MORNING_001",
    "ticketType": "SINGLE_RIDE",
    "price": 25.00,
    "paymentMethod": "CREDIT_CARD"
  }'
```

**Expected Response:**
```json
{
  "status": "TICKET_REQUEST_SENT",
  "message": "Your ticket request is being processed"
}
```

**Verify Kafka Message:**
```bash
docker exec -it kafka1 kafka-console-consumer \
  --topic ticket.requests \
  --from-beginning \
  --bootstrap-server localhost:9092
```

---

## Test 6: View Passenger Tickets

First, insert a test ticket in MongoDB:

```bash
docker exec -it mongodb mongosh
use transport_db
db.tickets.insertOne({
  ticketId: "TKT_001",
  userId: "PASS_TEST_001",
  routeId: "BUS_101_WINDHOEK_SWAKOP",
  tripId: "TRIP_MORNING_001",
  status: "CONFIRMED",
  purchaseDate: new Date(),
  expiryDate: new Date(Date.now() + 86400000),
  fare: 25.00
})
```

Then test the endpoint:

```bash
curl http://localhost:8081/passenger/tickets/PASS_TEST_001
```

**Expected Response:**
```json
[
  {
    "ticketId": "TKT_001",
    "userId": "PASS_TEST_001",
    "routeId": "BUS_101_WINDHOEK_SWAKOP",
    "tripId": "TRIP_MORNING_001",
    "status": "CONFIRMED",
    "purchaseDate": "2025-10-09T...",
    "expiryDate": "2025-10-10T...",
    "fare": 25.00
  }
]
```

---

## Test 7: Check for Kafka Updates (Consumer)

```bash
curl http://localhost:8081/passenger/updates
```

**Expected Response (if no messages):**
```json
{
  "status": "NO_UPDATES",
  "message": "No new messages"
}
```

**To simulate an update:**

1. Send a test message to one of the consumer topics:
```bash
docker exec -it kafka1 kafka-console-producer \
  --topic ticket.status.updates \
  --bootstrap-server localhost:9092
```

2. Then paste this JSON:
```json
{"ticketId":"TKT_001","status":"VALIDATED","message":"Ticket validated successfully"}
```

3. Call the updates endpoint again:
```bash
curl http://localhost:8081/passenger/updates
```

Now you should see:
```json
{
  "status": "SUCCESS",
  "count": 1,
  "updates": [
    {
      "ticketId": "TKT_001",
      "status": "VALIDATED",
      "message": "Ticket validated successfully"
    }
  ]
}
```

---

## Test 8: Check Service Disruptions

```bash
curl http://localhost:8081/passenger/disruptions
```

**Expected (if no disruptions):**
```json
{
  "status": "ALL_CLEAR",
  "message": "No service disruptions"
}
```

**To simulate a disruption:**

Send via Kafka:
```bash
docker exec -it kafka1 kafka-console-producer \
  --topic service.disruptions \
  --bootstrap-server localhost:9092
```

Paste:
```json
{"routeId":"BUS_101","severity":"HIGH","description":"Road closure on Main Street"}
```

Call endpoint again - should show the disruption.

---
