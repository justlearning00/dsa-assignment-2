# Smart Public Transport Ticketing System

Group project for Distributed Systems and Applications (DSA612S).
Each folder represents a microservice or component.
Each ervice works on a different port 
 Service               Port 
 
 Passenger Service      8081 
 Transport Service      8082 
 Ticketing Service      8083
 Payment Service        8084 
 Notification Service    8085 
 Admin Service           8086 


# Public Transport System – Docker Setup Guide

This project uses **Docker Compose** to run shared infrastructure (MongoDB + Kafka).
All microservices should connect to these containers instead of running their own instances.

---

## 1. Starting the Environment

From the project root, run:

```
docker-compose up -d
```

This will start:

* **MongoDB** (port `27017`)
* **ZooKeeper** (port `2181`)
* **Kafka Broker** (ports `9092` external, `19092` internal to Docker)

---

## 2. Connecting to MongoDB (Ballerina)

**Connection string inside Docker services:**

```
mongodb://root:password@mongo:27017
```

**Example Ballerina code:**

```ballerina
import ballerinax/mongodb;

mongodb:ConnectionConfig dbConfig = {
    connection: {
        host: "mongo",
        port: 27017,
        auth: {
            username: "root",
            password: "password"
        },
        options: {
            sslEnabled: false,
            serverSelectionTimeout: 5000
        }
    },
    databaseName: "tickets"
};

mongodb:Client mongoClient = check new (dbConfig);
```

---

## 3. Connecting to Kafka (Ballerina)

* **Inside Docker services (recommended):**

```
kafka1:19092
```

* **Outside Docker (running locally):**

```
localhost:9092
```

**Example Ballerina Producer:**

```ballerina
import ballerinax/kafka;

public function main() returns error? {
    kafka:Producer producer = check new ("kafka1:19092");

    check producer->send({
        topic: "ticket.requests",
        value: { passengerId: 123, route: "Windhoek-Swakop" }
    });
}
```

**Example Ballerina Consumer:**

```ballerina
import ballerinax/kafka;
import ballerina/io;

public function main() returns error? {
    kafka:Consumer consumer = check new ("kafka1:19092", {
        groupId: "ticket-service",
        topics: ["ticket.requests"]
    });

    json[] messages = check consumer->pollPayload(5000);

    foreach var msg in messages {
        io:println("Received: ", msg.toJsonString());
    }
}
```

---

## 4. Topics

Each service should publish/subscribe to the agreed topics. Suggested topics:

* `ticket.requests` → Passenger requests ticket
* `payments.processed` → Payment service confirms transaction
* `schedule.updates` → Central system publishes schedule changes
* `ticket.validations` → Validation results from inspectors

---

## 5. Stopping the Environment

```
docker-compose down
```

---

## 6. Notes

* Do **not** spin up your own Kafka or Mongo — always use this shared setup.
* If you add new topics, update this README so everyone stays in sync.
* Logs:

  * MongoDB → `docker logs -f mongo`
  * Kafka → `docker logs -f kafka1`
