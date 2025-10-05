import ballerina/http;
import ballerina/log;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/time;

type TripRequest record {
    string eventType;
    string passengerId;
    json data;
    time:Utc timestamp;
};

service /passenger on new http:Listener(8081) {

    // Service-level fields
    mongodb:Client mongoClient;
    mongodb:Database transportDb;
    mongodb:Collection usersCollection;
    mongodb:Collection ticketsCollection;

    kafka:Producer kafkaProducer;
    kafka:Consumer kafkaConsumer;

    // Service initialization
    isolated function init() returns error? {
        log:printInfo("Initializing Passenger Service...");

        // Initialize MongoDB
        self.mongoClient = check new ({
            connection: "mongodb://localhost:27017/transport_db"
        });

        self.transportDb = check self.mongoClient->getDatabase("transport_db");
        self.usersCollection = check self.transportDb->getCollection("users");
        self.ticketsCollection = check self.transportDb->getCollection("tickets");

        // Initialize Kafka
        self.kafkaProducer = check new ("kafka1:19092");
        self.kafkaConsumer = check new ("kafka1:19092", {
            groupId: "passenger-group",
            topics: ["passenger.trip.responses", "ticket.status.updates", "service.disruptions", "notifications"],
            offsetReset: "earliest"
        });

        log:printInfo("Passenger Service initialized successfully");
    }

    // REGISTER passenger
    isolated resource function post register(http:Caller caller, http:Request req) returns error? {
        log:printInfo("POST /passenger/register - Registering new passenger");
        json payload = check req.getJsonPayload();
        map<json> userData = <map<json>>payload;

        check self.usersCollection->insertOne(userData);

        string userId = (check payload.userId).toString();
        check caller->respond({status: "Passenger registered successfully", userId: userId});
    }

    // LOGIN passenger
    isolated resource function post login(http:Caller caller, http:Request req) returns error? {
        log:printInfo("POST /passenger/login - Login attempt");
        json payload = check req.getJsonPayload();
        
        string username = (check payload.username).toString();
        string password = (check payload.password).toString();

        stream<map<json>, error?> resultStream = check self.usersCollection->find({username: username, password: password});
        map<json>[] users = check from map<json> user in resultStream select user;

        if users.length() > 0 {
            check caller->respond({status: "Login successful", user: users[0]});
        } else {
            check caller->respond({status: "Invalid credentials"});
        }
    }

    // UPDATE account
    isolated resource function put account/[string userId](http:Caller caller, http:Request req) returns error? {
        log:printInfo("PUT /passenger/account/" + userId + " - Updating account");
        json payload = check req.getJsonPayload();
        map<json> updateData = <map<json>>payload;

        mongodb:UpdateResult result = check self.usersCollection->updateOne(
            {userId: userId}, {set: updateData}
        );

        check caller->respond({
            status: "Account updated",
            matchedCount: result.matchedCount,
            modifiedCount: result.modifiedCount
        });
    }

    // VIEW tickets
    isolated resource function get tickets/[string userId](http:Caller caller, http:Request req) returns error? {
        log:printInfo("GET /passenger/tickets/" + userId + " - Fetching tickets");
        
        stream<map<json>, error?> resultStream = check self.ticketsCollection->find({userId: userId});
        map<json>[] tickets = check from map<json> ticket in resultStream select ticket;
        
        check caller->respond(tickets);
    }

    // REQUEST TRIP - publishes to passenger.trip.requests
    isolated resource function post tripRequest(http:Caller caller, http:Request req) returns error? {
        log:printInfo("POST /passenger/tripRequest - Processing trip request");
        json payload = check req.getJsonPayload();

        string passengerId = (check payload.passengerId).toString();

        TripRequest tripReq = {
            eventType: "TRIP_REQUEST",
            passengerId: passengerId,
            data: payload,
            timestamp: time:utcNow()
        };

        check self.kafkaProducer->send({
            topic: "passenger.trip.requests",
            key: passengerId,
            value: tripReq
        });

        log:printInfo("Trip request sent to Kafka");
        check caller->respond({status: "REQUEST_SENT", message: "Your trip request is being processed"});
    }

    // REQUEST TICKET - publishes to ticket.requests
    isolated resource function post ticketRequest(http:Caller caller, http:Request req) returns error? {
        log:printInfo("POST /passenger/ticketRequest - Processing ticket request");
        json payload = check req.getJsonPayload();

        string passengerId = (check payload.passengerId).toString();

        check self.kafkaProducer->send({
            topic: "ticket.requests",
            key: passengerId,
            value: payload
        });

        log:printInfo("Ticket request sent to Kafka");
        check caller->respond({status: "TICKET_REQUEST_SENT", message: "Your ticket request is being processed"});
    }

    // LISTEN for responses from Kafka
    isolated resource function get updates(http:Caller caller, http:Request req) returns error? {
        log:printInfo("GET /passenger/updates - Checking for Kafka updates");

        json[] messages = check self.kafkaConsumer->pollPayload(5000);

        if messages.length() > 0 {
            log:printInfo("Received messages from Kafka: " + messages.length().toString());
            check caller->respond({status: "SUCCESS", count: messages.length(), updates: messages});
        } else {
            check caller->respond({status: "NO_UPDATES", message: "No new messages"});
        }
    }

    // LISTEN for trip responses
    isolated resource function get tripResponses(http:Caller caller, http:Request req) returns error? {
        log:printInfo("GET /passenger/tripResponses - Listening for trip responses");

        json[] responses = check self.kafkaConsumer->pollPayload(5000);
        check caller->respond(responses);
    }

    // LISTEN for service disruptions
    isolated resource function get disruptions(http:Caller caller, http:Request req) returns error? {
        log:printInfo("GET /passenger/disruptions - Checking for disruptions");

        json[] messages = check self.kafkaConsumer->pollPayload(3000);

        if messages.length() > 0 {
            log:printInfo("Disruptions found: " + messages.length().toString());
            check caller->respond({status: "DISRUPTIONS_FOUND", disruptions: messages});
        } else {
            check caller->respond({status: "ALL_CLEAR", message: "No service disruptions"});
        }
    }
}