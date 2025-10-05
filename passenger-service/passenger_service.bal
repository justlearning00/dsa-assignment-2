import ballerina/http;
import ballerina/io;
import ballerinax/mongodb;
import ballerinax/kafka;

// MongoDB connection - use Docker service name
mongodb:Client mongoClient = check new ({
    connection: "mongodb://root:password@mongodb:27017/transport_db"
});

// Kafka Producer - use Docker service name
kafka:Producer kafkaProducer = check new ("kafka1:19092");

// Kafka Consumer - listens to multiple topics
kafka:Consumer kafkaConsumer = check new ("kafka1:19092", {
    groupId: "passenger-group",
    topics: ["passenger.trip.responses", "ticket.status.updates", "service.disruptions", "notifications"],
    offsetReset: "earliest"
});

service /passenger on new http:Listener(8085) {

    // REGISTER passenger
    resource function post register(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Register passenger: ", body.toJsonString());
        
        // Uncomment to save to MongoDB
        // check mongoClient->insert("users", body);
        
        check caller->respond({status: "Passenger registered successfully"});
    }

    // LOGIN passenger
    resource function post login(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Login attempt: ", body.toJsonString());
        
        // Uncomment to validate credentials
        // json? user = check mongoClient->findOne("users", {
        //     username: body.username,
        //     password: body.password
        // });
        
        check caller->respond({status: "Login successful"});
    }

    // UPDATE account
    resource function put account/[string userId](http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Update account ", userId, ": ", body.toJsonString());
        
        // Uncomment to update MongoDB
        // check mongoClient->updateOne("users", {userId: userId}, {"$set": body});
        
        check caller->respond({status: "Account updated"});
    }

    // VIEW tickets
    resource function get tickets/[string userId](http:Caller caller, http:Request req) returns error? {
        io:println("Fetching tickets for user ", userId);
        
        // Uncomment to fetch from MongoDB
        // json[] tickets = check mongoClient->find("tickets", {userId: userId});
        json[] tickets = [];
        
        check caller->respond(tickets);
    }

    // REQUEST TRIP - publishes to passenger.trip.requests
    resource function post tripRequest(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Passenger trip request: ", body.toJsonString());

        // Validate required fields
        json passengerIdJson = check body.passengerId;
        
        string passengerId = passengerIdJson.toString();

        // Publish to Kafka topic
        var sendResult = kafkaProducer->send({
            topic: "passenger.trip.requests",
            key: passengerId,
            value: body
        });

        if sendResult is kafka:Error {
            io:println("Failed to send trip request: ", sendResult.message());
            json errorResponse = {status: "FAILED", errorMsg: sendResult.message()};
            check caller->respond(errorResponse);
        } else {
            io:println("Trip request sent to passenger.trip.requests topic");
            json successResponse = {status: "REQUEST_SENT", message: "Your trip request is being processed"};
            check caller->respond(successResponse);
        }
    }

    // REQUEST TICKET - publishes to ticket.requests
    resource function post ticketRequest(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Passenger ticket request: ", body.toJsonString());

        json passengerIdJson = check body.passengerId;
        string passengerId = passengerIdJson.toString();

        var sendResult = kafkaProducer->send({
            topic: "ticket.requests",
            key: passengerId,
            value: body
        });

        if sendResult is kafka:Error {
            io:println("Failed to send ticket request: ", sendResult.message());
            json errorResponse = {status: "FAILED", errorMsg: sendResult.message()};
            check caller->respond(errorResponse);
        } else {
            io:println("Ticket request sent to ticket.requests topic");
            json successResponse = {status: "TICKET_REQUEST_SENT", message: "Your ticket request is being processed"};
            check caller->respond(successResponse);
        }
    }

    // LISTEN for responses from Kafka (trip responses, ticket updates, notifications, disruptions)
    resource function get updates(http:Caller caller, http:Request req) returns error? {
        io:println("Checking for updates from Kafka...");

        json[] messages = check kafkaConsumer->pollPayload(5000);

        if messages.length() > 0 {
            io:println("Received ", messages.length().toString(), " messages");
            foreach var msg in messages {
                io:println("Message: ", msg.toJsonString());
            }
            json successResponse = {status: "SUCCESS", count: messages.length(), updates: messages};
            check caller->respond(successResponse);
        } else {
            json noMsgResponse = {status: "NO_UPDATES", message: "No new messages"};
            check caller->respond(noMsgResponse);
        }
    }

    // LISTEN specifically for trip responses
    resource function get tripResponses(http:Caller caller, http:Request req) returns error? {
        io:println("Listening for trip responses...");

        json[] responses = check kafkaConsumer->pollPayload(5000);
        
        if responses.length() > 0 {
            io:println("Received trip responses from Kafka:");
            foreach var res in responses {
                io:println(res.toJsonString());
            }
        }
        
        check caller->respond(responses);
    }

    // LISTEN for service disruptions
    resource function get disruptions(http:Caller caller, http:Request req) returns error? {
        io:println("Checking for service disruptions...");

        json[] messages = check kafkaConsumer->pollPayload(3000);

        if messages.length() > 0 {
            io:println("Service disruptions found: ", messages.length().toString());
            json disruptionResponse = {status: "DISRUPTIONS_FOUND", disruptions: messages};
            check caller->respond(disruptionResponse);
        } else {
            json clearResponse = {status: "ALL_CLEAR", message: "No service disruptions"};
            check caller->respond(clearResponse);
        }
    }
}