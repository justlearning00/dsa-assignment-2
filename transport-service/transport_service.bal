import ballerina/http;
import ballerina/io;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/time;

mongodb:Client mongoClient = check new ({
    connection: "mongodb://root:password@mongo-db:27017/transport_db"
});

kafka:Producer kafkaProducer = check new (kafka:DEFAULT_URL);
kafka:Consumer kafkaConsumer = check new (kafka:DEFAULT_URL, {
    groupId: "transport-group",
    topics: ["passenger.trip.requests"],
    offsetReset: "earliest"
});

service /transport on new http:Listener(8085) {

    // Fetch existing trips from MongoDB (read-only)
    resource function get trips(http:Caller caller, http:Request req) returns error? {
        io:println("Fetching existing trips from database...");
        
        // Uncomment when MongoDB collection exists
        // json[] trips = check mongoClient->find("trips", {});
        json[] trips = []; // Mocked for now
        
        io:println("Found ", trips.length().toString(), " trips");
        check caller->respond(trips);
    }

    // Fetch existing routes from MongoDB (read-only)
    resource function get routes(http:Caller caller, http:Request req) returns error? {
        io:println("Fetching existing routes from database...");
        
        // Uncomment when MongoDB collection exists
        // json[] routes = check mongoClient->find("routes", {});
        json[] routes = []; // Mocked for now
        
        check caller->respond(routes);
    }

    // Handle passenger trip requests from Kafka
    resource function get listenRequests(http:Caller caller, http:Request req) returns error? {
        io:println("Listening for passenger trip requests...");

        json[] messages = check kafkaConsumer->pollPayload(5000);

        if messages.length() > 0 {
            json[] responses = [];
            
            foreach var msg in messages {
                io:println("Passenger trip request received: ", msg.toJsonString());

                // Extract passenger details safely - handle potential errors
                json passengerIdJson = check msg.passengerId;
                json routeIdJson = check msg.routeId;
                
                string passengerId = passengerIdJson.toString();
                string routeId = routeIdJson.toString();

                // Check if trip exists in MongoDB
                // json? trip = check mongoClient->findOne("trips", {routeId: routeId, status: "ACTIVE"});
                
                // Simulate trip assignment
                json tripResponse = {
                    passengerId: passengerId,
                    routeId: routeId,
                    tripId: "TRIP_" + passengerId,
                    status: "CONFIRMED",
                    vehicle: "BUS_" + routeId,
                    assignedTime: time:utcToString(time:utcNow()),
                    estimatedDeparture: "2025-10-06T07:30:00Z"
                };

                // Send confirmation via Kafka
                var sendResult = kafkaProducer->send({
                    topic: "passenger.trip.responses",
                    key: passengerId,
                    value: tripResponse
                });

                if sendResult is kafka:Error {
                    io:println("Failed to send response: ", sendResult.message());
                } else {
                    io:println("Trip confirmation sent to passenger.trip.responses");
                    responses.push(tripResponse);
                }
            }

            int msgCount = messages.length();
            io:println("Processed ", msgCount.toString(), " requests");
            check caller->respond({
                status: "Processed requests",
                count: msgCount,
                confirmations: responses
            });
        } else {
            check caller->respond({status: "No trip requests pending"});
        }
    }

    // Publish schedule updates
    resource function post updates(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Publishing schedule update: ", body.toJsonString());

        var sendResult = kafkaProducer->send({
            topic: "transport.updates",
            key: "update_" + time:utcToString(time:utcNow()),
            value: body
        });

        if sendResult is kafka:Error {
            io:println("Failed to publish update: ", sendResult.message());
            check caller->respond({status: "Failed", errorMsg: sendResult.message()});
        } else {
            io:println("Update published to transport.updates");
            check caller->respond({status: "Schedule update broadcasted"});
        }
    }

    // Publish service disruptions
    resource function post disruptions(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Publishing service disruption: ", body.toJsonString());

        var sendResult = kafkaProducer->send({
            topic: "service.disruptions",
            key: "disruption_" + time:utcToString(time:utcNow()),
            value: body
        });

        if sendResult is kafka:Error {
            io:println("Failed to publish disruption: ", sendResult.message());
            check caller->respond({status: "Failed", errorMsg: sendResult.message()});
        } else {
            io:println("Disruption published to service.disruptions");
            check caller->respond({status: "Disruption alert sent"});
        }
    }

    // Monitor active trips
    resource function get monitor(http:Caller caller, http:Request req) returns error? {
        io:println("Monitoring active trips...");
        
        // In production: fetch from MongoDB where status = "ACTIVE" or "EN_ROUTE"
        json monitoring = {
            activeTrips: [
                {tripId: "TRIP_001", status: "EN_ROUTE", lastUpdate: time:utcToString(time:utcNow())},
                {tripId: "TRIP_002", status: "ARRIVED", lastUpdate: time:utcToString(time:utcNow())}
            ],
            totalActive: 2
        };
        
        check caller->respond(monitoring);
    }
}