import ballerina/http;
import ballerina/io;
import ballerinax/mongodb;

mongodb:Client mongoClient = check new ({
    connection: "mongodb://root:password@localhost:27017/transport_db"
});

// Placeholder for Kafka Producer (for schedule updates)
// kafka:Producer kafkaProducer = check new (...);

service /transport on new http:Listener(8082) {

    //  CREATE ROUTE
    resource function post routes(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Create route (mocked): ", body.toJsonString());
        // check mongoClient->insert("routes", body);
        check caller->respond({status: "Route created (mocked)"});
    }

    //  VIEW ROUTES
    resource function get routes(http:Caller caller, http:Request req) returns error? {
        io:println("Fetch all routes (mocked)");
        // json[] routes = await mongoClient->find("routes", {});
        json payload = [];
        check caller->respond(payload);
    }

    //  CREATE TRIP
    resource function post trips(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Create trip (mocked): ", body.toJsonString());
        // check mongoClient->insert("trips", body);
        check caller->respond({status: "Trip created (mocked)"});
    }

    //  VIEW TRIPS
    resource function get trips(http:Caller caller, http:Request req) returns error? {
        io:println("Fetch all trips (mocked)");
        // json[] trips = await mongoClient->find("trips", {});
        json payload = [];
        check caller->respond(payload);
    }

    //  UPDATE TRIP STATUS (e.g., delay, cancelled)
    resource function patch trips/[string tripId]/status(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Update trip status ", tripId, " (mocked): ", body.toJsonString());
        // check mongoClient->updateById("trips", tripId, body);
        // Kafka: check kafkaProducer->send({topic: "schedule.updates", value: body});
        check caller->respond({status: "Trip status updated (mocked)"});
    }
}
