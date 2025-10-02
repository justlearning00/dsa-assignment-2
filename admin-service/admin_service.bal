import ballerina/http;
import ballerina/io;
import ballerinax/mongodb;

mongodb:Client mongoClient = check new ({
    connection: "mongodb://root:password@localhost:27017/transport_db"});

// 

// Placeholder for Kafka Producer
// kafka:Producer kafkaProducer = check new (...);

service /admin on new http:Listener(8086) {

    
    // ROUTE MANAGEMENT
    
    resource function post routes(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Create route (mocked): ", body.toJsonString());
        // check mongoClient->insert("routes", body);
        check caller->respond({status: "Route created (mocked)"});
    }

    resource function get routes(http:Caller caller, http:Request req) returns error? {
        io:println("Fetch all routes (mocked)");
       // json[] routes = await mongoClient->find("routes", {});
    json payload = [];
    check caller->respond(payload);
    }

    resource function put routes/[string routeId](http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Update route ", routeId, " (mocked): ", body.toJsonString());
        // check mongoClient->updateById("routes", routeId, body);
        check caller->respond({status: "Route updated (mocked)"});
    }

    resource function delete routes/[string routeId](http:Caller caller, http:Request req) returns error? {
        io:println("Delete route ", routeId, " (mocked)");
        // check mongoClient->deleteById("routes", routeId);
        check caller->respond({status: "Route deleted (mocked)"});
    }

    // TRIP MANAGEMENT
    
    resource function post trips(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Create trip (mocked): ", body.toJsonString());
        // check mongoClient->insert("trips", body);
        check caller->respond({status: "Trip created (mocked)"});
    }

    resource function get trips(http:Caller caller, http:Request req) returns error? {
        io:println("Fetch all trips (mocked)");
        // json[] trips = await mongoClient->find("trips", {});
        json payload = [];
    check caller->respond(payload);
        
    }

    resource function put trips/[string tripId](http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Update trip ", tripId, " (mocked): ", body.toJsonString());
        // check mongoClient->updateById("trips", tripId, body);
        check caller->respond({status: "Trip updated (mocked)"});
    }

    resource function delete trips/[string tripId](http:Caller caller, http:Request req) returns error? {
        io:println("Delete trip ", tripId, " (mocked)");
        // check mongoClient->deleteById("trips", tripId);
        check caller->respond({status: "Trip deleted (mocked)"});
    }

    resource function patch trips/[string tripId]/status(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Trip status updated (mocked): ", body.toJsonString());
        // Kafka: check kafkaProducer->send({topic: "notifications", value: body});
        // DB: check mongoClient->updateById("trips", tripId, body);
        check caller->respond({status: "Trip status updated (mocked)"});
    }

   
    // SERVICE DISRUPTIONS
    
    resource function post disruptions(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Service disruption published (mocked): ", body.toJsonString());
        // Kafka: check kafkaProducer->send({topic: "notifications", value: body});
        // DB: check mongoClient->insert("disruptions", body);
        check caller->respond({status: "Disruption published (mocked)"});
    }

  
    // REPORTS
    
    resource function get reports(http:Caller caller, http:Request req) returns error? {
        io:println("Admin requested reports");
        // DB: json[] reports = await mongoClient->find("reports", {});
       json payload = [];
    check caller->respond(payload);
    }
}
