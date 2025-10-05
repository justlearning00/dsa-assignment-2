import ballerina/http;
import ballerina/log;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/time;

type ScheduleUpdate record {
    string eventType;
    string resourceId;
    json data;
    time:Utc timestamp;
};

type ServiceDisruption record {
    string disruptionId;
    string routeId;
    string severity;
    string description;
    time:Utc timestamp;
};

service /admin on new http:Listener(8089) {
  
    // Service-level fields
    mongodb:Client mongoClient;
    mongodb:Database transportDb;
    mongodb:Collection routesCollection;
    mongodb:Collection tripsCollection;
    mongodb:Collection disruptionsCollection;
    mongodb:Collection reportsCollection;

    kafka:Producer kafkaProducer;

    // Service initialization
    isolated function init() returns error? {
        log:printInfo("Initializing Admin Service...");

        // Initialize MongoDB client and collections
      self.mongoClient = check new ({
    connection: "mongodb://localhost:27017/transport_db"
});

        self.transportDb = check self.mongoClient->getDatabase("transport_db");
        self.routesCollection = check self.transportDb->getCollection("routes");
        self.tripsCollection = check self.transportDb->getCollection("trips");
        self.disruptionsCollection = check self.transportDb->getCollection("disruptions");
        self.reportsCollection = check self.transportDb->getCollection("reports");

        // Initialize Kafka producer
        self.kafkaProducer = check new ("localhost:9092");

        log:printInfo("Admin Service initialized successfully");
    }

    // ROUTE MANAGEMENT

    isolated resource function post routes(http:Caller caller, http:Request req) returns error? {
        log:printInfo("POST /admin/routes - Creating route");
        json payload = check req.getJsonPayload();
        map<json> routeData = <map<json>>payload;

        check self.routesCollection->insertOne(routeData);
log:printInfo("Inserted route into MongoDB: " + routeData.toJsonString());
        string routeId = (check payload.routeId).toString();

        // Kafka notification
        ScheduleUpdate update = {
            eventType: "ROUTE_CREATED",
            resourceId: routeId,
            data: payload,
            timestamp: time:utcNow()
        };
        check self.kafkaProducer->send({topic: "schedule.updates", value: update});

        check caller->respond({status: "Route created", id: routeId});
    }

    isolated  resource function get routes(http:Caller caller, http:Request req) returns error? {
        log:printInfo("GET /admin/routes - Fetching all routes");
    
    // Find all documents 
    map<json> projection = {
    routeId: 1,
    name: 1,
    startPoint: 1,
    endPoint: 1,
    vehicleType: 1,
    fare: 1
};

stream<map<json>, error?> resultStream = check self.routesCollection->find({}, {}, projection);

    map<json>[] routes = check from map<json> route in resultStream select route;

    log:printInfo("Fetched routes: " + routes.toString());
    check caller->respond(routes);
}


    isolated resource function put routes/[string routeId](http:Caller caller, http:Request req) returns error? {
        log:printInfo("PUT /admin/routes/" + routeId + " - Updating route");
        json payload = check req.getJsonPayload();
        map<json> updateData = <map<json>>payload;

        mongodb:UpdateResult result = check self.routesCollection->updateOne(
            {routeId: routeId}, {set: updateData}
        );

        ScheduleUpdate update = {
            eventType: "ROUTE_UPDATED",
            resourceId: routeId,
            data: payload,
            timestamp: time:utcNow()
        };
        check self.kafkaProducer->send({topic: "schedule.updates", value: update});

        check caller->respond({
            status: "Route updated",
            matchedCount: result.matchedCount,
            modifiedCount: result.modifiedCount
        });
    }

    isolated resource function delete routes/[string routeId](http:Caller caller, http:Request req) returns error? {
        log:printInfo("DELETE /admin/routes/" + routeId + " - Deleting route");
        mongodb:DeleteResult result = check self.routesCollection->deleteOne({routeId: routeId});

        ScheduleUpdate update = {
            eventType: "ROUTE_DELETED",
            resourceId: routeId,
            data: {},
            timestamp: time:utcNow()
        };
        check self.kafkaProducer->send({topic: "schedule.updates", value: update});

        check caller->respond({status: "Route deleted", deletedCount: result.deletedCount});
    }

    
    // TRIP MANAGEMENT
    
    isolated resource function post trips(http:Caller caller, http:Request req) returns error? {
        log:printInfo("POST /admin/trips - Creating trip");
        json payload = check req.getJsonPayload();
        map<json> tripData = <map<json>>payload;

        check self.tripsCollection->insertOne(tripData);
        log:printInfo("Inserted trip into MongoDB: " + tripData.toJsonString());
        string tripId = (check payload.tripId).toString();

        ScheduleUpdate update = {
            eventType: "TRIP_CREATED",
            resourceId: tripId,
            data: payload,
            timestamp: time:utcNow()
        };
        check self.kafkaProducer->send({topic: "schedule.updates", value: update});
        check caller->respond({status: "Trip created", id: tripId});
    }

   isolated resource function get trips(http:Caller caller, http:Request req) returns error? {
    log:printInfo("GET /admin/trips - Fetching all trips");
    
    map<json> projection = {
        tripId: 1,
        routeId: 1,
        vehicleId: 1,
        driverId: 1,
        schedule: 1,
        status: 1,
        availableSeats: 1
    };

    stream<map<json>, error?> resultStream = check self.tripsCollection->find({}, {}, projection);
    map<json>[] trips = check from map<json> trip in resultStream select trip;

    log:printInfo("Fetched trips: " + trips.toString());
    check caller->respond(trips);
}

    isolated resource function put trips/[string tripId](http:Caller caller, http:Request req) returns error? {
        log:printInfo("PUT /admin/trips/" + tripId + " - Updating trip");
        json payload = check req.getJsonPayload();
        map<json> updateData = <map<json>>payload;

        mongodb:UpdateResult result = check self.tripsCollection->updateOne(
            {tripId: tripId}, {set: updateData}
        );

        ScheduleUpdate update = {
            eventType: "TRIP_UPDATED",
            resourceId: tripId,
            data: payload,
            timestamp: time:utcNow()
        };
        check self.kafkaProducer->send({topic: "schedule.updates", value: update});

        check caller->respond({
            status: "Trip updated",
            matchedCount: result.matchedCount,
            modifiedCount: result.modifiedCount
        });
    }

    isolated resource function delete trips/[string tripId](http:Caller caller, http:Request req) returns error? {
        log:printInfo("DELETE /admin/trips/" + tripId + " - Deleting trip");
        mongodb:DeleteResult result = check self.tripsCollection->deleteOne({tripId: tripId});

        ScheduleUpdate update = {
            eventType: "TRIP_DELETED",
            resourceId: tripId,
            data: {},
            timestamp: time:utcNow()
        };
        check self.kafkaProducer->send({topic: "schedule.updates", value: update});
        check caller->respond({status: "Trip deleted", deletedCount: result.deletedCount});
    }

    isolated resource function patch trips/[string tripId]/status(http:Caller caller, http:Request req) returns error? {
        log:printInfo("PATCH /admin/trips/" + tripId + "/status - Updating status");
        json payload = check req.getJsonPayload();
        map<json> updateData = <map<json>>payload;

        mongodb:UpdateResult result = check self.tripsCollection->updateOne(
            {tripId: tripId}, {set: updateData}
        );

        ScheduleUpdate update = {
            eventType: "TRIP_STATUS_UPDATED",
            resourceId: tripId,
            data: payload,
            timestamp: time:utcNow()
        };
        check self.kafkaProducer->send({topic: "schedule.updates", value: update});

        check caller->respond({
            status: "Trip status updated",
            matchedCount: result.matchedCount,
            modifiedCount: result.modifiedCount
        });
    }

    
    // SERVICE DISRUPTIONS
   
    isolated resource function post disruptions(http:Caller caller, http:Request req) returns error? {
        log:printInfo("POST /admin/disruptions - Creating disruption");
        json payload = check req.getJsonPayload();
        map<json> disruptionData = <map<json>>payload;

        check self.disruptionsCollection->insertOne(disruptionData);
log:printInfo("Inserted disruption into MongoDB: " + disruptionData.toJsonString());
        string routeId = (check payload.routeId).toString();
        string severity = (check payload.severity).toString();
        string description = (check payload.description).toString();
        string disruptionId = "disruption-" + routeId + "-" + time:utcNow()[0].toString();

        ServiceDisruption disruption = {
            disruptionId: disruptionId,
            routeId: routeId,
            severity: severity,
            description: description,
            timestamp: time:utcNow()
        };
        check self.kafkaProducer->send({topic: "service.disruptions", value: disruption});
        check caller->respond({status: "Disruption published", id: disruptionId});
    }

    
    // REPORTS
    resource function post reports(http:Caller caller, http:Request req) returns error? {
    log:printInfo("POST /admin/reports - Creating report");
    json payload = check req.getJsonPayload();
    map<json> reportData = <map<json>>payload;

    check self.reportsCollection->insertOne(reportData);
    log:printInfo("Inserted report into MongoDB: " + reportData.toJsonString());
    
    string reportId = (check payload.reportId).toString();
    check caller->respond({status: "Report created", id: reportId});}
     resource function get reports(http:Caller caller, http:Request req) returns error? {
    log:printInfo("GET /admin/reports - Fetching all reports");
    
    map<json> projection = {
        reportId: 1,
        'type: 1,
        period: 1,
        totalPassengers: 1,
        totalRevenue: 1,
        generatedAt: 1
    };

    stream<map<json>, error?> resultStream = check self.reportsCollection->find({}, {}, projection);
    map<json>[] reports = check from map<json> report in resultStream select report;

    log:printInfo("Fetched reports: " + reports.toString());
    check caller->respond(reports);
}
    
}
