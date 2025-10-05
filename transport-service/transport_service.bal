import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/lang.'value;

listener http:Listener httpListener = new(8083);

// Configurable variables
configurable string dbUri = "mongodb://127.0.0.1:27017";
configurable string dbName = "transport_db";
configurable string kafkaUrl = "localhost:9092";

final PaymentRepository repo = check getPaymentRepository();
final kafka:Producer kafkaProducer = check getKafkaProducer();

public type Route record {
    string routeId;
    string startLocation;
    string endLocation;
    decimal distance;
    decimal estimatedCost;
    string createdTime;
    string? updatedTime;
};

public type Trip record {
    string tripId;
    string routeId;
    string vehicleId;
    string driverId;
    string status;
    string scheduledTime;
    string? actualStartTime;
    string? actualEndTime;
    decimal actualCost;
    string createdTime;
};

public type Payment record {
    string paymentId;
    string tripId;
    string passengerId;
    decimal amount;
    string currency;
    string status;
    string paymentMethod;
    string? transactionId;
    string createdTime;
    string? updatedTime;
};

public type TripStatusUpdate record {
    string status;
    string? reason;
};

public type PaymentStatusUpdate record {
    string status;
    string? transactionId;
};

// Helper functions to convert records to JSON
isolated function routeToJson(Route route) returns json {
    return {
        routeId: route.routeId,
        startLocation: route.startLocation,
        endLocation: route.endLocation,
        distance: route.distance,
        estimatedCost: route.estimatedCost,
        createdTime: route.createdTime,
        updatedTime: route.updatedTime
    };
}

isolated function tripToJson(Trip trip) returns json {
    return {
        tripId: trip.tripId,
        routeId: trip.routeId,
        vehicleId: trip.vehicleId,
        driverId: trip.driverId,
        status: trip.status,
        scheduledTime: trip.scheduledTime,
        actualStartTime: trip.actualStartTime,
        actualEndTime: trip.actualEndTime,
        actualCost: trip.actualCost,
        createdTime: trip.createdTime
    };
}

isolated function paymentToJson(Payment payment) returns json {
    return {
        paymentId: payment.paymentId,
        tripId: payment.tripId,
        passengerId: payment.passengerId,
        amount: payment.amount,
        currency: payment.currency,
        status: payment.status,
        paymentMethod: payment.paymentMethod,
        transactionId: payment.transactionId,
        createdTime: payment.createdTime,
        updatedTime: payment.updatedTime
    };
}

public isolated class PaymentRepository {
    private final mongodb:Collection routes;
    private final mongodb:Collection trips;
    private final mongodb:Collection payments;

    public isolated function init(mongodb:Collection routes, mongodb:Collection trips, mongodb:Collection payments) {
        self.routes = routes;
        self.trips = trips;
        self.payments = payments;
    }

    private isolated function createUpdate(string operator, map<json> updateData) returns mongodb:Update {
        return {[operator]: updateData};
    }

    public isolated function createRoute(Route route) returns Route|error {
        check self.routes->insertOne(route);
        return route;
    }

    public isolated function getAllRoutes() returns Route[]|error {
        stream<Route, error?> resultStream = check self.routes->find();
        Route[] routes = [];
        record {| Route value; |}|error? next = resultStream.next();
        while next is record {| Route value; |} {
            routes.push(next.value);
            next = resultStream.next();
        }
        check resultStream.close();
        return routes;
    }

    public isolated function getRoute(string routeId) returns Route|error {
        Route? result = check self.routes->findOne({"routeId": routeId});
        if result is () {
            return error("ROUTE_NOT_FOUND");
        }
        return result;
    }

    public isolated function updateRoute(string routeId, Route route) returns Route|error {
        map<json> routeMap = <map<json>>'value:toJson(route);
        mongodb:Update updateObj = self.createUpdate("set", routeMap);
        mongodb:UpdateResult result = check self.routes->updateOne({"routeId": routeId}, updateObj);
        if result.matchedCount == 0 {
            return error("ROUTE_NOT_FOUND");
        }
        return route;
    }

    public isolated function deleteRoute(string routeId) returns error? {
        mongodb:DeleteResult result = check self.routes->deleteOne({"routeId": routeId});
        if result.deletedCount == 0 {
            return error("ROUTE_NOT_FOUND");
        }
    }

    public isolated function createTrip(Trip trip) returns Trip|error {
        check self.trips->insertOne(trip);
        return trip;
    }

    public isolated function getAllTrips() returns Trip[]|error {
        stream<Trip, error?> resultStream = check self.trips->find();
        Trip[] trips = [];
        record {| Trip value; |}|error? next = resultStream.next();
        while next is record {| Trip value; |} {
            trips.push(next.value);
            next = resultStream.next();
        }
        check resultStream.close();
        return trips;
    }

    public isolated function getTrip(string tripId) returns Trip|error {
        Trip? result = check self.trips->findOne({"tripId": tripId});
        if result is () {
            return error("TRIP_NOT_FOUND");
        }
        return result;
    }

    public isolated function updateTripStatus(string tripId, TripStatusUpdate statusUpdate) returns Trip|error {
        mongodb:Update updateObj = self.createUpdate("set", {"status": statusUpdate.status});
        mongodb:UpdateResult result = check self.trips->updateOne({"tripId": tripId}, updateObj);
        if result.matchedCount == 0 {
            return error("TRIP_NOT_FOUND");
        }
        return self.getTrip(tripId);
    }

    public isolated function createPayment(string tripId, Payment payment) returns Payment|error {
        Trip? trip = check self.trips->findOne({"tripId": tripId});
        if trip is () {
            return error("TRIP_NOT_FOUND");
        }
        check self.payments->insertOne(payment);
        return payment;
    }

    public isolated function getTripPayments(string tripId) returns Payment[]|error {
        stream<Payment, error?> resultStream = check self.payments->find({"tripId": tripId});
        Payment[] payments = [];
        record {| Payment value; |}|error? next = resultStream.next();
        while next is record {| Payment value; |} {
            payments.push(next.value);
            next = resultStream.next();
        }
        check resultStream.close();
        return payments;
    }

    public isolated function updatePaymentStatus(string paymentId, PaymentStatusUpdate statusUpdate) returns Payment|error {
        mongodb:Update updateObj = self.createUpdate("set", {"status": statusUpdate.status});
        mongodb:UpdateResult result = check self.payments->updateOne({"paymentId": paymentId}, updateObj);
        if result.matchedCount == 0 {
            return error("PAYMENT_NOT_FOUND");
        }
        Payment? payment = check self.payments->findOne({"paymentId": paymentId});
        if payment is () {
            return error("PAYMENT_NOT_FOUND");
        }
        return payment;
    }
}

// Get MongoDB client
isolated function getMongoClient() returns mongodb:Client|error {
    mongodb:ConnectionConfig config = {
        connection: dbUri
    };
    log:printInfo("Connecting to MongoDB at: " + dbUri);
    return check new (config);
}

// Initialize repository with collections
isolated function getPaymentRepository() returns PaymentRepository|error {
    mongodb:Client mongoClient = check getMongoClient();
    mongodb:Database database = check mongoClient->getDatabase(dbName);
    log:printInfo("Connected to database: " + dbName);
    
    mongodb:Collection routes = check database->getCollection("routes");
    mongodb:Collection trips = check database->getCollection("trips");
    mongodb:Collection payments = check database->getCollection("payments");
    
    return new(routes, trips, payments);
}

// Get Kafka producer
isolated function getKafkaProducer() returns kafka:Producer|error {
    log:printInfo("Connecting to Kafka at: " + kafkaUrl);
    return check new (kafkaUrl);
}

service /api on httpListener {
    isolated resource function post routes(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        Route route = check payload.cloneWithType();
        Route created = check repo.createRoute(route);
        check kafkaProducer->send({topic: "route.created", value: routeToJson(created).toJsonString()});
        json response = {"message": "Route created successfully", "route": routeToJson(created)};
        check caller->respond(response);
    }

    isolated resource function get routes(http:Caller caller, http:Request req) returns error? {
        Route[] routes = check repo.getAllRoutes();
        json[] routeJsonArray = from Route route in routes select routeToJson(route);
        check caller->respond(routeJsonArray);
    }

    isolated resource function get routes/[string routeId](http:Caller caller, http:Request req) returns error? {
        Route|error routeResult = repo.getRoute(routeId);
        if routeResult is error {
            json errorResponse = {"error": "Route not found", "routeId": routeId};
            check caller->respond(errorResponse);
            return;
        }
        check caller->respond(routeToJson(routeResult));
    }

    isolated resource function put routes/[string routeId](http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        Route route = check payload.cloneWithType();
        Route|error updated = repo.updateRoute(routeId, route);
        if updated is error {
            json errorResponse = {"error": "Failed to update route", "routeId": routeId};
            check caller->respond(errorResponse);
            return;
        }
        check kafkaProducer->send({topic: "route.updated", value: routeToJson(updated).toJsonString()});
        json response = {"message": "Route updated successfully", "route": routeToJson(updated)};
        check caller->respond(response);
    }

    isolated resource function delete routes/[string routeId](http:Caller caller, http:Request req) returns error? {
        error? deleteResult = repo.deleteRoute(routeId);
        if deleteResult is error {
            json errorResponse = {"error": "Failed to delete route", "routeId": routeId};
            check caller->respond(errorResponse);
            return;
        }
        check kafkaProducer->send({topic: "route.deleted", value: {"routeId": routeId}.toJsonString()});
        json response = {"message": "Route deleted successfully", "routeId": routeId};
        check caller->respond(response);
    }

    isolated resource function post trips(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        Trip trip = check payload.cloneWithType();
        Trip created = check repo.createTrip(trip);
        check kafkaProducer->send({topic: "trip.created", value: tripToJson(created).toJsonString()});
        json response = {"message": "Trip created successfully", "trip": tripToJson(created)};
        check caller->respond(response);
    }

    isolated resource function get trips(http:Caller caller, http:Request req) returns error? {
        Trip[] trips = check repo.getAllTrips();
        json[] tripJsonArray = from Trip trip in trips select tripToJson(trip);
        check caller->respond(tripJsonArray);
    }

    isolated resource function get trips/[string tripId](http:Caller caller, http:Request req) returns error? {
        Trip|error tripResult = repo.getTrip(tripId);
        if tripResult is error {
            json errorResponse = {"error": "Trip not found", "tripId": tripId};
            check caller->respond(errorResponse);
            return;
        }
        check caller->respond(tripToJson(tripResult));
    }

    isolated resource function patch trips/[string tripId]/status(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        TripStatusUpdate statusUpdate = check payload.cloneWithType();
        Trip|error updated = repo.updateTripStatus(tripId, statusUpdate);
        if updated is error {
            json errorResponse = {"error": "Failed to update trip status", "tripId": tripId};
            check caller->respond(errorResponse);
            return;
        }
        check kafkaProducer->send({topic: "trip.status.updated", value: {"tripId": tripId, "status": statusUpdate.status, "updatedTrip": tripToJson(updated)}.toJsonString()});
        json response = {"message": "Trip status updated successfully", "trip": tripToJson(updated)};
        check caller->respond(response);
    }

    isolated resource function post trips/[string tripId]/payments(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        Payment payment = check payload.cloneWithType();
        Payment created = check repo.createPayment(tripId, payment);
        check kafkaProducer->send({topic: "payment.created", value: paymentToJson(created).toJsonString()});
        json response = {"message": "Payment created successfully", "payment": paymentToJson(created)};
        check caller->respond(response);
    }

    isolated resource function get trips/[string tripId]/payments(http:Caller caller, http:Request req) returns error? {
        Payment[] payments = check repo.getTripPayments(tripId);
        json[] paymentJsonArray = from Payment payment in payments select paymentToJson(payment);
        check caller->respond(paymentJsonArray);
    }

    isolated resource function patch payments/[string paymentId]/status(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        PaymentStatusUpdate statusUpdate = check payload.cloneWithType();
        Payment|error updated = repo.updatePaymentStatus(paymentId, statusUpdate);
        if updated is error {
            json errorResponse = {"error": "Failed to update payment status", "paymentId": paymentId};
            check caller->respond(errorResponse);
            return;
        }
        check kafkaProducer->send({topic: "payment.status.updated", value: {"paymentId": paymentId, "status": statusUpdate.status, "updatedPayment": paymentToJson(updated)}.toJsonString()});
        json response = {"message": "Payment status updated successfully", "payment": paymentToJson(updated)};
        check caller->respond(response);
    }

    isolated resource function get health(http:Caller caller, http:Request req) returns error? {
        json response = {"status": "healthy", "service": "Payment Service API", "timestamp": time:utcNow()};
        check caller->respond(response);
    }
}

public function main() returns error? {
    log:printInfo("Payment Service API server started on port 8083");
}