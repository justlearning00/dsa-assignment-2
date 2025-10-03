import ballerina/http;
import ballerina/io;
import ballerinax/mongodb;

mongodb:Client mongoClient = check new ({
    connection: "mongodb://root:password@mongo-db:27017/transport_db"
});

// Placeholder for Kafka Producer (when passenger buys ticket)
// kafka:Producer kafkaProducer = check new (...);

service /passenger on new http:Listener(8081) {

    //  REGISTER passenger
    resource function post register(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Register passenger (mocked): ", body.toJsonString());
        // check mongoClient->insert("users", body);
        check caller->respond({status: "Passenger registered successfully"});
    }

    //  LOGIN passenger
    resource function post login(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Login attempt (mocked): ", body.toJsonString());
        // json? user = check mongoClient->findOne("users", body);
        check caller->respond({status: "Login success (mocked)"});
    }

    //  MANAGE ACCOUNT (update profile)
    resource function put account/[string userId](http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Update account ", userId, " (mocked): ", body.toJsonString());
        // check mongoClient->updateById("users", userId, body);
        check caller->respond({status: "Account updated (mocked)"});
    }

    //  VIEW tickets
    resource function get tickets/[string userId](http:Caller caller, http:Request req) returns error? {
        io:println("Fetch tickets for user ", userId, " (mocked)");
        // json[] tickets = await mongoClient->find("tickets", { userId: userId });
        json payload = [];
        check caller->respond(payload);
    }
}
