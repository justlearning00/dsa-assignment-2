import ballerina/http;
import ballerina/io;
import ballerinax/mongodb;
import ballerinax/kafka;

mongodb:Client mongoClient = check new ({
    connection: "mongodb://root:password@localhost:27017/transport_db"});

// Placeholder for Kafka Consumer
 kafka:Consumer consumer = check new ("kafka1:19092");

service /notifications on new http:Listener(8083) {

    
    // Send a notification 
   
    resource function post send(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Notification sent (mocked): ", body.toJsonString());
        // Kafka: publish notification
        // check kafkaProducer->send({topic: "notifications", value: body});
  
        // DB: insert into notifications collection
        // check mongoClient->insert("notifications", body);
       

        check caller->respond({status: "Notification stored (mocked)"});
    }

    
    // Get all notifications 
    
    resource function get all(http:Caller caller, http:Request req) returns error? {
        io:println("Fetching all notifications (mocked)");

       
        // DB: query notifications collection
        // json[] notifications = await mongoClient->find("notifications", {});
        json payload = [];
    check caller->respond(payload);
    }

    //  Get notifications for a specific user
   
    resource function get user/[string userId](http:Caller caller, http:Request req) returns error? {
        io:println("Fetching notifications for user: ", userId);

        
        // DB: query notifications collection with userId filter
        // json[] notifications = await mongoClient->find("notifications", {userId: userId});
    json payload = [];
    check caller->respond(payload);
        
    }

    
    //  Mark notification as read
    
    resource function put user/[string userId]/[string notificationId]/read(http:Caller caller, http:Request req) returns error? {
        io:println("Marking notification ", notificationId, " as read for user ", userId);

        
        // DB: update notification document
        // check mongoClient->updateById("notifications", notificationId, {read: true})
        check caller->respond({status: "Notification marked as read (mocked)"});
    }
}
