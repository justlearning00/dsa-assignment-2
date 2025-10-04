import ballerina/http;
import ballerina/log;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/time;


service /notifications on new http:Listener(8083) {
  
    // Service-level fields
    mongodb:Client mongoClient;
    mongodb:Database transportDb;
    mongodb:Collection notificationsCollection;
    
    kafka:Consumer kafkaConsumer;

    // Service initialization
     function init() returns error? {
        log:printInfo("Initializing Notification Service...");

        // Initialize MongoDB
        self.mongoClient = check new ({
            connection: "mongodb://root:password@mongodb:27017/transport_db"
        });

        self.transportDb = check self.mongoClient->getDatabase("transport_db");
        self.notificationsCollection = check self.transportDb->getCollection("notifications");

        
        
        // Initialize Kafka consumer to listen to admin service events
        self.kafkaConsumer = check new (kafka:DEFAULT_URL, {
            groupId: "notification-consumer-group",
            topics: ["schedule.updates", "service.disruptions"]
        });

        log:printInfo("Notification Service initialized successfully");
          _ = start self.processKafkaMessages();

    }

    // Process Kafka messages from admin service
    isolated function processKafkaMessages() returns error? {
        while true {
            kafka:AnydataConsumerRecord[] records = check self.kafkaConsumer->poll(1000);
            
            foreach var rec in records {
                byte[] valueBytes = <byte[]>rec.value;
string message = check string:fromBytes(valueBytes);
json eventData = check message.fromJsonString();

                // Create notification based on event
                map<json> notification = {
                     notificationId: "kafka-" + time:utcNow()[0].toString(),
                    userId: "all", // Or extract from event
                    message: "Event: " + (check eventData.eventType).toString(),
                    'type: (check eventData.eventType).toString(),
                    read: false,
                    timestamp: time:utcNow(),
                    eventData: eventData
                };
                
                check self.notificationsCollection->insertOne(notification);
                log:printInfo("Created notification from Kafka event");
            }
        }
    }

    // Send a notification
    isolated resource function post send(http:Caller caller, http:Request req) returns error? {
        log:printInfo("POST /notifications/send - Creating notification");
        
        json payload = check req.getJsonPayload();
        map<json> notificationData = <map<json>>payload;
        
        // Store in database
        check self.notificationsCollection->insertOne(notificationData);
        check caller->respond({status: "Notification sent"});
    }

    // Get all notifications
    isolated resource function get all(http:Caller caller, http:Request req) returns error? {
        log:printInfo("GET /notifications/all - Fetching all notifications");

        stream<map<json>, error?> resultStream = check self.notificationsCollection->find();
        map<json>[] notifications = check from map<json> notif in resultStream select notif;
        
        check caller->respond(notifications);
    }

    // Get notifications for a user
    isolated resource function get user/[string userId](http:Caller caller, http:Request req) returns error? {
        log:printInfo("GET /notifications/user/" + userId);

        stream<map<json>, error?> resultStream = check self.notificationsCollection->find(
            filter = {userId: userId}
        );
        map<json>[] notifications = check from map<json> notif in resultStream select notif;
        
        check caller->respond(notifications);
    }

    // Mark notification as read
    isolated resource function put [string notificationId]/read(http:Caller caller, http:Request req) returns error? {
        log:printInfo("PUT /notifications/" + notificationId + "/read");

        mongodb:UpdateResult result = check self.notificationsCollection->updateOne(
            {notificationId: notificationId},
            {set: {read: true}}
        );

        check caller->respond({
            status: "Notification marked as read",
            modifiedCount: result.modifiedCount
        });
    }

    // Delete a notification
    isolated resource function delete [string notificationId](http:Caller caller, http:Request req) returns error? {
        log:printInfo("DELETE /notifications/" + notificationId);

        mongodb:DeleteResult result = check self.notificationsCollection->deleteOne(
            {notificationId: notificationId}
        );

        check caller->respond({
            status: "Notification deleted",
            deletedCount: result.deletedCount
        });
    }

}

public function main() returns error? {
    log:printInfo("Starting Notification Service on port 8083...");

    // Start Kafka consumer in background
    // Note: In production, you'd run this in a separate worker or service
    // For now, you need to call processKafkaMessages() 
    
}