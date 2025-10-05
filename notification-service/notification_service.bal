import ballerina/http;
import ballerina/log;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/time;


service /notifications on new http:Listener(8085) {
  
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
// Temporarily remove authentication to test
connection: "mongodb://localhost:27017/transport_db"
});
        self.transportDb = check self.mongoClient->getDatabase("transport_db");
        self.notificationsCollection = check self.transportDb->getCollection("notifications");

        
        
        // Initialize Kafka consumer to listen to admin service events
        self.kafkaConsumer = check new ("localhost:9092", {
            groupId: "notification-consumer-group",
            topics: ["schedule.updates", "service.disruptions","payment.status","passenger.trip.requests","ticket.requests","payment.requests","ticket.purchased", "ticket.validated"]
        });

        log:printInfo("Notification Service initialized successfully");
          _ = start self.processKafkaMessages();

    }

    // Process Kafka messages 
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
resource function get all(http:Caller caller, http:Request req) returns error? {
    log:printInfo("GET /notifications/all - Fetching all notifications");
    
    map<json> projection = {
        notificationId: 1,
        userId: 1,
        message: 1,
        'type: 1,
        read: 1,
        timestamp: 1
    };

    stream<map<json>, error?> resultStream = check self.notificationsCollection->find({}, {}, projection);
    map<json>[] notifications = check from map<json> notif in resultStream select notif;

    log:printInfo("Fetched notifications: " + notifications.toString());
    check caller->respond(notifications);
}

// Get user notifications
resource function get user/[string userId](http:Caller caller, http:Request req) returns error? {
    log:printInfo("GET /notifications/user/" + userId);
    
    map<json> projection = {
        notificationId: 1,
        userId: 1,
        message: 1,
        'type: 1,
        read: 1,
        timestamp: 1
    };

    stream<map<json>, error?> resultStream = check self.notificationsCollection->find({userId: userId}, {}, projection);
    map<json>[] notifications = check from map<json> notif in resultStream select notif;

    log:printInfo("Fetched user notifications: " + notifications.toString());
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
    log:printInfo("Starting Notification Service on port 8085...");

  
    
}