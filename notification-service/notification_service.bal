import ballerina/http;
import ballerina/io;

// -----------------------------
// Mock Data (replace with DB later)
// -----------------------------
type Notification record {
    int id;
    int userId;
    string message;
    boolean read;
};

Notification[] notifications = [
    {id: 1, userId: 101, message: "Your ticket has been validated", read: false},
    {id: 2, userId: 101, message: "Train 5 delayed by 10 minutes", read: false},
    {id: 3, userId: 102, message: "Your monthly pass is expiring soon", read: true},
    {id: 4, userId: 103, message: "Bus route 12 cancelled", read: false}
];

// -----------------------------
// REST API
// -----------------------------
service /notification on new http:Listener(8085) {

    // Fetch notifications for a specific user
    resource function get user/[int userId](http:Caller caller, http:Request req) returns error? {
        Notification[] userNotifications = [];
        foreach var n in notifications {
            if n.userId == userId {
                userNotifications.push(n);
            }
        }
        io:println("Fetched notifications for user: ", userId.toString());
        check caller->respond(userNotifications);
    }

    // Send a new notification (mock/test)
    resource function post send(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("New notification received: ", body.toJsonString());

        // -----------------------------
        // Placeholder: insert into DB here
        // Example: await dbClient->insert("notifications", body);
        // -----------------------------

        // -----------------------------
        // Placeholder: publish to Kafka topic here
        // Example: kafkaProducer->publish("notifications", body);
        // -----------------------------

        check caller->respond({status: "Notification sent (mocked)"});
    }
}
