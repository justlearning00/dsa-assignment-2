
import ballerina/http;
import ballerina/io;
// import ballerinax/kafka;
// import ballerinax/mongodb;

// TODO: Uncomment when Kafka is setup
// final kafka:Producer paymentProducer = check new ({bootstrapServers: "localhost:9092"});
// final kafka:Consumer paymentConsumer = check new ({
//     bootstrapServers: "localhost:9092",
//     groupId: "payment-group",
//     topics: ["payment.requests"]
// });

// TODO: Uncomment when MongoDB is setup  
// final mongodb:Client mongoClient = check new ({
//     connection: "mongodb://root:password@localhost:27017/payment_db"
// });

// HTTP API - This works without Kafka/MongoDB
service /payments on new http:Listener(8083) {

    // Simple payment processing endpoint
    resource function post process(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Processing payment: ", body.toJsonString());
        
        // Extract payment details
        string ticketId = check body.ticketId;
        string passengerId = check body.passengerId;
        decimal amount = check body.amount;
        
        // Simulate payment processing
        boolean paymentSuccess = simulatePaymentProcessing();
        
        if paymentSuccess {
            // TODO: Uncomment when MongoDB is ready
            // json paymentRecord = {
            //     paymentId: generatePaymentId(),
            //     ticketId: ticketId,
            //     passengerId: passengerId,
            //     amount: amount,
            //     status: "COMPLETED",
            //     timestamp: time:utcNow().toString()
            // };
            // check mongoClient->insert("payments", paymentRecord);
            
            // TODO: Uncomment when Kafka is ready
            // json paymentEvent = {
            //     paymentId: paymentRecord.paymentId,
            //     ticketId: ticketId,
            //     passengerId: passengerId,
            //     status: "PAID"
            // };
            // paymentProducer->send({
            //     topic: "payments.processed",
            //     value: paymentEvent.toJsonString().toBytes()
            // });
            
            check caller->respond({
                paymentId: generatePaymentId(),
                status: "Payment processed successfully (mocked)"
            });
        } else {
            check caller->respond({
                status: "Payment failed"
            }, statusCode = 400);
        }
    }

    // Get payment status - Mocked for now
    resource function get status/[string paymentId](http:Caller caller) returns error? {
        io:println("Fetching payment status for: ", paymentId);
        
        // TODO: Replace with actual DB query when MongoDB is ready
        // json? payment = check mongoClient->findById("payments", paymentId);
        
        json mockedPayment = {
            paymentId: paymentId,
            status: "COMPLETED",
            amount: 25.50,
            timestamp: "2025-10-02T10:30:00Z"
        };
        
        check caller->respond(mockedPayment);
    }
}

// TODO: Uncomment when Kafka consumer is needed
// service kafka:Service on paymentConsumer {
//     resource function onMessage(kafka:Consumer consumer, kafka:AnonRecord[] records) returns error? {
//         foreach var kafkaRecord in records {
//             json paymentRequest = check json.constructFromString(check string:fromBytes(kafkaRecord.value));
//             io:println("Received payment request from Kafka: ", paymentRequest.toJsonString());
//             processPayment(paymentRequest);
//         }
//     }
// }

// Helper functions
function simulatePaymentProcessing() returns boolean {
    // Simulate 95% success rate
    return math:random() > 0.05;
}

function generatePaymentId() returns string {
    return "PAY-" + time:utcNow().toString()[0:19].replace("T", "-").replace(":", "-");
}