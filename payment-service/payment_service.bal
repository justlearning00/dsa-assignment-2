
import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/lang.'value;

listener http:Listener httpListener = new(8084);

final PaymentRepository repo = check getPaymentRepository();
final kafka:Producer kafkaProducer = check new ("kafka1:19092");

type Payment record {|
    string paymentId;
    string ticketId;
    string passengerId;
    decimal amount;
    string currency;
    string status; // PENDING, SUCCESS, FAILED
    string paymentMethod;
    string? transactionId;
    time:Utc createdAt;
    time:Utc? updatedAt;
|};

type PaymentRequest record {|
    string ticketId;
    string passengerId;
    string tripId;
    string ticketType;
    decimal amount;
    time:Utc timestamp;
|};

type PaymentStatus record {|
    string paymentId;
    string ticketId;
    string passengerId;
    string status; // SUCCESS, FAILED
    decimal amount;
    time:Utc timestamp;
|};

type PaymentResponse record {|
    string paymentId;
    string status;
    string? transactionId;
    string message;
|};

isolated function toJson(Payment payment) returns json {
    return {
        paymentId: payment.paymentId,
        ticketId: payment.ticketId,
        passengerId: payment.passengerId,
        amount: payment.amount,
        currency: payment.currency,
        status: payment.status,
        paymentMethod: payment.paymentMethod,
        transactionId: payment.transactionId,
        createdAt: payment.createdAt.toString(),
        updatedAt: payment.updatedAt is time:Utc ? payment.updatedAt.toString() : ()
    };
}

public isolated class PaymentRepository {
    private final mongodb:Collection payments;

    public isolated function init(mongodb:Collection payments) {
        self.payments = payments;
    }

    private isolated function createUpdate(string operator, map<json> updateData) returns mongodb:Update {
        return {[operator]: updateData};
    }

    public isolated function createPayment(Payment payment) returns Payment|error {
        check self.payments->insertOne(payment);
        return payment;
    }

    public isolated function getPayment(string paymentId) returns Payment|error {
        Payment? result = check self.payments->findOne({paymentId: paymentId});
        if result is () {
            return error("PAYMENT_NOT_FOUND");
        }
        return result;
    }

    public isolated function getPaymentsByTicket(string ticketId) returns Payment[]|error {
        stream<Payment, error?> resultStream = check self.payments->find({ticketId: ticketId});
        Payment[] payments = [];
        record {| Payment value; |}|error? next = resultStream.next();
        while next is record {| Payment value; |} {
            payments.push(next.value);
            next = resultStream.next();
        }
        check resultStream.close();
        return payments;
    }

    public isolated function updatePaymentStatus(string paymentId, string status, string? transactionId) returns Payment|error {
        map<json> updateData = {
            status: status,
            updatedAt: time:utcNow().toString()
        };
        
        if transactionId is string {
            updateData["transactionId"] = transactionId;
        }

        mongodb:Update updateObj = self.createUpdate("$set", updateData);
        mongodb:UpdateResult result = check self.payments->updateOne({paymentId: paymentId}, updateObj);
        
        if result.matchedCount == 0 {
            return error("PAYMENT_NOT_FOUND");
        }
        
        return self.getPayment(paymentId);
    }

    public isolated function getPassengerPayments(string passengerId) returns Payment[]|error {
        stream<Payment, error?> resultStream = check self.payments->find({passengerId: passengerId});
        Payment[] payments = [];
        record {| Payment value; |}|error? next = resultStream.next();
        while next is record {| Payment value; |} {
            payments.push(next.value);
            next = resultStream.next();
        }
        check resultStream.close();
        return payments;
    }
}

isolated function getPaymentRepository() returns PaymentRepository|error {
    mongodb:Client mongoClient = check new ({
        connection: "mongodb://root:password@mongodb:27017/payment_db"
    });
    
    mongodb:Database database = check mongoClient->getDatabase("payment_db");
    mongodb:Collection payments = check database->getCollection("payments");
    return new(payments);
}

function generatePaymentId() returns string {
    time:Utc now = time:utcNow();
    return "PAY-" + now[0].toString() + now[1].toString() + now[2].toString();
}

function generateTransactionId() returns string {
    time:Utc now = time:utcNow();
    return "TXN-" + now[0].toString() + now[1].toString() + now[2].toString();
}

// Simulate payment processing - in real scenario, integrate with payment gateway
function processPayment(decimal amount, string paymentMethod) returns boolean {
    // Simulate payment processing logic
    // For demo purposes, 90% success rate
    float random = <float>time:utcNow()[2] / 1000000000; // Use nanoseconds for randomness
    return random < 0.9; // 90% success rate
}

// Kafka Consumer - listens for payment requests from ticketing service
kafka:Consumer kafkaConsumer = check new ("kafka1:19092", {
    groupId: "payment-service",
    topics: ["payment.requests"]
});

service kafka:Service on kafkaConsumer {
    isolated remote function onConsumerRecord(kafka:ConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            PaymentRequest paymentReq = check value:fromJson(kafkaRecord.value);
            log:printInfo("Received payment request for ticket: " + paymentReq.ticketId);
            
            string paymentId = generatePaymentId();
            time:Utc createdAt = time:utcNow();
            
            // Create payment record
            Payment payment = {
                paymentId: paymentId,
                ticketId: paymentReq.ticketId,
                passengerId: paymentReq.passengerId,
                amount: paymentReq.amount,
                currency: "NAD", // Namibian Dollars
                status: "PENDING",
                paymentMethod: "CARD", // Default for demo
                createdAt: createdAt,
                updatedAt: ()
            };
            
            Payment created = check repo.createPayment(payment);
            
            // Simulate payment processing
            boolean paymentSuccess = processPayment(paymentReq.amount, "CARD");
            string status = paymentSuccess ? "SUCCESS" : "FAILED";
            string? transactionId = paymentSuccess ? generateTransactionId() : ();
            
            // Update payment status
            Payment updated = check repo.updatePaymentStatus(paymentId, status, transactionId);
            
            // Send payment status back to ticketing service
            PaymentStatus paymentStatus = {
                paymentId: paymentId,
                ticketId: paymentReq.ticketId,
                passengerId: paymentReq.passengerId,
                status: status,
                amount: paymentReq.amount,
                timestamp: time:utcNow()
            };
            
            check kafkaProducer->send({
                topic: "payment.status", 
                value: paymentStatus
            });
            
            log:printInfo("Payment processed: " + paymentId + " - " + status);
        }
    }
}

service /payments on httpListener {
    isolated resource function get [string paymentId](http:Caller caller) returns error? {
        Payment|error paymentResult = repo.getPayment(paymentId);
        if paymentResult is error {
            json errorResponse = {"error": "Payment not found", "paymentId": paymentId};
            check caller->respond(errorResponse);
            return;
        }
        check caller->respond(toJson(paymentResult));
    }

    isolated resource function get ticket/[string ticketId](http:Caller caller) returns error? {
        Payment[]|error paymentsResult = repo.getPaymentsByTicket(ticketId);
        if paymentsResult is error {
            json errorResponse = {"error": "Failed to fetch payments", "ticketId": ticketId};
            check caller->respond(errorResponse);
            return;
        }
        json[] paymentJsonArray = [];
        foreach var payment in paymentsResult {
            paymentJsonArray.push(toJson(payment));
        }
        check caller->respond(paymentJsonArray);
    }

    isolated resource function get passenger/[string passengerId](http:Caller caller) returns error? {
        Payment[]|error paymentsResult = repo.getPassengerPayments(passengerId);
        if paymentsResult is error {
            json errorResponse = {"error": "Failed to fetch payments", "passengerId": passengerId};
            check caller->respond(errorResponse);
            return;
        }
        json[] paymentJsonArray = [];
        foreach var payment in paymentsResult {
            paymentJsonArray.push(toJson(payment));
        }
        check caller->respond(paymentJsonArray);
    }

    isolated resource function post process(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        string ticketId = check payload.ticketId.toString();
        string passengerId = check payload.passengerId.toString();
        decimal amount = <decimal>check payload.amount;
        string paymentMethod = check payload.paymentMethod.toString();
        
        string paymentId = generatePaymentId();
        time:Utc createdAt = time:utcNow();
        
        // Create payment
        Payment payment = {
            paymentId: paymentId,
            ticketId: ticketId,
            passengerId: passengerId,
            amount: amount,
            currency: "NAD",
            status: "PENDING",
            paymentMethod: paymentMethod,
            createdAt: createdAt,
            updatedAt: ()
        };
        
        Payment created = check repo.createPayment(payment);
        
        // Process payment
        boolean paymentSuccess = processPayment(amount, paymentMethod);
        string status = paymentSuccess ? "SUCCESS" : "FAILED";
        string? transactionId = paymentSuccess ? generateTransactionId() : ();
        
        Payment updated = check repo.updatePaymentStatus(paymentId, status, transactionId);
        
        // Send to Kafka for ticketing service
        PaymentStatus paymentStatus = {
            paymentId: paymentId,
            ticketId: ticketId,
            passengerId: passengerId,
            status: status,
            amount: amount,
            timestamp: time:utcNow()
        };
        
        check kafkaProducer->send({
            topic: "payment.status", 
            value: paymentStatus
        });
        
        PaymentResponse response = {
            paymentId: paymentId,
            status: status,
            transactionId: transactionId,
            message: status == "SUCCESS" ? "Payment processed successfully" : "Payment failed"
        };
        
        check caller->respond(response);
    }

    isolated resource function get health() returns json {
        return {"status": "healthy", "service": "Payment Service", "timestamp": time:utcNow().toString()};
    }
}

public function main() returns error? {
    log:printInfo("Payment Service started on port 8084");
}