
import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/mongodb;
import ballerinax/kafka;

listener http:Listener httpListener = new(8084);

public type Payment record {|
    string paymentId;
    string ticketId;
    string passengerId;
    decimal amount;
    string currency;
    string status;
    string paymentMethod;
    string? transactionId;
    time:Utc createdAt;
    time:Utc? updatedAt;
|};

public type PaymentRequest record {|
    string ticketId;
    string passengerId;
    string tripId;
    string ticketType;
    decimal amount;
    time:Utc timestamp;
|};

public type PaymentStatus record {|
    string paymentId;
    string ticketId;
    string passengerId;
    string status;
    decimal amount;
    time:Utc timestamp;
|};

public type PaymentResponse record {|
    string paymentId;
    string status;
    string? transactionId;
    string message;
|};

function toJson(Payment payment) returns json {
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

public class PaymentRepository {
    private final mongodb:Collection payments;

    public function init(mongodb:Collection payments) {
        self.payments = payments;
    }

    private function createUpdate(string operator, map<json> updateData) returns mongodb:Update {
        return {[operator]: updateData};
    }

    public function createPayment(Payment payment) returns Payment|error {
        check self.payments->insertOne(payment);
        return payment;
    }

    public function getPayment(string paymentId) returns Payment|error {
        Payment? result = check self.payments->findOne({paymentId: paymentId});
        if result is () {
            return error("PAYMENT_NOT_FOUND: No payment found with ID " + paymentId);
        }
        return result;
    }

    public function getPaymentsByTicket(string ticketId) returns Payment[]|error {
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

    public function updatePaymentStatus(string paymentId, string status, string? transactionId) returns Payment|error {
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
            return error("PAYMENT_NOT_FOUND: No payment found with ID " + paymentId);
        }
        
        return self.getPayment(paymentId);
    }

    public function getPassengerPayments(string passengerId) returns Payment[]|error {
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

function getPaymentRepository() returns PaymentRepository|error {
    mongodb:Client mongoClient = check new ({
        connection: "mongodb://root:password@mongodb:27017/payment_db"
    });
    
    mongodb:Database database = check mongoClient->getDatabase("payment_db");
    mongodb:Collection payments = check database->getCollection("payments");
    return new(payments);
}

function generatePaymentId() returns string {
    time:Utc now = time:utcNow();
    return "PAY-" + now[0].toString() + now[1].toString();
}

function generateTransactionId() returns string {
    time:Utc now = time:utcNow();
    return "TXN-" + now[0].toString() + now[1].toString();
}

function processPayment(decimal amount, string paymentMethod) returns boolean {
    decimal nanos = time:utcNow()[1];
    float random = <float>nanos;
    return random % 10.0 < 9.0;
}

function validatePaymentInput(string ticketId, string passengerId, decimal amount, string paymentMethod) returns error? {
    if ticketId.trim().length() == 0 {
        return error("Invalid input: ticketId cannot be empty");
    }
    if passengerId.trim().length() == 0 {
        return error("Invalid input: passengerId cannot be empty");
    }
    if amount <= 0.0d {
        return error("Invalid input: amount must be positive");
    }
    if paymentMethod.trim().length() == 0 {
        return error("Invalid input: paymentMethod cannot be empty");
    }
}

service /payments on httpListener {
    
    PaymentRepository repo;
    kafka:Producer kafkaProducer;
    kafka:Consumer kafkaConsumer;
    
    function init() returns error? {
        log:printInfo("Initializing Payment Service...");
        
        self.repo = check getPaymentRepository();
        self.kafkaProducer = check new ("localhost:9092");
        self.kafkaConsumer = check new (kafka:DEFAULT_URL, {
            groupId: "payment-service",
            topics: ["payment.requests"]
        });
        
        log:printInfo("Payment Service initialized successfully");
        _ = start self.processPaymentRequests();
    }
    
    function processPaymentRequests() returns error? {
        while true {
            do {
                kafka:AnydataConsumerRecord[] records = check self.kafkaConsumer->poll(1000);
                
                foreach var rec in records {
                    byte[] valueBytes = <byte[]>rec.value;
                    string jsonString = check string:fromBytes(valueBytes);
                    json paymentData = check jsonString.fromJsonString();
                    PaymentRequest paymentReq = check paymentData.cloneWithType();
                    
                    log:printInfo("Received payment request for ticket: " + paymentReq.ticketId);
                    
                    string paymentId = generatePaymentId();
                    time:Utc createdAt = time:utcNow();
                    
                    Payment payment = {
                        paymentId: paymentId,
                        ticketId: paymentReq.ticketId,
                        passengerId: paymentReq.passengerId,
                        amount: paymentReq.amount,
                        currency: "NAD",
                        status: "PENDING",
                        paymentMethod: "CARD",
                        transactionId: (),
                        createdAt: createdAt,
                        updatedAt: ()
                    };
                    
                    _ = check self.repo.createPayment(payment);
                    
                    boolean paymentSuccess = processPayment(paymentReq.amount, "CARD");
                    string status = paymentSuccess ? "SUCCESS" : "FAILED";
                    string? transactionId = paymentSuccess ? generateTransactionId() : ();
                    
                    _ = check self.repo.updatePaymentStatus(paymentId, status, transactionId);
                    
                    PaymentStatus paymentStatus = {
                        paymentId: paymentId,
                        ticketId: paymentReq.ticketId,
                        passengerId: paymentReq.passengerId,
                        status: status,
                        amount: paymentReq.amount,
                        timestamp: time:utcNow()
                    };
                    
                    check self.kafkaProducer->send({
                        topic: "payment.status", 
                        value: paymentStatus
                    });
                    
                    log:printInfo("Payment processed: " + paymentId + " - " + status);
                }
            } on fail error e {
                log:printError("Error processing payment request: " + e.message());
            }
        }
    }

    resource function get [string paymentId](http:Caller caller) returns error? {
        Payment|error paymentResult = self.repo.getPayment(paymentId);
        if paymentResult is error {
            http:Response response = new;
            response.statusCode = 404;
            response.setJsonPayload({"error": "Payment not found", "paymentId": paymentId});
            check caller->respond(response);
            return;
        }
        check caller->respond(toJson(paymentResult));
    }

    resource function get ticket/[string ticketId](http:Caller caller) returns error? {
        Payment[]|error paymentsResult = self.repo.getPaymentsByTicket(ticketId);
        if paymentsResult is error {
            http:Response response = new;
            response.statusCode = 500;
            response.setJsonPayload({"error": "Failed to fetch payments", "ticketId": ticketId});
            check caller->respond(response);
            return;
        }
        json[] paymentJsonArray = [];
        foreach var payment in paymentsResult {
            paymentJsonArray.push(toJson(payment));
        }
        check caller->respond(paymentJsonArray);
    }

    resource function get passenger/[string passengerId](http:Caller caller) returns error? {
        Payment[]|error paymentsResult = self.repo.getPassengerPayments(passengerId);
        if paymentsResult is error {
            http:Response response = new;
            response.statusCode = 500;
            response.setJsonPayload({"error": "Failed to fetch payments", "passengerId": passengerId});
            check caller->respond(response);
            return;
        }
        json[] paymentJsonArray = [];
        foreach var payment in paymentsResult {
            paymentJsonArray.push(toJson(payment));
        }
        check caller->respond(paymentJsonArray);
    }

    resource function post process(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        string ticketId = check payload.ticketId;
        string passengerId = check payload.passengerId;
        decimal amount = <decimal>check payload.amount;
        string paymentMethod = check payload.paymentMethod;
        
        error? validationResult = validatePaymentInput(ticketId, passengerId, amount, paymentMethod);
        if validationResult is error {
            http:Response response = new;
            response.statusCode = 400;
            response.setJsonPayload({"error": validationResult.message()});
            check caller->respond(response);
            return;
        }
        
        string paymentId = generatePaymentId();
        time:Utc createdAt = time:utcNow();
        
        Payment payment = {
            paymentId: paymentId,
            ticketId: ticketId,
            passengerId: passengerId,
            amount: amount,
            currency: "NAD",
            status: "PENDING",
            paymentMethod: paymentMethod,
            transactionId: (),
            createdAt: createdAt,
            updatedAt: ()
        };
        
        _ = check self.repo.createPayment(payment);
        
        boolean paymentSuccess = processPayment(amount, paymentMethod);
        string status = paymentSuccess ? "SUCCESS" : "FAILED";
        string? transactionId = paymentSuccess ? generateTransactionId() : ();
        
        _ = check self.repo.updatePaymentStatus(paymentId, status, transactionId);
        
        PaymentStatus paymentStatus = {
            paymentId: paymentId,
            ticketId: ticketId,
            passengerId: passengerId,
            status: status,
            amount: amount,
            timestamp: time:utcNow()
        };
        
        check self.kafkaProducer->send({
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

    resource function get health() returns json {
        return {"status": "healthy", "service": "Payment Service", "timestamp": time:utcNow().toString()};
    }
}

public function main() returns error? {
    log:printInfo("Payment Service started on port 8084");
}