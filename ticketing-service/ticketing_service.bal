
import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/mongodb;
import ballerinax/kafka;
import ballerina/lang.'value;

listener http:Listener httpListener = new(8083);

final TicketRepository repo = check getTicketRepository();
final kafka:Producer kafkaProducer = check new ("kafka1:19092");

type Ticket record {|
    string ticketId;
    string passengerId;
    string tripId;
    string ticketType;
    decimal amount;
    string status;
    time:Utc createdAt;
    time:Utc expiresAt;
    time:Utc? paidAt;
    time:Utc? validatedAt;
    string? validatorId;
    string? paymentId;
|};

type TicketRequest record {|
    string passengerId;
    string tripId;
    string ticketType;
    decimal amount;
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
    string status;
    decimal amount;
    time:Utc timestamp;
|};

type TicketStatusUpdate record {|
    string ticketId;
    string passengerId;
    string oldStatus;
    string newStatus;
    string? validatorId;
    time:Utc timestamp;
|};

isolated function toJson(Ticket ticket) returns json {
    return {
        ticketId: ticket.ticketId,
        passengerId: ticket.passengerId,
        tripId: ticket.tripId,
        ticketType: ticket.ticketType,
        amount: ticket.amount,
        status: ticket.status,
        createdAt: ticket.createdAt.toString(),
        expiresAt: ticket.expiresAt.toString(),
        paidAt: ticket.paidAt is time:Utc ? ticket.paidAt.toString() : (),
        validatedAt: ticket.validatedAt is time:Utc ? ticket.validatedAt.toString() : (),
        validatorId: ticket.validatorId,
        paymentId: ticket.paymentId
    };
}

public isolated class TicketRepository {
    private final mongodb:Collection tickets;

    public isolated function init(mongodb:Collection tickets) {
        self.tickets = tickets;
    }

    private isolated function createUpdate(string operator, map<json> updateData) returns mongodb:Update {
        return {[operator]: updateData};
    }

    public isolated function createTicket(Ticket ticket) returns Ticket|error {
        check self.tickets->insertOne(ticket);
        return ticket;
    }

    public isolated function getTicket(string ticketId) returns Ticket|error {
        Ticket? result = check self.tickets->findOne({ticketId: ticketId});
        if result is () {
            return error("TICKET_NOT_FOUND");
        }
        return result;
    }

    public isolated function getPassengerTickets(string passengerId) returns Ticket[]|error {
        stream<Ticket, error?> resultStream = check self.tickets->find({passengerId: passengerId});
        Ticket[] tickets = [];
        record {| Ticket value; |}|error? next = resultStream.next();
        while next is record {| Ticket value; |} {
            tickets.push(next.value);
            next = resultStream.next();
        }
        check resultStream.close();
        return tickets;
    }

    public isolated function updateTicketStatus(string ticketId, string status, string? validatorId, string? paymentId) returns Ticket|error {
        map<json> updateData = {status: status};
        
        if validatorId is string {
            updateData["validatorId"] = validatorId;
            updateData["validatedAt"] = time:utcNow().toString();
        }
        
        if paymentId is string {
            updateData["paymentId"] = paymentId;
            updateData["paidAt"] = time:utcNow().toString();
        }

        mongodb:Update updateObj = self.createUpdate("$set", updateData);
        mongodb:UpdateResult result = check self.tickets->updateOne({ticketId: ticketId}, updateObj);
        
        if result.matchedCount == 0 {
            return error("TICKET_NOT_FOUND");
        }
        
        return self.getTicket(ticketId);
    }
}

isolated function getTicketRepository() returns TicketRepository|error {
    mongodb:Client mongoClient = check new ({
        connection: "mongodb://root:password@mongodb:27017/ticketing_db"
    });
    
    mongodb:Database database = check mongoClient->getDatabase("ticketing_db");
    mongodb:Collection tickets = check database->getCollection("tickets");
    return new(tickets);
}

function generateTicketId() returns string {
    time:Utc now = time:utcNow();
    return "TKT-" + now[0].toString() + now[1].toString() + now[2].toString();
}

function calculateExpiry(string ticketType) returns time:Utc {
    time:Utc now = time:utcNow();
    match ticketType {
        "SINGLE" => {
            return now.addHours(2);
        }
        "DAY_PASS" => {
            return now.addHours(24);
        }
        "WEEK_PASS" => {
            return now.addHours(7 * 24);
        }
        _ => {
            return now.addHours(24);
        }
    }
}

// Kafka Consumer for payment status updates
kafka:Consumer kafkaConsumer = check new ("kafka1:19092", {
    groupId: "ticketing-group",
    topics: ["payment.status"]
});

service kafka:Service on kafkaConsumer {
    isolated remote function onConsumerRecord(kafka:ConsumerRecord[] records) returns error? {
        foreach var kafkaRecord in records {
            PaymentStatus paymentStatus = check value:fromJson(kafkaRecord.value);
            log:printInfo("Received payment status for ticket: " + paymentStatus.ticketId + " - " + paymentStatus.status);
            
            if paymentStatus.status == "SUCCESS" {
                Ticket|error updated = repo.updateTicketStatus(
                    paymentStatus.ticketId, 
                    "PAID", 
                    (), 
                    paymentStatus.paymentId
                );
                
                if updated is error {
                    log:printError("Failed to update ticket status: " + updated.message());
                } else {
                    // Send ticket purchased event
                    check kafkaProducer->send({
                        topic: "ticket.purchased", 
                        value: {
                            ticketId: paymentStatus.ticketId,
                            passengerId: paymentStatus.passengerId,
                            status: "PURCHASED",
                            timestamp: time:utcNow().toString()
                        }
                    });
                    
                    log:printInfo("Ticket status updated to PAID: " + paymentStatus.ticketId);
                }
            } else {
                // Payment failed - cancel ticket
                _ = repo.updateTicketStatus(paymentStatus.ticketId, "CANCELLED", (), ());
                log:printInfo("Ticket cancelled due to payment failure: " + paymentStatus.ticketId);
            }
        }
    }
}

service /tickets on httpListener {
    isolated resource function post create(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        string passengerId = check payload.passengerId.toString();
        string tripId = check payload.tripId.toString();
        string ticketType = check payload.ticketType.toString();
        decimal amount = <decimal>check payload.amount;
        
        string ticketId = generateTicketId();
        time:Utc expiresAt = calculateExpiry(ticketType);
        time:Utc createdAt = time:utcNow();
        
        Ticket ticket = {
            ticketId: ticketId,
            passengerId: passengerId,
            tripId: tripId,
            ticketType: ticketType,
            amount: amount,
            status: "CREATED",
            createdAt: createdAt,
            expiresAt: expiresAt
        };
        
        Ticket created = check repo.createTicket(ticket);
        
        // Send payment request to Kafka
        PaymentRequest paymentReq = {
            ticketId: ticketId,
            passengerId: passengerId,
            tripId: tripId,
            ticketType: ticketType,
            amount: amount,
            timestamp: createdAt
        };
        check kafkaProducer->send({
            topic: "payment.requests", 
            value: paymentReq
        });
        
        log:printInfo("Ticket created and payment requested: " + ticketId);
        json response = {
            ticketId: ticketId,
            status: "CREATED",
            message: "Ticket created - payment processing started"
        };
        check caller->respond(response);
    }

    isolated resource function post validate/[string ticketId](http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        string validatorId = check payload.validatorId.toString();
        
        Ticket|error ticketResult = repo.getTicket(ticketId);
        if ticketResult is error {
            json errorResponse = {"error": "Ticket not found", "ticketId": ticketId};
            check caller->respond(errorResponse);
            return;
        }
        
        Ticket ticket = ticketResult;
        
        if ticket.status != "PAID" {
            json errorResponse = {"error": "Ticket cannot be validated - current status: " + ticket.status};
            check caller->respond(errorResponse);
            return;
        }
        
        // ==== FIXED EXPIRATION CHECK ====
        // Check if ticket expired - CORRECT LOGIC
        if time:utcDiff(ticket.expiresAt, time:utcNow()).seconds < 0 {
            _ = repo.updateTicketStatus(ticketId, "EXPIRED", (), ());
            json errorResponse = {"error": "Ticket has expired"};
            check caller->respond(errorResponse);
            return;
        }
        
        Ticket|error updated = repo.updateTicketStatus(ticketId, "VALIDATED", validatorId, ());
        
        if updated is error {
            json errorResponse = {"error": "Failed to validate ticket", "ticketId": ticketId};
            check caller->respond(errorResponse);
            return;
        }
        
        // Send ticket validated event
        check kafkaProducer->send({
            topic: "ticket.validated", 
            value: {
                ticketId: ticketId,
                passengerId: ticket.passengerId,
                validatorId: validatorId,
                timestamp: time:utcNow().toString()
            }
        });
        
        log:printInfo("Ticket validated: " + ticketId);
        json response = {"status": "VALIDATED", "message": "Ticket validated successfully"};
        check caller->respond(response);
    }

    isolated resource function get [string ticketId](http:Caller caller) returns error? {
        Ticket|error ticketResult = repo.getTicket(ticketId);
        if ticketResult is error {
            json errorResponse = {"error": "Ticket not found", "ticketId": ticketId};
            check caller->respond(errorResponse);
            return;
        }
        check caller->respond(toJson(ticketResult));
    }

    isolated resource function get passenger/[string passengerId](http:Caller caller) returns error? {
        Ticket[]|error ticketsResult = repo.getPassengerTickets(passengerId);
        if ticketsResult is error {
            json errorResponse = {"error": "Failed to fetch tickets", "passengerId": passengerId};
            check caller->respond(errorResponse);
            return;
        }
        json[] ticketJsonArray = [];
        foreach var ticket in ticketsResult {
            ticketJsonArray.push(toJson(ticket));
        }
        check caller->respond(ticketJsonArray);
    }

    isolated resource function get health() returns json {
        return {"status": "healthy", "service": "Ticketing Service", "timestamp": time:utcNow().toString()};
    }
}

public function main() returns error? {
    log:printInfo("Ticketing Service started on port 8083");
}