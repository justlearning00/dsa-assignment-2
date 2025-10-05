
import ballerina/http;
import ballerina/log;
import ballerina/time;
import ballerinax/mongodb;
import ballerinax/kafka;

listener http:Listener httpListener = new(8083);

final TicketRepository repo = check getTicketRepository();

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

function toJson(Ticket ticket) returns json {
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

public class TicketRepository {
    private final mongodb:Collection tickets;

    public function init(mongodb:Collection tickets) {
        self.tickets = tickets;
    }

    private function createUpdate(string operator, map<json> updateData) returns mongodb:Update {
        return {[operator]: updateData};
    }

    public function createTicket(Ticket ticket) returns Ticket|error {
        check self.tickets->insertOne(ticket);
        return ticket;
    }

    public function getTicket(string ticketId) returns Ticket|error {
        Ticket? result = check self.tickets->findOne({ticketId: ticketId});
        if result is () {
            return error("TICKET_NOT_FOUND");
        }
        return result;
    }

    public function getPassengerTickets(string passengerId) returns Ticket[]|error {
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

    public function updateTicketStatus(string ticketId, string status, string? validatorId, string? paymentId) returns Ticket|error {
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

function getTicketRepository() returns TicketRepository|error {
    mongodb:Client mongoClient = check new ({
        connection: "mongodb://root:password@mongodb:27017/ticketing_db"
    });
    
    mongodb:Database database = check mongoClient->getDatabase("ticketing_db");
    mongodb:Collection tickets = check database->getCollection("tickets");
    return new(tickets);
}

function generateTicketId() returns string {
    time:Utc now = time:utcNow();
    string nowStr = now.toString();
    return "TKT-" + nowStr.substring(0, 19);
}

function calculateExpiry(string ticketType) returns time:Utc {
    time:Utc now = time:utcNow();
    
    match ticketType {
        "SINGLE" => {
            return time:utcAddSeconds(now, 7200);
        }
        "DAY_PASS" => {
            return time:utcAddSeconds(now, 86400);
        }
        "WEEK_PASS" => {
            return time:utcAddSeconds(now, 604800);
        }
        _ => {
            return time:utcAddSeconds(now, 86400);
        }
    }
}

service /tickets on httpListener {
    
    kafka:Producer kafkaProducer;
    
    function init() returns error? {
        self.kafkaProducer = check new ("localhost:9092");
        log:printInfo("Ticketing Service initialized with Kafka producer");
    }
    
    resource function post create(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        TicketRequest request = check payload.cloneWithType();
        
        string ticketId = generateTicketId();
        time:Utc expiresAt = calculateExpiry(request.ticketType);
        time:Utc createdAt = time:utcNow();
        
        Ticket ticket = {
            ticketId: ticketId,
            passengerId: request.passengerId,
            tripId: request.tripId,
            ticketType: request.ticketType,
            amount: request.amount,
            status: "CREATED",
            createdAt: createdAt,
            expiresAt: expiresAt,
            paidAt: (),
            validatedAt: (),
            validatorId: (),
            paymentId: ()
        };
        
        Ticket created = check repo.createTicket(ticket);
        
        // Send payment request to Kafka
        PaymentRequest paymentReq = {
            ticketId: ticketId,
            passengerId: request.passengerId,
            tripId: request.tripId,
            ticketType: request.ticketType,
            amount: request.amount,
            timestamp: createdAt
        };
        check self.kafkaProducer->send({
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

    resource function post payment/[string ticketId]/status(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        PaymentStatus paymentStatus = check payload.cloneWithType();
        
        log:printInfo("Manual payment status update for ticket: " + ticketId + " - " + paymentStatus.status);
        
        if paymentStatus.status == "SUCCESS" {
            Ticket|error updated = repo.updateTicketStatus(ticketId, "PAID", (), paymentStatus.paymentId);
            
            if updated is error {
                json errorResponse = {"error": "Failed to update ticket status: " + updated.message()};
                check caller->respond(errorResponse);
                return;
            } else {
                json purchaseEvent = {
                    ticketId: ticketId,
                    passengerId: paymentStatus.passengerId,
                    status: "PURCHASED",
                    timestamp: time:utcNow().toString()
                };
                check self.kafkaProducer->send({
                    topic: "ticket.purchased", 
                    value: purchaseEvent
                });
                
                json response = {"status": "PAID", "message": "Ticket payment confirmed"};
                check caller->respond(response);
            }
        } else {
            Ticket|error cancelResult = repo.updateTicketStatus(ticketId, "CANCELLED", (), ());
            if cancelResult is error {
                json errorResponse = {"error": "Failed to cancel ticket: " + cancelResult.message()};
                check caller->respond(errorResponse);
            } else {
                json response = {"status": "CANCELLED", "message": "Ticket cancelled due to payment failure"};
                check caller->respond(response);
            }
        }
    }

    resource function post validate/[string ticketId](http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        string validatorId = check payload.validatorId;
        
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
        
        
        // Check if ticket expired
        time:Utc now = time:utcNow();
        string nowStr = now.toString();
        string expiresStr = ticket.expiresAt.toString();
        
        if nowStr > expiresStr {
            Ticket|error expireResult = repo.updateTicketStatus(ticketId, "EXPIRED", (), ());
            if expireResult is error {
                log:printError("Failed to expire ticket: " + expireResult.message());
            }
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
        json validationEvent = {
            ticketId: ticketId,
            passengerId: ticket.passengerId,
            validatorId: validatorId,
            timestamp: time:utcNow().toString()
        };
        check self.kafkaProducer->send({
            topic: "ticket.validated", 
            value: validationEvent
        });
        
        log:printInfo("Ticket validated: " + ticketId);
        json response = {"status": "VALIDATED", "message": "Ticket validated successfully"};
        check caller->respond(response);
    }

    resource function get [string ticketId](http:Caller caller) returns error? {
        Ticket|error ticketResult = repo.getTicket(ticketId);
        if ticketResult is error {
            json errorResponse = {"error": "Ticket not found", "ticketId": ticketId};
            check caller->respond(errorResponse);
            return;
        }
        check caller->respond(toJson(ticketResult));
    }

    resource function get passenger/[string passengerId](http:Caller caller) returns error? {
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

    resource function get health() returns json {
        return {"status": "healthy", "service": "Ticketing Service", "timestamp": time:utcNow().toString()};
    }
}

public function main() returns error? {
    log:printInfo("Ticketing Service started on port 8083");
}