import ballerinax/kafka;
import ballerina/io;
import ballerina/time;

// Ticket Request - What passenger wants to buy
type TicketRequest record {
    string passengerId;
    string routeId;
    string tripId;
    TicketType ticketType;
    decimal price;
    time:Utc preferredTime;
    string paymentMethod;
};

// Status - What system returns after processing
type TicketStatus record {
    string ticketId;
    string passengerId;
    string routeId;
    TicketType ticketType;
    TicketState status;
    time:Utc purchaseTime;
    time:Utc expiryTime;
    string? validationCode;
    string? message;
};

// Ticket Type
enum TicketType {
    SINGLE_RIDE,
    MULTI_RIDE,
    DAY_PASS,
    WEEK_PASS,
    MONTH_PASS
}

// Ticket State
enum TicketState {
    REQUESTED,
    PAYMENT_PENDING,
    PAYMENT_FAILED,
    CONFIRMED,
    VALIDATED,
    EXPIRED,
    CANCELLED
}

// Function to simulate passenger buying tickets
function simulatePassengerPurchase(kafka:Producer producer) returns error? {
    // Paasenger Requests
    TicketRequest[] ticketRequests = [
        {
            passengerId: "PASS_001",
            routeId: "BUS_101_WINDHOEK_SWAKOP",
            tripId: "TRIP_MORNING_001",
            ticketType: SINGLE_RIDE,
            price: 25.00,
            preferredTime: time:utcNow(),
            paymentMethod: "CREDIT_CARD"
        },
        {
            passengerId: "PASS_002", 
            routeId: "TRAIN_202_KATUTURA",
            tripId: "TRIP_EVENING_045",
            ticketType: DAY_PASS,
            price: 50.00,
            preferredTime: time:utcAddSeconds(time:utcNow(), 300),
            paymentMethod: "MOBILE_WALLET"
        },
        {
            passengerId: "PASS_003",
            routeId: "BUS_305_NORTHERN_INDUSTRIAL", 
            tripId: "TRIP_NOON_112",
            ticketType: WEEK_PASS,
            price: 200.00,
            preferredTime: time:utcAddSeconds(time:utcNow(), 600),
            paymentMethod: "CASH"
        }
    ];

    // Send Ticket Requests
    foreach var request in ticketRequests {
        io:println("Passenger " + request.passengerId + " Requesting Ticket");
        io:println("Route: " + request.routeId);
        io:println("Type: " + request.ticketType.toString());
        io:println("Price: N$" + request.price.toString());
        
        kafka:Error? sendResult = producer->send({
            topic: "ticket.requests",
            key: request.passengerId,
            value: request
        });

        if sendResult is kafka:Error {
            io:println("Failed to send request: " + sendResult.message());
        } else {
            io:println("Request sent to ticket.requests topic");
        }
    }
}

// Function to consume and display ticket status updates
function consumeTicketUpdates(kafka:Consumer consumer) returns error? {
    io:println("Listening");
    io:println("Press Ctrl+C to stop monitoring");
    
    while true {
        TicketStatus[] updates = check consumer->pollPayload(5000);
        
        if updates.length() > 0 {
            foreach var update in updates {
                displayTicketUpdate(update);
            }
        }
    }
}

// Function to display ticket status updates
function displayTicketUpdate(TicketStatus update) {
    io:println("TICKET UPDATE RECEIVED:");
    io:println("Ticket ID: " + update.ticketId);
    io:println("Passenger: " + update.passengerId); 
    io:println("Route: " + update.routeId);
    io:println("Type: " + update.ticketType.toString());
    io:println("Status: " + getStatus(update.status));
    io:println("Purchase Time: " + time:utcToString(update.purchaseTime));
    
    if update.validationCode is string {
        io:println(update.validationCode);
    }
    
    if update.message is string {
        io:println(update.message);
    }
}

// Helper function for status s
function getStatus(TicketState status) returns string {
    match status {
        CONFIRMED => { return "CONFIRMED"; }
        PAYMENT_PENDING => { return "PENDING"; }
        PAYMENT_FAILED => { return "FAILED"; }
        VALIDATED => { return "VALIDATED"; }
        EXPIRED => { return "EXPIRED"; }
        _ => { return "UNRESOLVED"; }
    }
}

// MAIN FUNCTION - Orchestrates everything
public function main() returns error? {
    io:println("SMART TICKETING SYSTEM DEMO");

    // Initialize Kafka
    kafka:Producer ticketProducer = check new (kafka:DEFAULT_URL);
    kafka:Consumer statusConsumer = check new (kafka:DEFAULT_URL, {
        groupId: "passenger-group",
        topics: ["ticket.status.updates"],
        offsetReset: "earliest"
    });

    // Send ticket requests
    io:println("Sending Ticket Requests");
    check simulatePassengerPurchase(ticketProducer);

    // Check for responses
    io:println("Checking for Responses");
    check consumeTicketUpdates(statusConsumer);

    check ticketProducer->close();
    check statusConsumer->close();
    
    io:println("Demo Complete");
}