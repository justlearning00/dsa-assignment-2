
import ballerina/http;
import ballerina/io;
// import ballerinax/kafka;
// import ballerinax/mongodb;

// TODO: Uncomment when Kafka is setup
// final kafka:Producer ticketProducer = check new ({bootstrapServers: "localhost:9092"});
// final kafka:Consumer ticketConsumer = check new ({
//     bootstrapServers: "localhost:9092",
//     groupId: "ticketing-group", 
//     topics: ["payments.processed"]
// });

// TODO: Uncomment when MongoDB is setup
// final mongodb:Client mongoClient = check new ({
//     connection: "mongodb://root:password@localhost:27017/ticketing_db"
// });

// HTTP API - This works without Kafka/MongoDB
service /tickets on new http:Listener(8082) {

    // Create a new ticket
    resource function post create(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Creating ticket: ", body.toJsonString());
        
        string ticketId = generateTicketId();
        string passengerId = check body.passengerId;
        string tripId = check body.tripId;
        string ticketType = check body.ticketType;
        decimal amount = check body.amount;
        
        // Create ticket object
        json ticket = {
            ticketId: ticketId,
            passengerId: passengerId,
            tripId: tripId,
            ticketType: ticketType,
            amount: amount,
            status: "CREATED", // Will change to PAID after payment
            createdAt: time:utcNow().toString(),
            expiresAt: calculateExpiry(ticketType)
        };
        
        // TODO: Uncomment when MongoDB is ready
        // check mongoClient->insert("tickets", ticket);
        
        // TODO: Uncomment when Kafka is ready  
        // json ticketRequest = {
        //     ticketId: ticketId,
        //     passengerId: passengerId,
        //     amount: amount
        // };
        // ticketProducer->send({
        //     topic: "ticket.requests", 
        //     value: ticketRequest.toJsonString().toBytes()
        // });
        
        check caller->respond({
            ticketId: ticketId,
            status: "Ticket created (mocked - no Kafka/MongoDB yet)"
        });
    }

    // Validate a ticket
    resource function post validate/[string ticketId](http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        string validatorId = check body.validatorId;
        
        io:println("Validating ticket: ", ticketId, " by validator: ", validatorId);
        
        // TODO: Replace with actual DB query when MongoDB is ready
        // json? ticketResult = check mongoClient->findById("tickets", ticketId);
        
        // Mock ticket data for now
        json mockedTicket = {
            ticketId: ticketId,
            passengerId: "PASS-123",
            status: "PAID", // Assume it's paid for demo
            tripId: "TRIP-456"
        };
        
        // Check if ticket can be validated
        if mockedTicket.status == "PAID" {
            // TODO: Uncomment when MongoDB is ready
            // check mongoClient->updateById("tickets", ticketId, {
            //     status: "VALIDATED",
            //     validatedAt: time:utcNow().toString()
            // });
            
            // TODO: Uncomment when Kafka is ready
            // json validationEvent = {
            //     ticketId: ticketId,
            //     status: "VALIDATED",
            //     validatorId: validatorId
            // };
            // ticketProducer->send({
            //     topic: "ticket.validated",
            //     value: validationEvent.toJsonString().toBytes()
            // });
            
            check caller->respond({
                status: "Ticket validated successfully (mocked)"
            });
        } else {
            check caller->respond({
                status: "Ticket cannot be validated - status: " + mockedTicket.status
            }, statusCode = 400);
        }
    }

    // Get ticket details
    resource function get [string ticketId](http:Caller caller) returns error? {
        io:println("Fetching ticket: ", ticketId);
        
        // TODO: Replace with actual DB query when MongoDB is ready
        // json? ticketResult = check mongoClient->findById("tickets", ticketId);
        
        // Return mocked ticket data
        json mockedTicket = {
            ticketId: ticketId,
            passengerId: "PASS-123",
            tripId: "TRIP-456", 
            ticketType: "SINGLE",
            amount: 25.50,
            status: "PAID",
            createdAt: "2025-10-02T10:00:00Z",
            expiresAt: "2025-10-02T12:00:00Z"
        };
        
        check caller->respond(mockedTicket);
    }

    // Get all tickets for a passenger
    resource function get passenger/[string passengerId](http:Caller caller) returns error? {
        io:println("Fetching tickets for passenger: ", passengerId);
        
        // TODO: Replace with actual DB query when MongoDB is ready
        // json[] tickets = check mongoClient->find("tickets", {passengerId: passengerId});
        
        // Return mocked tickets
        json[] mockedTickets = [
            {
                ticketId: "TKT-001",
                passengerId: passengerId,
                status: "PAID",
                tripId: "TRIP-123"
            },
            {
                ticketId: "TKT-002", 
                passengerId: passengerId,
                status: "VALIDATED",
                tripId: "TRIP-456"
            }
        ];
        
        check caller->respond(mockedTickets);
    }
}

// TODO: Uncomment when Kafka consumer is needed
// service kafka:Service on ticketConsumer {
//     resource function onMessage(kafka:Consumer consumer, kafka:AnonRecord[] records) returns error? {
//         foreach var kafkaRecord in records {
//             json paymentData = check json.constructFromString(check string:fromBytes(kafkaRecord.value));
//             io:println("Received payment confirmation: ", paymentData.toJsonString());
//             updateTicketStatus(paymentData);
//         }
//     }
// }

// Helper functions
function generateTicketId() returns string {
    return "TKT-" + time:utcNow().toString()[0:19].replace("T", "-").replace(":", "-");
}

function calculateExpiry(string ticketType) returns string {
    time:Utc now = time:utcNow();
    
    match ticketType {
        "SINGLE" => {
            return time:utcAddDuration(now, {hours: 2}).toString();
        }
        "DAY_PASS" => {
            return time:utcAddDuration(now, {days: 1}).toString();
        }
        _ => {
            return time:utcAddDuration(now, {hours: 24}).toString();
        }
    }
}

// TODO: Uncomment when needed
// function updateTicketStatus(json paymentData) returns error? {
//     string ticketId = check paymentData.ticketId;
//     // Update ticket status to PAID in database
//     check mongoClient->updateById("tickets", ticketId, {
//         status: "PAID",
//         paidAt: time:utcNow().toString()
//     });
// }