import ballerina/http;
import ballerina/io;

service /tickets on new http:Listener(8084) {

    // Book a ticket (mocked)
    resource function post book(http:Caller caller, http:Request req) returns error? {
        json ticket = check req.getJsonPayload();
        io:println("Booking ticket (mocked): ", ticket.toJsonString());

        check caller->respond({status: "Ticket booked successfully (mocked)"});
    }

    // Get all tickets (mocked)
    resource function get all(http:Caller caller, http:Request req) returns error? {
        io:println("Fetching all tickets (mocked)");

        json[] tickets = [
            {"userId": "123", "destination": "Windhoek"},
            {"userId": "456", "destination": "Swakopmund"}
        ];

        check caller->respond(tickets);
    }
}
