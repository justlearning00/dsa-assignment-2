import ballerina/http;
import ballerina/io;

service /pay on new http:Listener(8082) {

    resource function post .(http:Caller caller, http:Request req) returns error? {
        json payload = check req.getJsonPayload();
        io:println("Payment request received: ", payload.toJsonString());
        check caller->respond({status: "Payment processed (mocked)"});
    }
}
