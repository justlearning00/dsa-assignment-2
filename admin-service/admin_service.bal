import ballerina/http;
import ballerina/io;


// -----------------------------
// Mock Data (replace with DB later)
// -----------------------------
type Report record {
    int id;
    string type_of;
    string data;
};

Report[] reports = [
    {id: 1, type_of: "ticket-sales", data: "Total tickets sold today: 152"},
    {id: 2, type_of: "traffic", data: "Peak hour validations: 88"},
    {id: 3, type_of: "revenue", data: "Revenue collected today: N$ 1,500"}
];

// -----------------------------
// REST API
// -----------------------------
service /admin on new http:Listener(8086) {

    // Publish a disruption
    resource function post disruptions(http:Caller caller, http:Request req) returns error? {
        json body = check req.getJsonPayload();
        io:println("Service disruption published: ", body.toJsonString());

        // -----------------------------
        // Placeholder: publish disruption to Kafka topic
        // Example: kafkaProducer->publish("notifications", body);
        // -----------------------------

        // -----------------------------
        // Optional: store in DB for history/logs
        //  await dbClient->insert("disruptions", body);
        // -----------------------------

        check caller->respond({status: "Disruption published (mocked)"});
    }

    // Fetch reports
    resource function get reports(http:Caller caller, http:Request req) returns error? {
        io:println("Admin requested reports");

        // -----------------------------
        // Placeholder: compute reports dynamically from DB instead of mock array
        // reports = await dbClient->query("SELECT * FROM reports");
        // -----------------------------

        check caller->respond(reports);
    }
}
