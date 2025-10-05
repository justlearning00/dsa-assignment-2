FROM ballerina/ballerina:2201.10.0

WORKDIR /home/ballerina/passenger-service

COPY . .

# Fix permissions
RUN chmod -R 777 /home/ballerina/passenger-service

# Build
RUN bal build

EXPOSE 8081

CMD ["bal", "run", "passenger_service.bal"]
