# Use the official Ballerina image as base
FROM ballerina/ballerina:2201.9.0

# Set working directory inside the container
WORKDIR /home/ballerina/notification-service

# Copy the service files into the container
COPY . .

# Build the Ballerina service
RUN bal build

# Expose the port your service will run on
EXPOSE 8083

# Run the service
CMD ["bal", "run", "target/bin/notification_service.jar"]
