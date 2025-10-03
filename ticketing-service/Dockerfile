# Use the official Ballerina image
FROM ballerina/ballerina:2201.9.0

# Set the working directory inside the container
WORKDIR /home/ballerina/ticketing-service

# Copy all files from your service folder into the container
COPY . .

# Build the Ballerina project
RUN bal build

# Expose the port your service listens on
EXPOSE 8084

# Command to run the service when container starts
CMD ["bal", "run", "target/bin/ticketing_service.jar"]
