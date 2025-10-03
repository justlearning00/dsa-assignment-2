FROM ballerina/ballerina:2201.9.0

# Set workdir to passenger-service
WORKDIR /home/ballerina/passenger-service

# Copy passenger-service files into container
COPY . .

# Build the Ballerina project
RUN bal build

# Expose the port your service runs on
EXPOSE 8081

# Run the service
CMD ["bal", "run"]
