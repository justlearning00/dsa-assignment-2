# Use Ballerina base image
FROM ballerina/ballerina:2201.9.0

# Set working directory
WORKDIR /app

# Copy service code
COPY . .

# Build the service
RUN bal build

# Expose the service port
EXPOSE 8086

# Run the service
CMD ["bal", "run", "target/bin/admin_service.jar"]
