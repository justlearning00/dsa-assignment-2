# Use Ballerina base image
FROM ballerina/ballerina:2201.9.0

# Set working directory
WORKDIR /app

# Copy all files into the container
COPY . .

# Give write permissions to avoid target build issues
RUN chmod -R 777 /app

# Build Ballerina service
RUN bal build

# Expose the service port
EXPOSE 8085

# Run the service
CMD ["bal", "run", "target/bin/admin_service.jar"]
