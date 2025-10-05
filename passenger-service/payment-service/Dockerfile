FROM ballerina/ballerina:2201.9.0

WORKDIR /home/ballerina/payment-service

COPY . .

RUN bal build

EXPOSE 8082

CMD ["bal", "run"]
