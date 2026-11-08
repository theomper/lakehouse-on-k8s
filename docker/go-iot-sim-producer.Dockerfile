# docker/go-iot-sim-producer.Dockerfile
FROM golang:1.23-alpine

# Set the working directory
WORKDIR /app

# Copy the Go application source code
COPY docker/go/go_iot_sim_data_producer.go .

# Install necessary dependencies
RUN go mod init go_iot_sim_data_producer \
    && go get github.com/segmentio/kafka-go@latest \
    && go mod tidy

# Build the Go application
RUN go build -o go_iot_sim_data_producer go_iot_sim_data_producer.go

# Set environment variables
ENV KFK_BOOTSTRAP_SERVERS=kafka:9092 \
    KFK_TOPIC_HOME=iot-sensor-general \
    KFK_TOPIC_PLUG=iot-sensor-plug \
    PRODUCER_STREAM=both \
    SIM_PLUGS_PER_HOME=4 \
    SIM_SLEEP_SEC=1.0 \
    SIM_HOME_ID_START=1 \
    SIM_HOME_ID_END=42

# Run the Go application
ENTRYPOINT ["./go_iot_sim_data_producer"]