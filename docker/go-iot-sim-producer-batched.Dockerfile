# docker/go-iot-sim-producer-batched.Dockerfile
FROM golang:1.23-alpine

WORKDIR /app

# Copy the batched Go producer source
COPY docker/go-batched/go_iot_sim_data_producer_batched.go .

# Install dependencies & build
RUN go mod init go_iot_sim_data_producer_batched \
    && go get github.com/segmentio/kafka-go@latest \
    && go mod tidy

RUN go build -o go_iot_sim_data_producer_batched go_iot_sim_data_producer_batched.go

# Default environment variables
ENV KFK_BOOTSTRAP_SERVERS=kafka:9092 \
    KFK_TOPIC_HOME=iot-sensor-general \
    KFK_TOPIC_PLUG=iot-sensor-plug \
    PRODUCER_STREAM=both \
    SIM_PLUGS_PER_HOME=4 \
    SIM_SLEEP_SEC=1.0 \
    SIM_HOME_ID_START=1 \
    SIM_HOME_ID_END=42 \
    BATCH_SIZE_HOME=500 \
    BATCH_SIZE_PLUG=1000 \
    BATCH_TIMEOUT_MS=50

ENTRYPOINT ["./go_iot_sim_data_producer_batched"]
