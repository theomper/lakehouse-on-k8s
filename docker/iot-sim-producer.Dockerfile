# docker/iot-sim-producer.Dockerfile
FROM python:3.11-slim

# Small, stable Kafka client
RUN pip install --no-cache-dir kafka-python==2.2.15

WORKDIR /app
COPY docker/python/iot_sim_data_producer.py .

# Defaults are safe for local dev; override in K8s
ENV PYTHONUNBUFFERED=1 \
    KFK_BOOTSTRAP_SERVERS=kafka:9092 \
    KFK_TOPIC_HOME=iot-sensor-general \
    KFK_TOPIC_PLUG=iot-sensor-plug \
    PRODUCER_STREAM=both \
    SIM_PLUGS_PER_HOME=4 \
    SIM_SLEEP_SEC=1.0 \
    SIM_HOME_ID_START=1 \
    SIM_HOME_ID_END=42

ENTRYPOINT ["python", "iot_sim_data_producer.py"]
