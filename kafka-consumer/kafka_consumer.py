import json
import os
import time
import logging
import threading
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from prometheus_client import start_http_server, Gauge, Counter, Histogram
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'canary-results')
CONSUMER_GROUP_ID = os.getenv('CONSUMER_GROUP_ID', 'canary-consumer-group-1')
PROMETHEUS_PORT = int(os.getenv('PROMETHEUS_PORT', '8000'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Prometheus Metrics Definition ---
# Define labels that will be common across metrics
LABELS = ['canary_id', 'target', 'type']

# Gauge for latency (e.g., RTT, HTTP response time)
CANARY_LATENCY_MS = Gauge(
    'canary_latency_ms',
    'Latency of canary test in milliseconds',
    LABELS
)

# Counter for test status (SUCCESS, FAILURE, ERROR, TIMEOUT)
CANARY_STATUS_TOTAL = Counter(
    'canary_status_total',
    'Total count of canary test results by status',
    LABELS + ['status'] # Add status label here
)

# Gauge for packet loss percentage (specific to ping)
PING_PACKET_LOSS_PERCENT = Gauge(
    'ping_packet_loss_percent',
    'Packet loss percentage for ping canary tests',
    LABELS # Uses the standard labels
)

# Optional: Histogram for latency distribution (more complex but powerful)
# CANARY_LATENCY_HISTOGRAM = Histogram(
#     'canary_latency_histogram_ms',
#     'Histogram of canary test latency in milliseconds',
#     LABELS,
#     buckets=[10, 50, 100, 250, 500, 1000, 2500, 5000, 10000] # Example buckets
# )

# --- Kafka Consumer Setup ---
consumer = None
while consumer is None:
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=CONSUMER_GROUP_ID,
            auto_offset_reset='latest', # Start consuming from the latest message
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=1000 # Timeout for polling
        )
        logging.info(f"Successfully connected to Kafka broker at {KAFKA_BROKER} and subscribed to topic {KAFKA_TOPIC}")
    except NoBrokersAvailable:
        logging.error(f"Kafka broker at {KAFKA_BROKER} not available. Retrying in 10 seconds...")
        time.sleep(10)
    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}. Retrying in 10 seconds...")
        time.sleep(10)

# --- Metrics Processing Function ---
def process_message(message):
    """Parses a message and updates Prometheus metrics."""
    try:
        data = message.value
        logging.debug(f"Received message: {data}")

        # Basic validation
        if not all(k in data for k in ['canary_id', 'target', 'type', 'status']):
            logging.warning(f"Skipping malformed message (missing required keys): {data}")
            return

        canary_id = data.get('canary_id', 'unknown')
        target = data.get('target', 'unknown')
        canary_type = data.get('type', 'unknown')
        status = data.get('status', 'ERROR').upper() # Normalize status

        # Update status counter
        CANARY_STATUS_TOTAL.labels(
            canary_id=canary_id,
            target=target,
            type=canary_type,
            status=status
        ).inc()

        # Update latency if available and status is not ERROR/TIMEOUT
        latency = data.get('rtt_avg_ms') # Defaulting to ping's key for now
        if latency is None:
             latency = data.get('latency_ms') # Check for generic latency key

        if latency is not None and status in ['SUCCESS', 'FAILURE']: # Only record latency for valid attempts
            try:
                latency_float = float(latency)
                CANARY_LATENCY_MS.labels(
                    canary_id=canary_id,
                    target=target,
                    type=canary_type
                ).set(latency_float)
                # Optional: Update histogram
                # CANARY_LATENCY_HISTOGRAM.labels(...).observe(latency_float)
            except (ValueError, TypeError):
                 logging.warning(f"Invalid latency value '{latency}' in message: {data}")


        # Update ping-specific metrics
        if canary_type == 'ping':
            loss = data.get('packet_loss_percent')
            if loss is not None:
                try:
                    loss_float = float(loss)
                    PING_PACKET_LOSS_PERCENT.labels(
                        canary_id=canary_id,
                        target=target,
                        type=canary_type # Redundant here but keeps labels consistent
                    ).set(loss_float)
                except (ValueError, TypeError):
                    logging.warning(f"Invalid packet_loss_percent value '{loss}' in message: {data}")

        # TODO: Add processing for other canary types (http, dns, traceroute) here
        # elif canary_type == 'http':
        #     status_code = data.get('status_code')
        #     # Define and update HTTP specific metrics...
        # elif canary_type == 'dns':
        #     # Define and update DNS specific metrics...

    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON message: {message.value}")
    except Exception as e:
        logging.exception(f"Error processing message: {message.value}") # Log full traceback

# --- Main Loop ---
def main():
    logging.info(f"Starting Kafka Consumer for topic {KAFKA_TOPIC}")
    logging.info(f"Exposing Prometheus metrics on port {PROMETHEUS_PORT}")

    # Start Prometheus HTTP server in a background thread
    prometheus_thread = threading.Thread(target=start_http_server, args=(PROMETHEUS_PORT,), daemon=True)
    prometheus_thread.start()

    logging.info("Prometheus metrics server started.")

    while True:
        try:
            for message in consumer:
                process_message(message)
            # If consumer_timeout_ms is set, the loop continues here after timeout
            # time.sleep(0.1) # Optional small sleep if timeout is not used or very long

        except Exception as e:
            logging.error(f"Error in consumer loop: {e}. Attempting to reconnect...")
            # Basic reconnect logic (KafkaConsumer handles some internally)
            time.sleep(5)
            # More robust reconnect/re-initialization might be needed here

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Consumer stopped by user.")
    finally:
        if consumer:
            consumer.close()
            logging.info("Kafka consumer closed.")
        # Prometheus server thread is daemon, will exit automatically
