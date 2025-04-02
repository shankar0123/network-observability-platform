import subprocess
import json
import os
import time
import logging
import re
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv() # Load .env file if present (for local development)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'canary-results')
CANARY_ID = os.getenv('CANARY_ID', 'ping-canary-local-01')
# Comma-separated list of targets
TARGET_HOSTS = os.getenv('TARGET_HOSTS', '8.8.8.8,1.1.1.1').split(',')
PING_INTERVAL_SECONDS = int(os.getenv('PING_INTERVAL_SECONDS', '60'))
PING_COUNT = int(os.getenv('PING_COUNT', '5')) # Number of pings per target per interval

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Kafka Producer Setup ---
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5, # Retry sending messages
            request_timeout_ms=30000 # Increase timeout
        )
        logging.info(f"Successfully connected to Kafka broker at {KAFKA_BROKER}")
    except NoBrokersAvailable:
        logging.error(f"Kafka broker at {KAFKA_BROKER} not available. Retrying in 10 seconds...")
        time.sleep(10)
    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}. Retrying in 10 seconds...")
        time.sleep(10)


# --- Ping Function ---
def perform_ping(target):
    """
    Performs ping measurement using fping and returns results.
    fping output (sent to stderr with -q):
    Target Name : xmt/rcv/%loss = 5/5/0%, min/avg/max = 1.23/4.56/7.89
    or if 100% loss:
    Target Name : xmt/rcv/%loss = 5/0/100%
    """
    logging.info(f"Pinging target: {target} ({PING_COUNT} times)")
    result = {
        "status": "ERROR",
        "rtt_avg_ms": None,
        "packet_loss_percent": None,
        "error_message": None
    }
    # fping command: quiet, count, interval 20ms, timeout 500ms per ping
    command = ["fping", "-q", "-C", str(PING_COUNT), "-p", "20", "-t", "500", target]

    try:
        # fping sends summary results to stderr when using -q
        process = subprocess.run(command, capture_output=True, text=True, check=False, timeout=10) # Added timeout

        if process.returncode > 1: # 0 = all reachable, 1 = some unreachable, >1 = error
             result["error_message"] = f"fping command error (return code {process.returncode}): {process.stderr.strip()}"
             logging.error(f"fping error for {target}: {result['error_message']}")
             return result

        output = process.stderr.strip()
        logging.debug(f"fping output for {target}: {output}")

        # Regex to parse fping output
        # Example: google.com : xmt/rcv/%loss = 5/5/0%, min/avg/max = 10.7/11.0/11.6
        # Example: 10.0.0.1 : xmt/rcv/%loss = 5/0/100%
        match = re.search(
            r": xmt/rcv/%loss = (\d+)/(\d+)/(\d+)%(?:, min/avg/max = ([0-9.]+)/([0-9.]+)/([0-9.]+))?",
            output
        )

        if match:
            sent, recv, loss_percent, min_rtt, avg_rtt, max_rtt = match.groups()
            result["packet_loss_percent"] = float(loss_percent)

            if int(recv) > 0 and avg_rtt is not None: # Check if avg_rtt was captured (not 100% loss)
                result["status"] = "SUCCESS"
                result["rtt_avg_ms"] = float(avg_rtt)
            elif int(loss_percent) == 100:
                 result["status"] = "FAILURE" # Target likely down
                 result["error_message"] = "100% packet loss"
                 logging.warning(f"100% packet loss for {target}")
            else:
                 # Should not happen with current fping flags, but handle defensively
                 result["status"] = "ERROR"
                 result["error_message"] = "Partial loss but couldn't parse RTT"
                 logging.error(f"Partial loss but couldn't parse RTT for {target}: {output}")

        else:
            result["error_message"] = f"Could not parse fping output: {output}"
            logging.error(f"Could not parse fping output for {target}: {output}")

    except FileNotFoundError:
        result["error_message"] = "fping command not found. Please install fping."
        logging.critical("fping command not found. Please install fping.")
        # Consider exiting or disabling ping if fping is missing
    except subprocess.TimeoutExpired:
        result["error_message"] = f"fping command timed out after 10 seconds for target {target}"
        result["status"] = "TIMEOUT"
        logging.error(f"fping command timed out for {target}")
    except Exception as e:
        result["error_message"] = f"An unexpected error occurred during ping: {e}"
        logging.exception(f"Unexpected error pinging {target}") # Log full traceback

    return result

# --- Main Loop ---
def main():
    logging.info(f"Starting Ping Canary: {CANARY_ID}")
    logging.info(f"Targets: {TARGET_HOSTS}")
    logging.info(f"Interval: {PING_INTERVAL_SECONDS}s")
    logging.info(f"Kafka Topic: {KAFKA_TOPIC}")

    while True:
        for target in TARGET_HOSTS:
            target = target.strip() # Remove leading/trailing whitespace
            if not target:
                continue

            ping_result = perform_ping(target)
            timestamp = datetime.now(timezone.utc).isoformat()

            message = {
                "type": "ping",
                "canary_id": CANARY_ID,
                "target": target,
                "timestamp": timestamp,
                "status": ping_result["status"],
                "rtt_avg_ms": ping_result["rtt_avg_ms"],
                "packet_loss_percent": ping_result["packet_loss_percent"],
                "error_message": ping_result["error_message"]
            }

            try:
                future = producer.send(KAFKA_TOPIC, value=message)
                # Optional: Wait for send confirmation (can slow down if Kafka is slow)
                # record_metadata = future.get(timeout=10)
                # logging.debug(f"Message sent to {record_metadata.topic} partition {record_metadata.partition}")
                logging.info(f"Sent ping result for {target} to Kafka topic {KAFKA_TOPIC}")
            except Exception as e:
                logging.error(f"Failed to send message to Kafka: {e}")
                # Consider buffering or other error handling here

        logging.info(f"Completed ping cycle. Sleeping for {PING_INTERVAL_SECONDS} seconds...")
        time.sleep(PING_INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("Ping Canary stopped by user.")
    finally:
        if producer:
            producer.flush()
            producer.close()
            logging.info("Kafka producer closed.")
