import subprocess
import json
import os
import time
import logging
import re
from datetime import datetime, timezone # Ensure this is present
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# --- Configuration ---
load_dotenv() # Load .env file if present (for local development)

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'canary-results')
CANARY_ID = os.getenv('CANARY_ID', 'ping-canary-local-01') # Ensure CANARY_ID is defined
# InfluxDB Configuration
INFLUXDB_URL = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN', '')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG', '')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET', '')
# Comma-separated list of targets
TARGET_HOSTS = os.getenv('TARGET_HOSTS', '8.8.8.8,1.1.1.1').split(',')
PING_INTERVAL_SECONDS = int(os.getenv('PING_INTERVAL_SECONDS', '60'))
PING_COUNT = int(os.getenv('PING_COUNT', '5')) # Number of pings per target per interval

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Kafka Producer Setup ---
producer = None
if KAFKA_BROKER:
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
else:
    logging.warning("KAFKA_BROKER environment variable not set. Kafka producer will not be initialized.")


# --- Ping Function ---
def perform_ping(target_host, write_api=None): # Added write_api, renamed target to target_host
    """
    Performs ping measurement using fping and returns results.
    Also sends metrics to InfluxDB if write_api is provided.
    """
    logging.info(f"Pinging target: {target_host} ({PING_COUNT} times)")
    result = {
        "status": "ERROR", # SUCCESS, FAILURE, TIMEOUT
        "rtt_min_ms": None,
        "rtt_avg_ms": None,
        "rtt_max_ms": None,
        "packet_loss_percent": None,
        "packets_sent": None,
        "packets_received": None,
        "error_message": None
    }
    # fping command: quiet, count, interval 20ms, timeout 500ms per ping
    command = ["fping", "-q", "-C", str(PING_COUNT), "-p", "20", "-t", "500", target_host]
    
    fping_success_flag = False # For InfluxDB success field (0 or 1)
    
    try:
        process = subprocess.run(command, capture_output=True, text=True, check=False, timeout=10)
        output = process.stderr.strip() # fping -q sends summary to stderr
        logging.debug(f"fping output for {target_host}: {output}")

        if process.returncode > 1: # 0 = all reachable, 1 = some unreachable, >1 = error
             result["error_message"] = f"fping command error (return code {process.returncode}): {output}"
             logging.error(f"fping error for {target_host}: {result['error_message']}")
             # Don't return early, try to send to InfluxDB if possible
        
        match = re.search(
            # google.com : xmt/rcv/%loss = 5/5/0%, min/avg/max = 10.7/11.0/11.6
            # 10.0.0.1   : xmt/rcv/%loss = 5/0/100%
            r": xmt/rcv/%loss = (\d+)/(\d+)/(\d+)%(?:, min/avg/max = ([0-9.]+)/([0-9.]+)/([0-9.]+))?",
            output
        )

        if match:
            sent, recv, loss_percent_str, min_rtt_str, avg_rtt_str, max_rtt_str = match.groups()
            result["packets_sent"] = int(sent)
            result["packets_received"] = int(recv)
            result["packet_loss_percent"] = float(loss_percent_str)

            if int(recv) > 0 and avg_rtt_str is not None:
                result["status"] = "SUCCESS"
                fping_success_flag = True
                result["rtt_min_ms"] = float(min_rtt_str)
                result["rtt_avg_ms"] = float(avg_rtt_str)
                result["rtt_max_ms"] = float(max_rtt_str)
            elif float(loss_percent_str) == 100.0:
                 result["status"] = "FAILURE"
                 result["error_message"] = "100% packet loss"
                 logging.warning(f"100% packet loss for {target_host}")
            else: # Partial loss or other parsing issue
                 result["status"] = "ERROR"
                 result["error_message"] = f"Partial loss or unparseable RTTs: {output}"
                 logging.error(result["error_message"])
        else:
            result["status"] = "ERROR" # Keep status as ERROR if parsing failed
            result["error_message"] = f"Could not parse fping output: {output}"
            if process.returncode == 0 and not output: # fping -q might give no output if host is unreachable by name
                result["status"] = "FAILURE"
                result["error_message"] = "fping gave no output for target, likely resolution or immediate unreachability issue."
            logging.error(f"Could not parse fping output for {target_host}: {output}. RC: {process.returncode}")


    except FileNotFoundError:
        result["error_message"] = "fping command not found. Please install fping."
        logging.critical("fping command not found. Please install fping.")
        # No InfluxDB write here, as it's a canary setup issue
        return result
    except subprocess.TimeoutExpired:
        result["error_message"] = f"fping command timed out after 10 seconds for target {target_host}"
        result["status"] = "TIMEOUT"
        logging.error(f"fping command timed out for {target_host}")
    except Exception as e:
        result["error_message"] = f"An unexpected error occurred during ping: {e}"
        logging.exception(f"Unexpected error pinging {target_host}")

    # --- InfluxDB Data Sending ---
    if write_api and INFLUXDB_BUCKET: # Ensure bucket is also defined
        try:
            point = Point("ping_canary_stats") \
                .tag("target_host", target_host) \
                .tag("canary_id", CANARY_ID) \
                .field("success", 1 if fping_success_flag else 0) \
                .time(datetime.now(timezone.utc), WritePrecision.NS)

            if result["packets_sent"] is not None:
                point = point.field("packets_sent", result["packets_sent"])
            if result["packets_received"] is not None:
                point = point.field("packets_received", result["packets_received"])
            if result["packet_loss_percent"] is not None:
                point = point.field("packet_loss_percent", result["packet_loss_percent"])
            
            # Only add RTT fields if ping was successful and they exist
            if fping_success_flag:
                if result["rtt_min_ms"] is not None:
                    point = point.field("rtt_min_ms", result["rtt_min_ms"])
                if result["rtt_avg_ms"] is not None:
                    point = point.field("rtt_avg_ms", result["rtt_avg_ms"])
                if result["rtt_max_ms"] is not None:
                    point = point.field("rtt_max_ms", result["rtt_max_ms"])
            
            # Add error message if one occurred and it's not just "100% packet loss" (which is covered by success=0)
            if result["error_message"] and result["status"] not in ["SUCCESS", "FAILURE"]: # FAILURE for 100% loss has its own error
                point = point.tag("error_details", result["error_message"][:250]) # Limit length if necessary

            write_api.write(bucket=INFLUXDB_BUCKET, record=point)
            logging.debug(f"Sent Ping check data to InfluxDB for {target_host}")
        except Exception as e:
            logging.error(f"Failed to send Ping check data to InfluxDB for {target_host}: {e}")
    
    return result

# --- Main Loop Function ---
def main_loop(write_api_loop): # Accepts write_api from the main setup
    global producer # Ensure producer is accessible
    logging.info(f"Starting Ping Canary main loop: {CANARY_ID}")
    logging.info(f"Targets: {TARGET_HOSTS}")
    logging.info(f"Interval: {PING_INTERVAL_SECONDS}s")
    if KAFKA_BROKER and producer:
        logging.info(f"Kafka Topic: {KAFKA_TOPIC}")
    else:
        logging.warning("Kafka is not configured or producer failed to initialize.")
    if write_api_loop:
        logging.info(f"InfluxDB integration enabled: {INFLUXDB_URL}")
    else:
        logging.info("InfluxDB integration disabled (or bucket not set).")

    while True:
        for target_host_loop in TARGET_HOSTS: # Renamed target to avoid conflict
            target_host_loop = target_host_loop.strip()
            if not target_host_loop:
                continue

            ping_result = perform_ping(target_host_loop, write_api_loop) # Pass write_api
            timestamp = datetime.now(timezone.utc).isoformat()

            message = {
                "type": "ping",
                "canary_id": CANARY_ID,
                "target": target_host_loop, # Use 'target' for Kafka message consistency
                "timestamp": timestamp,
                "status": ping_result["status"],
                "rtt_avg_ms": ping_result["rtt_avg_ms"], # Keep for Kafka even if more details in Influx
                "packet_loss_percent": ping_result["packet_loss_percent"],
                "error_message": ping_result["error_message"],
                # Include detailed RTTs if needed for Kafka, though Influx is primary for these details
                "rtt_min_ms": ping_result["rtt_min_ms"],
                "rtt_max_ms": ping_result["rtt_max_ms"],
                "packets_sent": ping_result["packets_sent"],
                "packets_received": ping_result["packets_received"],
            }
            
            if producer:
                try:
                    producer.send(KAFKA_TOPIC, value=message)
                    logging.info(f"Sent ping result for {target_host_loop} to Kafka topic {KAFKA_TOPIC}")
                except Exception as e:
                    logging.error(f"Failed to send message to Kafka for {target_host_loop}: {e}")
            else:
                logging.debug(f"Kafka producer not available. Skipping Kafka send for {target_host_loop}.")

        logging.info(f"Completed ping cycle. Sleeping for {PING_INTERVAL_SECONDS} seconds...")
        time.sleep(PING_INTERVAL_SECONDS)

if __name__ == "__main__":
    influx_client_for_shutdown = None
    write_api_main = None

    if INFLUXDB_URL and INFLUXDB_TOKEN and INFLUXDB_ORG and INFLUXDB_BUCKET:
        try:
            influx_client_for_shutdown = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
            write_api_main = influx_client_for_shutdown.write_api(write_options=SYNCHRONOUS)
            logging.info(f"Successfully connected to InfluxDB: {INFLUXDB_URL}")
        except Exception as e:
            logging.error(f"Failed to connect to InfluxDB: {e}. InfluxDB integration will be disabled.")
            influx_client_for_shutdown = None # Ensure client is None if connection fails
            write_api_main = None
    else:
        logging.info("InfluxDB environment variables not fully set (URL, Token, Org, Bucket). Skipping InfluxDB integration.")

    try:
        main_loop(write_api_main) # Pass the write_api to the main loop
    except KeyboardInterrupt:
        logging.info("Ping Canary stopped by user.")
    finally:
        if producer:
            producer.flush()
            producer.close()
            logging.info("Kafka producer closed.")
        if influx_client_for_shutdown:
            influx_client_for_shutdown.close()
            logging.info("InfluxDB client closed.")
        logging.info("Ping Canary shutting down.")
