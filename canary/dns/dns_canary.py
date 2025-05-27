import dns.resolver
import json
import os
import time
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# --- Configuration ---
load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'canary-results')
CANARY_ID = os.getenv('CANARY_ID', 'dns-canary-local-01')
# InfluxDB Configuration
INFLUXDB_URL = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN', '')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG', '')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET', '')
# Comma-separated list of target domains
TARGET_DOMAINS = os.getenv('TARGET_DOMAINS', 'google.com,github.com,cloudflare.com').split(',')
DNS_INTERVAL_SECONDS = int(os.getenv('DNS_INTERVAL_SECONDS', '60'))
QUERY_TYPE = os.getenv('QUERY_TYPE', 'A') # e.g., A, AAAA, CNAME, MX, NS, TXT
# Optional: Specify a target resolver IP. If empty, use system default.
TARGET_RESOLVER = os.getenv('TARGET_RESOLVER', '') # e.g., '8.8.8.8'
QUERY_TIMEOUT_SECONDS = int(os.getenv('QUERY_TIMEOUT_SECONDS', '5'))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Kafka Producer Setup ---
producer = None
# Ensure KAFKA_BROKER is set before attempting to connect
if KAFKA_BROKER:
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                retries=5,
                request_timeout_ms=30000
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

# --- DNS Query Function ---
def perform_dns_query(domain, qtype, resolver_ip=None, timeout=5, write_api=None): # Added write_api
    """
    Performs a DNS query using dnspython and returns results.
    """
    logging.info(f"Querying {domain} for {qtype} record (Resolver: {resolver_ip or 'System Default'})")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
            request_timeout_ms=30000
        )
        logging.info(f"Successfully connected to Kafka broker at {KAFKA_BROKER}")
    except NoBrokersAvailable:
        logging.error(f"Kafka broker at {KAFKA_BROKER} not available. Retrying in 10 seconds...")
        time.sleep(10)
    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}. Retrying in 10 seconds...")
        time.sleep(10)

# --- DNS Query Function ---
def perform_dns_query(domain, qtype, resolver_ip=None, timeout=5):
    """
    Performs a DNS query using dnspython and returns results.
    """
    logging.info(f"Querying {domain} for {qtype} record (Resolver: {resolver_ip or 'System Default'})")
    result = {
        "status": "ERROR",
        "latency_ms": None,
        "answer": None, # List of strings from the answer
        "error_message": None,
        "resolver": resolver_ip or 'System Default'
    }
    resolver = dns.resolver.Resolver()
    resolver.timeout = timeout
    resolver.lifetime = timeout # Total time including retries

    if resolver_ip:
        resolver.nameservers = [resolver_ip]

    start_time = time.monotonic()
    try:
        answer = resolver.resolve(domain, qtype)
        end_time = time.monotonic()
        latency_ms = round((end_time - start_time) * 1000, 2)

        result["status"] = "SUCCESS"
        result["latency_ms"] = latency_ms
        # Extract relevant parts of the answer, convert to string list
        result["answer"] = sorted([str(rdata) for rdata in answer])
        logging.debug(f"DNS query for {domain} {qtype} successful: {result['answer']}")

        if write_api:
            try:
                point_time = Point("dns_query_time_seconds") \
                    .tag("target_server", resolver_ip or "System Default") \
                    .tag("domain", domain) \
                    .tag("query_type", qtype) \
                    .tag("canary_id", CANARY_ID) \
                    .field("duration", latency_ms / 1000.0) \
                    .time(datetime.now(timezone.utc), WritePrecision.NS)

                point_success = Point("dns_query_success") \
                    .tag("target_server", resolver_ip or "System Default") \
                    .tag("domain", domain) \
                    .tag("query_type", qtype) \
                    .tag("canary_id", CANARY_ID) \
                    .field("status", 1) \
                    .time(datetime.now(timezone.utc), WritePrecision.NS) # Success is 1

                write_api.write(bucket=INFLUXDB_BUCKET, record=[point_time, point_success])
                logging.debug(f"Sent DNS query data to InfluxDB for {domain} (resolver: {result['resolver']})")
            except Exception as e:
                logging.error(f"Failed to send DNS query data to InfluxDB: {e}")

    except dns.resolver.NXDOMAIN:
        end_time = time.monotonic()
        latency_ms = round((end_time - start_time) * 1000, 2)
        result["status"] = "FAILURE"
        result["latency_ms"] = latency_ms
        result["error_message"] = "NXDOMAIN (Domain does not exist)"
        logging.warning(f"NXDOMAIN for {domain} {qtype}")
        if write_api:
            try:
                point_failure = Point("dns_query_success") \
                    .tag("target_server", resolver_ip or "System Default") \
                    .tag("domain", domain) \
                    .tag("query_type", qtype) \
                    .tag("canary_id", CANARY_ID) \
                    .tag("error_type", "NXDOMAIN") \
                    .field("status", 0) \
                    .time(datetime.now(timezone.utc), WritePrecision.NS)
                # Also send latency for failures if meaningful
                point_time = Point("dns_query_time_seconds") \
                    .tag("target_server", resolver_ip or "System Default") \
                    .tag("domain", domain) \
                    .tag("query_type", qtype) \
                    .tag("canary_id", CANARY_ID) \
                    .field("duration", latency_ms / 1000.0) \
                    .time(datetime.now(timezone.utc), WritePrecision.NS)
                write_api.write(bucket=INFLUXDB_BUCKET, record=[point_failure, point_time])
                logging.debug(f"Sent DNS failure (NXDOMAIN) data to InfluxDB for {domain}")
            except Exception e_influx:
                logging.error(f"Failed to send NXDOMAIN data to InfluxDB: {e_influx}")

    except dns.resolver.Timeout:
        # Latency is not typically recorded for timeouts in the same way, as it's the timeout value itself
        result["status"] = "TIMEOUT"
        result["error_message"] = f"Query timed out after {timeout} seconds"
        logging.error(f"Timeout querying {domain} {qtype}")
        if write_api:
            try:
                point_timeout = Point("dns_query_success") \
                    .tag("target_server", resolver_ip or "System Default") \
                    .tag("domain", domain) \
                    .tag("query_type", qtype) \
                    .tag("canary_id", CANARY_ID) \
                    .tag("error_type", "TIMEOUT") \
                    .field("status", 0) \
                    .time(datetime.now(timezone.utc), WritePrecision.NS)
                write_api.write(bucket=INFLUXDB_BUCKET, record=point_timeout)
                logging.debug(f"Sent DNS timeout data to InfluxDB for {domain}")
            except Exception as e_influx:
                logging.error(f"Failed to send TIMEOUT data to InfluxDB: {e_influx}")

    except dns.resolver.NoNameservers as e:
        result["status"] = "ERROR"
        result["error_message"] = f"No nameservers available: {e}"
        logging.error(f"No nameservers for {domain} {qtype}: {e}")
        if write_api:
            try:
                point_error = Point("dns_query_success") \
                    .tag("target_server", resolver_ip or "System Default") \
                    .tag("domain", domain) \
                    .tag("query_type", qtype) \
                    .tag("canary_id", CANARY_ID) \
                    .tag("error_type", "NoNameservers") \
                    .field("status", 0) \
                    .time(datetime.now(timezone.utc), WritePrecision.NS)
                write_api.write(bucket=INFLUXDB_BUCKET, record=point_error)
                logging.debug(f"Sent DNS error (NoNameservers) data to InfluxDB for {domain}")
            except Exception as e_influx:
                logging.error(f"Failed to send NoNameservers error data to InfluxDB: {e_influx}")

    except dns.resolver.NoAnswer:
        end_time = time.monotonic()
        latency_ms = round((end_time - start_time) * 1000, 2)
        result["status"] = "FAILURE" # Technically successful query, but no answer of the requested type
        result["latency_ms"] = latency_ms
        result["error_message"] = f"No {qtype} record found (NoAnswer)"
        logging.warning(f"No {qtype} record found for {domain}")
        if write_api:
            try:
                # Send success for query time, but failure for answer status
                point_time = Point("dns_query_time_seconds") \
                    .tag("target_server", resolver_ip or "System Default") \
                    .tag("domain", domain) \
                    .tag("query_type", qtype) \
                    .tag("canary_id", CANARY_ID) \
                    .field("duration", latency_ms / 1000.0) \
                    .time(datetime.now(timezone.utc), WritePrecision.NS)
                point_noanswer = Point("dns_query_success") \
                    .tag("target_server", resolver_ip or "System Default") \
                    .tag("domain", domain) \
                    .tag("query_type", qtype) \
                    .tag("canary_id", CANARY_ID) \
                    .tag("error_type", "NoAnswer") \
                    .field("status", 0) \
                    .time(datetime.now(timezone.utc), WritePrecision.NS)
                write_api.write(bucket=INFLUXDB_BUCKET, record=[point_time, point_noanswer])
                logging.debug(f"Sent DNS NoAnswer data to InfluxDB for {domain}")
            except Exception as e_influx:
                logging.error(f"Failed to send NoAnswer data to InfluxDB: {e_influx}")

    except Exception as e:
        # Generic error, latency might not be available or meaningful
        result["status"] = "ERROR"
        result["error_message"] = f"An unexpected error occurred: {e}"
        logging.exception(f"Unexpected error querying {domain} {qtype}")
        if write_api:
            try:
                point_error = Point("dns_query_success") \
                    .tag("target_server", resolver_ip or "System Default") \
                    .tag("domain", domain) \
                    .tag("query_type", qtype) \
                    .tag("canary_id", CANARY_ID) \
                    .tag("error_type", "Unknown") \
                    .field("status", 0) \
                    .time(datetime.now(timezone.utc), WritePrecision.NS)
                write_api.write(bucket=INFLUXDB_BUCKET, record=point_error)
                logging.debug(f"Sent DNS generic error data to InfluxDB for {domain}")
            except Exception as e_influx:
                logging.error(f"Failed to send generic error data to InfluxDB: {e_influx}")
    return result

# --- Main Loop ---
def main():
    logging.info(f"Starting DNS Canary: {CANARY_ID}")
    logging.info(f"Targets: {TARGET_DOMAINS}")
    logging.info(f"Query Type: {QUERY_TYPE}")
    logging.info(f"Target Resolver: {TARGET_RESOLVER or 'System Default'}")
    logging.info(f"Interval: {DNS_INTERVAL_SECONDS}s")
    if KAFKA_BROKER:
        logging.info(f"Kafka Topic: {KAFKA_TOPIC}")
    else:
        logging.warning("Kafka is not configured.")

    # --- InfluxDB Client Setup ---
    influx_client = None
    write_api = None
    if INFLUXDB_URL and INFLUXDB_TOKEN and INFLUXDB_ORG and INFLUXDB_BUCKET:
        try:
            influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
            write_api = influx_client.write_api(write_options=SYNCHRONOUS)
            logging.info(f"Successfully connected to InfluxDB: {INFLUXDB_URL}")
        except Exception as e:
            logging.error(f"Failed to connect to InfluxDB: {e}. InfluxDB integration will be disabled.")
            influx_client = None # Ensure client is None if connection fails
            write_api = None
    else:
        logging.info("InfluxDB environment variables not fully set. Skipping InfluxDB integration.")


    while True:
        for domain in TARGET_DOMAINS:
            domain = domain.strip()
            if not domain:
                continue

            query_result = perform_dns_query(domain, QUERY_TYPE, TARGET_RESOLVER, QUERY_TIMEOUT_SECONDS, write_api) # Pass write_api
            timestamp = datetime.now(timezone.utc).isoformat()

            message = {
                "type": "dns", # This identifies the message type for Kafka consumers
                "canary_id": CANARY_ID,
                "target": domain,
                "timestamp": timestamp,
                "status": query_result["status"],
                "latency_ms": query_result["latency_ms"],
                "query_type": QUERY_TYPE,
                "resolver": query_result["resolver"],
                "answer": query_result["answer"], # List of IPs or other records
                "error_message": query_result["error_message"]
            }
            
            if producer: # Only send to Kafka if producer is initialized
                try:
                    future = producer.send(KAFKA_TOPIC, value=message)
                    # Optionally, can add .get(timeout=10) to wait for ack, but makes it blocking
                    logging.info(f"Sent DNS result for {domain} to Kafka topic {KAFKA_TOPIC}")
                except Exception as e:
                    logging.error(f"Failed to send message to Kafka: {e}")
            else:
                logging.debug(f"Kafka producer not available. Skipping Kafka send for {domain}.")


        logging.info(f"Completed DNS query cycle. Sleeping for {DNS_INTERVAL_SECONDS} seconds...")
        time.sleep(DNS_INTERVAL_SECONDS)

if __name__ == "__main__":
    influx_client_main = None # Define here to be accessible in finally
    try:
        # InfluxDB client setup is now inside main() and write_api is passed around.
        # However, the client itself needs to be closed here.
        # To do this cleanly, main() would need to return the client,
        # or we find another way to access it.
        # For now, let's adjust main to initialize it and handle it there.
        # The prompt's example implies influx_client is accessible in this __main__ scope.
        # Let's assume main() will assign to a global or a class member if needed,
        # or that the example's structure is slightly different.
        # Given the current structure, the influx_client is local to main().
        # We need to adjust this.
        # Re-evaluating: The prompt's example for closing is in the __main__ block's finally.
        # This means the client must be initialized or made available in this scope.

        # For simplicity and to match the example, we'll initialize Influx client here
        # and pass it to main, which then passes write_api to perform_dns_query.
        # This is a bit of a structural change from the original dns_canary.
        
        # --- InfluxDB Client Setup (moved here for graceful shutdown per example) ---
        # This is somewhat redundant with the setup in main(), but aligns with prompt's close example.
        # A better refactor would be to have main() return the client or use a class.
        # For this task, let's follow the prompt's structure for closing.
        # The `main` function will initialize its own `influx_client` and `write_api`
        # The `finally` block here will attempt to close `influx_client_main`
        # This is not ideal, the client should be managed by the `main` function or a class.
        # I will keep the client initialization within main as it's cleaner,
        # and adjust the finally block to reflect that the client is managed there.
        # The prompt example for closing was a general guide.

        main() # main handles its own influx client setup and closure via its own try/finally

    except KeyboardInterrupt:
        logging.info("DNS Canary stopped by user.")
    # The finally block in main() will handle Kafka and InfluxDB client closure.
    # No, the prompt clearly shows influx_client being closed in the __name__ == "__main__" block's finally.
    # This means main() should not have its own try/finally for the client, or it should return the client.

    # Let's re-do the main structure slightly to align with the prompt's closing pattern.
    # The main() will not have its own try/finally for client closure.
    # Client will be initialized before calling main_loop (new name for the loop part of main)
    # and closed in the finally block here.

# Redefining main and adding main_loop to fit the close pattern
influx_client_for_shutdown = None # Global for access in finally

def main_loop(write_api_loop): # write_api_loop is the write_api from the main setup
    global producer # Make sure producer is accessible or passed
    logging.info(f"Starting DNS Canary main loop: {CANARY_ID}")
    # logging calls from original main() that are relevant before the loop
    logging.info(f"Targets: {TARGET_DOMAINS}")
    logging.info(f"Query Type: {QUERY_TYPE}")
    logging.info(f"Target Resolver: {TARGET_RESOLVER or 'System Default'}")
    logging.info(f"Interval: {DNS_INTERVAL_SECONDS}s")
    if KAFKA_BROKER and producer:
        logging.info(f"Kafka Topic: {KAFKA_TOPIC}")
    else:
        logging.warning("Kafka is not configured or producer failed to initialize.")
    if write_api_loop:
        logging.info(f"InfluxDB integration enabled: {INFLUXDB_URL}")
    else:
        logging.info("InfluxDB integration disabled.")

    while True:
        for domain in TARGET_DOMAINS:
            domain = domain.strip()
            if not domain:
                continue

            query_result = perform_dns_query(domain, QUERY_TYPE, TARGET_RESOLVER, QUERY_TIMEOUT_SECONDS, write_api_loop)
            timestamp = datetime.now(timezone.utc).isoformat()

            message = {
                "type": "dns",
                "canary_id": CANARY_ID,
                "target": domain,
                "timestamp": timestamp,
                "status": query_result["status"],
                "latency_ms": query_result["latency_ms"],
                "query_type": QUERY_TYPE,
                "resolver": query_result["resolver"],
                "answer": query_result["answer"],
                "error_message": query_result["error_message"]
            }
            
            if producer:
                try:
                    producer.send(KAFKA_TOPIC, value=message)
                    logging.info(f"Sent DNS result for {domain} to Kafka topic {KAFKA_TOPIC}")
                except Exception as e:
                    logging.error(f"Failed to send message to Kafka: {e}")
            else:
                logging.debug(f"Kafka producer not available. Skipping Kafka send for {domain}.")

        logging.info(f"Completed DNS query cycle. Sleeping for {DNS_INTERVAL_SECONDS} seconds...")
        time.sleep(DNS_INTERVAL_SECONDS)

if __name__ == "__main__":
    # Kafka producer is already initialized globally
    # InfluxDB client setup
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
        logging.info("InfluxDB environment variables not fully set. Skipping InfluxDB integration.")

    try:
        main_loop(write_api_main) # Pass the write_api to the main loop
    except KeyboardInterrupt:
        logging.info("DNS Canary stopped by user.")
    finally:
        if producer:
            producer.flush()
            producer.close()
            logging.info("Kafka producer closed.")
        if influx_client_for_shutdown:
            influx_client_for_shutdown.close()
            logging.info("InfluxDB client closed.")
        logging.info("DNS Canary shutting down.")
