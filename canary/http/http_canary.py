import requests
import requests
import json
import os
import time
import logging
from datetime import datetime, timezone # Ensure this is present
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


# --- Configuration ---
load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'canary-results')
CANARY_ID = os.getenv('CANARY_ID', 'http-canary-local-01') # Ensure CANARY_ID is defined
# InfluxDB Configuration
INFLUXDB_URL = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN', '')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG', '')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET', '')
# Comma-separated list of target URLs
TARGET_URLS = os.getenv('TARGET_URLS', 'https://google.com,https://github.com,https://httpbin.org/delay/1').split(',')
HTTP_INTERVAL_SECONDS = int(os.getenv('HTTP_INTERVAL_SECONDS', '60'))
REQUEST_TIMEOUT_SECONDS = int(os.getenv('REQUEST_TIMEOUT_SECONDS', '10'))
# Optional: String to check for in the response body
EXPECTED_STRING = os.getenv('EXPECTED_STRING', '')
# Optional: User-Agent header
USER_AGENT = os.getenv('USER_AGENT', f'NetworkObservabilityCanary/{CANARY_ID}')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Kafka Producer Setup ---
producer = None
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


# --- HTTP Request Function (renamed to check_url) ---
def check_url(url, method="GET", timeout=10, expected_string="", user_agent="", write_api=None):
    """
    Performs an HTTP request and returns results.
    Sends metrics to InfluxDB if write_api is provided.
    Note: 'method' parameter added for future use, currently only GET is performed.
    """
    logging.info(f"Requesting URL ({method}): {url}")
    result = {
        "status": "ERROR", # Overall status: SUCCESS, FAILURE, TIMEOUT, ERROR
        "latency_ms": None,
        "status_code": None,
        "response_size_bytes": None,
        "content_match": None, # True, False, or None if not checked
        "error_message": None,
        "ssl_expiry_days_left": None, # Placeholder, not implemented yet
    }
    headers = {'User-Agent': user_agent} if user_agent else {}

    start_time = time.monotonic()
    response = None
    success_flag = False # For InfluxDB success field

    try:
        if method.upper() == "GET":
            response = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
        # Add other methods like POST if needed in the future
        # elif method.upper() == "POST":
        # response = requests.post(url, timeout=timeout, headers=headers, data=payload, allow_redirects=True)
        else:
            result["error_message"] = f"Unsupported HTTP method: {method}"
            logging.error(result["error_message"])
            # No InfluxDB write here as it's a config error, not a target error
            return result # Early exit

        end_time = time.monotonic()
        duration_seconds = end_time - start_time
        result["latency_ms"] = round(duration_seconds * 1000, 2)
        result["status_code"] = response.status_code
        result["response_size_bytes"] = len(response.content)

        if response.ok: # Status code < 400
            result["status"] = "SUCCESS"
            success_flag = True
            if expected_string:
                if expected_string in response.text:
                    result["content_match"] = True
                    logging.debug(f"Expected string '{expected_string}' found in response from {url}")
                else:
                    result["content_match"] = False
                    result["status"] = "FAILURE" # Mark as failure if content doesn't match
                    success_flag = False
                    result["error_message"] = f"Expected string '{expected_string}' not found in response"
                    logging.warning(f"Expected string not found in response from {url}")
        else:
            result["status"] = "FAILURE"
            result["error_message"] = f"HTTP Error: {response.status_code} {response.reason}"
            logging.warning(f"HTTP error for {url}: {result['error_message']}")

    except requests.exceptions.Timeout:
        end_time = time.monotonic() # Record time even for timeout for partial latency
        duration_seconds = end_time - start_time
        result["latency_ms"] = round(duration_seconds * 1000, 2) # This will be close to timeout value
        result["status"] = "TIMEOUT"
        result["error_message"] = f"Request timed out after {timeout} seconds"
        logging.error(f"Timeout requesting {url}")
    except requests.exceptions.SSLError as e: # More specific SSL error handling
        end_time = time.monotonic()
        duration_seconds = end_time - start_time
        result["latency_ms"] = round(duration_seconds * 1000, 2)
        result["status"] = "ERROR"
        result["error_message"] = f"SSL Error: {type(e).__name__} - {e}"
        logging.error(f"SSL error for {url}: {result['error_message']}")
    except requests.exceptions.RequestException as e:
        end_time = time.monotonic()
        duration_seconds = end_time - start_time
        result["latency_ms"] = round(duration_seconds * 1000, 2)
        result["status"] = "ERROR"
        result["error_message"] = f"Request failed: {type(e).__name__} - {e}"
        logging.error(f"Request failed for {url}: {result['error_message']}")
    except Exception as e: # Catch-all for other unexpected errors
        end_time = time.monotonic()
        duration_seconds = end_time - start_time # Try to capture time if possible
        result["latency_ms"] = round(duration_seconds * 1000, 2) if start_time else None
        result["status"] = "ERROR"
        result["error_message"] = f"An unexpected error occurred: {e}"
        logging.exception(f"Unexpected error requesting {url}")

    # --- InfluxDB Data Sending ---
    if write_api:
        try:
            # Ensure duration_seconds is defined; it might not be if error occurred before request
            # For TIMEOUT or ERROR, latency_ms might be set, convert it back to seconds for Influx.
            # If latency_ms is None (e.g. pre-request error), maybe don't send duration.
            influx_duration = None
            if result["latency_ms"] is not None:
                 influx_duration = result["latency_ms"] / 1000.0

            # Point for duration and success status
            point_status = Point("http_canary_status") \
                .tag("target_url", url) \
                .tag("method", method.upper()) \
                .tag("canary_id", CANARY_ID) \
                .field("success", 1 if success_flag else 0)
            
            if influx_duration is not None:
                 point_status = point_status.field("duration_seconds", influx_duration)
            if result["status_code"] is not None:
                 point_status = point_status.field("status_code", result["status_code"])
            if result["response_size_bytes"] is not None:
                 point_status = point_status.field("response_size_bytes", result["response_size_bytes"])
            if result["content_match"] is not None: # content_match is boolean
                 point_status = point_status.field("content_match_status", 1 if result["content_match"] else 0)
            if result["status"] == "TIMEOUT": # Add a specific tag for timeout
                point_status = point_status.tag("error_type", "TIMEOUT")
            elif result["status"] == "ERROR" and result["error_message"]:
                 # Try to get a general error type
                 error_tag = "UNKNOWN_ERROR"
                 if "SSL Error" in result["error_message"]: error_tag = "SSL_ERROR"
                 elif "Request failed" in result["error_message"]: error_tag = "REQUEST_EXCEPTION"
                 point_status = point_status.tag("error_type", error_tag)


            point_status = point_status.time(datetime.now(timezone.utc), WritePrecision.NS)
            
            points_to_write = [point_status]

            # Point for SSL expiry if available (currently result["ssl_expiry_days_left"] is always None)
            if result["ssl_expiry_days_left"] is not None: # Check if it was calculated
                point_ssl = Point("http_canary_ssl_expiry") \
                    .tag("target_url", url) \
                    .tag("canary_id", CANARY_ID) \
                    .field("days_left", int(result["ssl_expiry_days_left"])) \
                    .time(datetime.now(timezone.utc), WritePrecision.NS)
                points_to_write.append(point_ssl)

            write_api.write(bucket=INFLUXDB_BUCKET, record=points_to_write)
            logging.debug(f"Sent HTTP check data to InfluxDB for {url}")
        except Exception as e:
            logging.error(f"Failed to send HTTP check data to InfluxDB for {url}: {e}")

    return result

# --- Main Loop Function ---
def main_loop(write_api_loop): # Accepts write_api from the main setup
    global producer # Ensure producer is accessible
    logging.info(f"Starting HTTP Canary main loop: {CANARY_ID}")
    logging.info(f"Target URLs: {TARGET_URLS}")
    logging.info(f"Interval: {HTTP_INTERVAL_SECONDS}s")
    if KAFKA_BROKER and producer:
        logging.info(f"Kafka Topic: {KAFKA_TOPIC}")
    else:
        logging.warning("Kafka is not configured or producer failed to initialize.")
    if write_api_loop:
        logging.info(f"InfluxDB integration enabled: {INFLUXDB_URL}")
    else:
        logging.info("InfluxDB integration disabled.")
    if EXPECTED_STRING:
        logging.info(f"Checking for string: '{EXPECTED_STRING}'")

    while True:
        for url in TARGET_URLS:
            url = url.strip()
            if not url:
                continue

            # Using "GET" method by default. This can be parameterized if needed.
            request_result = check_url(url, "GET", REQUEST_TIMEOUT_SECONDS, EXPECTED_STRING, USER_AGENT, write_api_loop)
            timestamp = datetime.now(timezone.utc).isoformat()

            message = {
                "type": "http",
                "canary_id": CANARY_ID,
                "target": url,
                "timestamp": timestamp,
                "status": request_result["status"],
                "latency_ms": request_result["latency_ms"],
                "status_code": request_result["status_code"],
                "response_size_bytes": request_result["response_size_bytes"],
                "content_match": request_result["content_match"],
                "error_message": request_result["error_message"],
                # "ssl_expiry_days_left": request_result["ssl_expiry_days_left"] # Add if implemented
            }
            
            if producer:
                try:
                    producer.send(KAFKA_TOPIC, value=message)
                    logging.info(f"Sent HTTP result for {url} to Kafka topic {KAFKA_TOPIC}")
                except Exception as e:
                    logging.error(f"Failed to send message to Kafka: {e}")
            else:
                logging.debug(f"Kafka producer not available. Skipping Kafka send for {url}.")


        logging.info(f"Completed HTTP request cycle. Sleeping for {HTTP_INTERVAL_SECONDS} seconds...")
        time.sleep(HTTP_INTERVAL_SECONDS)

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
            influx_client_for_shutdown = None
            write_api_main = None
    else:
        logging.info("InfluxDB environment variables not fully set. Skipping InfluxDB integration.")

    try:
        main_loop(write_api_main) # Pass the write_api to the main loop
    except KeyboardInterrupt:
        logging.info("HTTP Canary stopped by user.")
    finally:
        if producer:
            producer.flush()
            producer.close()
            logging.info("Kafka producer closed.")
        if influx_client_for_shutdown:
            influx_client_for_shutdown.close()
            logging.info("InfluxDB client closed.")
        logging.info("HTTP Canary shutting down.")
