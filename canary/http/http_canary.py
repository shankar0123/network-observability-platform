import requests
import json
import os
import time
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'canary-results')
CANARY_ID = os.getenv('CANARY_ID', 'http-canary-local-01')
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

# --- HTTP Request Function ---
def perform_http_request(url, timeout=10, expected_string="", user_agent=""):
    """
    Performs an HTTP GET request and returns results.
    """
    logging.info(f"Requesting URL: {url}")
    result = {
        "status": "ERROR",
        "latency_ms": None,
        "status_code": None,
        "response_size_bytes": None,
        "content_match": None, # True, False, or None if not checked
        "error_message": None,
    }
    headers = {'User-Agent': user_agent} if user_agent else {}

    start_time = time.monotonic()
    try:
        response = requests.get(url, timeout=timeout, headers=headers, allow_redirects=True)
        end_time = time.monotonic()

        result["latency_ms"] = round((end_time - start_time) * 1000, 2)
        result["status_code"] = response.status_code
        result["response_size_bytes"] = len(response.content)

        if response.ok: # Status code < 400
            result["status"] = "SUCCESS"
            if expected_string:
                if expected_string in response.text:
                    result["content_match"] = True
                    logging.debug(f"Expected string '{expected_string}' found in response from {url}")
                else:
                    result["content_match"] = False
                    result["status"] = "FAILURE" # Mark as failure if content doesn't match
                    result["error_message"] = f"Expected string '{expected_string}' not found in response"
                    logging.warning(f"Expected string not found in response from {url}")
        else:
            result["status"] = "FAILURE"
            result["error_message"] = f"HTTP Error: {response.status_code} {response.reason}"
            logging.warning(f"HTTP error for {url}: {result['error_message']}")

    except requests.exceptions.Timeout:
        result["status"] = "TIMEOUT"
        result["error_message"] = f"Request timed out after {timeout} seconds"
        logging.error(f"Timeout requesting {url}")
    except requests.exceptions.RequestException as e:
        result["status"] = "ERROR"
        # Extract a more specific error if possible
        result["error_message"] = f"Request failed: {type(e).__name__} - {e}"
        logging.error(f"Request failed for {url}: {result['error_message']}")
    except Exception as e:
        result["status"] = "ERROR"
        result["error_message"] = f"An unexpected error occurred: {e}"
        logging.exception(f"Unexpected error requesting {url}")

    return result

# --- Main Loop ---
def main():
    logging.info(f"Starting HTTP Canary: {CANARY_ID}")
    logging.info(f"Target URLs: {TARGET_URLS}")
    logging.info(f"Interval: {HTTP_INTERVAL_SECONDS}s")
    logging.info(f"Kafka Topic: {KAFKA_TOPIC}")
    if EXPECTED_STRING:
        logging.info(f"Checking for string: '{EXPECTED_STRING}'")

    while True:
        for url in TARGET_URLS:
            url = url.strip()
            if not url:
                continue

            request_result = perform_http_request(url, REQUEST_TIMEOUT_SECONDS, EXPECTED_STRING, USER_AGENT)
            timestamp = datetime.now(timezone.utc).isoformat()

            message = {
                "type": "http",
                "canary_id": CANARY_ID,
                "target": url, # Use 'target' for consistency
                "timestamp": timestamp,
                "status": request_result["status"],
                "latency_ms": request_result["latency_ms"],
                "status_code": request_result["status_code"],
                "response_size_bytes": request_result["response_size_bytes"],
                "content_match": request_result["content_match"],
                "error_message": request_result["error_message"]
            }

            try:
                future = producer.send(KAFKA_TOPIC, value=message)
                logging.info(f"Sent HTTP result for {url} to Kafka topic {KAFKA_TOPIC}")
            except Exception as e:
                logging.error(f"Failed to send message to Kafka: {e}")

        logging.info(f"Completed HTTP request cycle. Sleeping for {HTTP_INTERVAL_SECONDS} seconds...")
        time.sleep(HTTP_INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("HTTP Canary stopped by user.")
    finally:
        if producer:
            producer.flush()
            producer.close()
            logging.info("Kafka producer closed.")
