import dns.resolver
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
CANARY_ID = os.getenv('CANARY_ID', 'dns-canary-local-01')
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

        result["status"] = "SUCCESS"
        result["latency_ms"] = round((end_time - start_time) * 1000, 2)
        # Extract relevant parts of the answer, convert to string list
        result["answer"] = sorted([str(rdata) for rdata in answer])
        logging.debug(f"DNS query for {domain} {qtype} successful: {result['answer']}")

    except dns.resolver.NXDOMAIN:
        end_time = time.monotonic()
        result["status"] = "FAILURE"
        result["latency_ms"] = round((end_time - start_time) * 1000, 2)
        result["error_message"] = "NXDOMAIN (Domain does not exist)"
        logging.warning(f"NXDOMAIN for {domain} {qtype}")
    except dns.resolver.Timeout:
        result["status"] = "TIMEOUT"
        result["error_message"] = f"Query timed out after {timeout} seconds"
        logging.error(f"Timeout querying {domain} {qtype}")
    except dns.resolver.NoNameservers as e:
        result["status"] = "ERROR"
        result["error_message"] = f"No nameservers available: {e}"
        logging.error(f"No nameservers for {domain} {qtype}: {e}")
    except dns.resolver.NoAnswer:
        end_time = time.monotonic()
        result["status"] = "FAILURE" # Technically successful query, but no answer of the requested type
        result["latency_ms"] = round((end_time - start_time) * 1000, 2)
        result["error_message"] = f"No {qtype} record found (NoAnswer)"
        logging.warning(f"No {qtype} record found for {domain}")
    except Exception as e:
        result["status"] = "ERROR"
        result["error_message"] = f"An unexpected error occurred: {e}"
        logging.exception(f"Unexpected error querying {domain} {qtype}")

    return result

# --- Main Loop ---
def main():
    logging.info(f"Starting DNS Canary: {CANARY_ID}")
    logging.info(f"Targets: {TARGET_DOMAINS}")
    logging.info(f"Query Type: {QUERY_TYPE}")
    logging.info(f"Target Resolver: {TARGET_RESOLVER or 'System Default'}")
    logging.info(f"Interval: {DNS_INTERVAL_SECONDS}s")
    logging.info(f"Kafka Topic: {KAFKA_TOPIC}")

    while True:
        for domain in TARGET_DOMAINS:
            domain = domain.strip()
            if not domain:
                continue

            query_result = perform_dns_query(domain, QUERY_TYPE, TARGET_RESOLVER, QUERY_TIMEOUT_SECONDS)
            timestamp = datetime.now(timezone.utc).isoformat()

            message = {
                "type": "dns",
                "canary_id": CANARY_ID,
                "target": domain, # Use 'target' for consistency across canaries
                "timestamp": timestamp,
                "status": query_result["status"],
                "latency_ms": query_result["latency_ms"],
                "query_type": QUERY_TYPE,
                "resolver": query_result["resolver"],
                "answer": query_result["answer"],
                "error_message": query_result["error_message"]
            }

            try:
                future = producer.send(KAFKA_TOPIC, value=message)
                logging.info(f"Sent DNS result for {domain} to Kafka topic {KAFKA_TOPIC}")
            except Exception as e:
                logging.error(f"Failed to send message to Kafka: {e}")

        logging.info(f"Completed DNS query cycle. Sleeping for {DNS_INTERVAL_SECONDS} seconds...")
        time.sleep(DNS_INTERVAL_SECONDS)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("DNS Canary stopped by user.")
    finally:
        if producer:
            producer.flush()
            producer.close()
            logging.info("Kafka producer closed.")
