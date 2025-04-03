import pybgpstream
import time
import os
import logging
import threading
from prometheus_client import start_http_server, Counter, Gauge
from dotenv import load_dotenv
from datetime import datetime, timezone

# --- Configuration ---
load_dotenv()

# Comma-separated list of prefixes to monitor
PREFIXES_TO_MONITOR = os.getenv('PREFIXES_TO_MONITOR', '1.1.1.0/24,8.8.8.0/24').split(',')
# Comma-separated list of BGPStream projects (e.g., routeviews, ris)
BGPSTREAM_PROJECTS = os.getenv('BGPSTREAM_PROJECTS', 'routeviews,ris').split(',')
# Optional: Start time for BGPStream (e.g., 'YYYY-MM-DD HH:MM:SS'). If empty, uses live data.
BGPSTREAM_START_TIME = os.getenv('BGPSTREAM_START_TIME', '')
# Port for exposing Prometheus metrics
PROMETHEUS_PORT = int(os.getenv('PROMETHEUS_PORT', '8001'))
# Log level
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Prometheus Metrics Definition ---
BGP_ANNOUNCEMENTS_TOTAL = Counter(
    'bgp_announcements_total',
    'Total number of BGP announcements observed for monitored prefixes',
    ['prefix', 'peer_asn', 'origin_as', 'project']
)

BGP_WITHDRAWALS_TOTAL = Counter(
    'bgp_withdrawals_total',
    'Total number of BGP withdrawals observed for monitored prefixes',
    ['prefix', 'peer_asn', 'project']
)

# Gauge to track the timestamp of the last processed BGP event (useful for monitoring staleness)
BGP_LAST_EVENT_TIMESTAMP = Gauge(
    'bgp_last_event_timestamp_seconds',
    'Unix timestamp of the last processed BGP event',
    ['project']
)

# --- BGPStream Processing Function ---
def process_bgp_stream():
    """Connects to BGPStream and processes elements."""
    logging.info("Starting BGPStream processing...")
    logging.info(f"Monitoring Prefixes: {PREFIXES_TO_MONITOR}")
    logging.info(f"Using Projects: {BGPSTREAM_PROJECTS}")

    while True: # Keep trying to connect/reconnect
        try:
            # Create a new BGPStream instance
            stream = pybgpstream.BGPStream(
                # Use 'live' for real-time data, or specify time range
                from_time=BGPSTREAM_START_TIME if BGPSTREAM_START_TIME else None,
                until_time=None, # None means live/until now
                collectors=BGPSTREAM_PROJECTS, # Filter by project implicitly via collectors
                record_type="updates", # Process announcements and withdrawals
                filter=",".join([f"prefix {p}" for p in PREFIXES_TO_MONITOR]) # Filter for specific prefixes
            )

            logging.info("BGPStream instance created and filters applied.")

            for elem in stream:
                timestamp = time.time() # Record processing time
                project = elem.collector # Identify the source project/collector

                BGP_LAST_EVENT_TIMESTAMP.labels(project=project).set(timestamp)

                prefix = str(elem.fields.get('prefix'))
                peer_asn = str(elem.peer_asn)

                # Check if the element's prefix is one we are monitoring
                # Note: BGPStream filter should handle this, but double-check
                if prefix not in PREFIXES_TO_MONITOR:
                     # This might happen if a more specific prefix matches the filter
                     # logging.debug(f"Skipping element for non-monitored prefix {prefix}")
                     continue

                if elem.type == "A" or elem.type == "announce":
                    origin_as = 'N/A'
                    as_path = elem.fields.get("as-path", "")
                    if as_path:
                        # Origin AS is the last one in the path
                        origin_as = as_path.split(" ")[-1]

                    logging.info(f"ANNOUNCE: Prefix={prefix}, PeerAS={peer_asn}, OriginAS={origin_as}, Path={as_path}, Collector={project}")
                    BGP_ANNOUNCEMENTS_TOTAL.labels(
                        prefix=prefix,
                        peer_asn=peer_asn,
                        origin_as=origin_as,
                        project=project
                    ).inc()

                elif elem.type == "W" or elem.type == "withdraw":
                    logging.info(f"WITHDRAW: Prefix={prefix}, PeerAS={peer_asn}, Collector={project}")
                    BGP_WITHDRAWALS_TOTAL.labels(
                        prefix=prefix,
                        peer_asn=peer_asn,
                        project=project
                    ).inc()
                else:
                    logging.debug(f"Ignoring BGP element of type: {elem.type}")

        except Exception as e:
            logging.exception("Error during BGPStream processing. Restarting stream in 30 seconds...")
            time.sleep(30) # Wait before attempting to restart

# --- Main ---
def main():
    logging.info(f"Starting BGP Analyzer Service")
    logging.info(f"Exposing Prometheus metrics on port {PROMETHEUS_PORT}")

    # Start Prometheus HTTP server in a background thread
    try:
        start_http_server(PROMETHEUS_PORT)
        logging.info("Prometheus metrics server started.")
    except Exception as e:
        logging.exception(f"Failed to start Prometheus server on port {PROMETHEUS_PORT}")
        return # Exit if we can't expose metrics

    # Start BGP processing in the main thread (or optionally another thread)
    process_bgp_stream()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("BGP Analyzer stopped by user.")
    except Exception as e:
        logging.exception("Unhandled exception in BGP Analyzer main.")
    finally:
        logging.info("BGP Analyzer shutting down.")
