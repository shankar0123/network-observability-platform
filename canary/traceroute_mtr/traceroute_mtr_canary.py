import os
import subprocess
import json
import time
import logging
import re # Added for traceroute parsing
from datetime import datetime, timezone
from dotenv import load_dotenv

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
# from kafka import KafkaProducer # Optional: if using Kafka

# --- Configuration ---
load_dotenv()

TARGET_HOSTS = os.getenv('TARGET_HOSTS', 'google.com,8.8.8.8').split(',')
TEST_INTERVAL_SECONDS = int(os.getenv('TEST_INTERVAL_SECONDS', '300'))
# MTR_ENABLE = os.getenv('MTR_ENABLE', 'True').lower() == 'true' # Will use this later
MTR_CYCLES = int(os.getenv('MTR_CYCLES', '10'))
MTR_MAX_HOPS = int(os.getenv('MTR_MAX_HOPS', '30'))
TRACEROUTE_ENABLE = os.getenv('TRACEROUTE_ENABLE', 'True').lower() == 'true'
TRACEROUTE_MAX_HOPS = int(os.getenv('TRACEROUTE_MAX_HOPS', '30'))

INFLUXDB_URL = os.getenv('INFLUXDB_URL', 'http://localhost:8086')
INFLUXDB_TOKEN = os.getenv('INFLUXDB_TOKEN', '')
INFLUXDB_ORG = os.getenv('INFLUXDB_ORG', '')
INFLUXDB_BUCKET = os.getenv('INFLUXDB_BUCKET', '')
CANARY_ID = os.getenv('CANARY_ID', 'traceroute-mtr-canary-local')

LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')

# Optional Kafka Configuration
# KAFKA_BROKER = os.getenv('KAFKA_BROKER')
# KAFKA_TOPIC_MTR = os.getenv('KAFKA_TOPIC_MTR', 'mtr_results')
# kafka_producer = None

# --- InfluxDB Client Setup ---
influx_write_api = None
influx_client = None
if INFLUXDB_URL and INFLUXDB_TOKEN and INFLUXDB_ORG and INFLUXDB_BUCKET:
    try:
        influx_client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
        influx_write_api = influx_client.write_api(write_options=SYNCHRONOUS)
        logging.info(f"Successfully connected to InfluxDB: {INFLUXDB_URL}")
    except Exception as e:
        logging.error(f"Failed to connect to InfluxDB: {e}")
        influx_client = None
        influx_write_api = None
else:
    logging.info("InfluxDB environment variables not fully set. Skipping InfluxDB integration.")

# Optional Kafka Producer Setup
# if KAFKA_BROKER:
#     try:
#         kafka_producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER,
#                                        value_serializer=lambda v: json.dumps(v).encode('utf-8'))
#         logging.info(f"Kafka producer initialized for broker: {KAFKA_BROKER}")
#     except Exception as e:
#         logging.error(f"Failed to initialize Kafka producer: {e}")
#         kafka_producer = None


def run_mtr(target_host: str, cycles: int, max_hops: int):
    mtr_command = [
        'mtr',
        '-n',        # No DNS resolution for hops in mtr output
        '--json',
        '-c', str(cycles),
        '-m', str(max_hops),
        target_host
    ]
    logging.info(f"Running MTR for {target_host}: {' '.join(mtr_command)}")
    
    try:
        process = subprocess.run(mtr_command, capture_output=True, text=True, timeout=60 + cycles * 2) # Generous timeout
        
        if process.returncode != 0 and not process.stdout: # If MTR fails and produces no JSON
            logging.error(f"MTR command failed for {target_host}. Return code: {process.returncode}, Error: {process.stderr}")
            # Send error metric to InfluxDB if needed
            if influx_write_api:
                point = Point("mtr_execution_error") \
                    .tag("canary_id", CANARY_ID) \
                    .tag("target_host", target_host) \
                    .field("return_code", process.returncode) \
                    .field("error_message", process.stderr[:1024]) \
                    .time(datetime.now(timezone.utc), WritePrecision.NS)
                try:
                    influx_write_api.write(bucket=INFLUXDB_BUCKET, record=point)
                except Exception as e_db:
                    logging.error(f"Failed to write MTR execution error to InfluxDB: {e_db}")
            return None # Indicate MTR run failed before JSON parsing

        # MTR with --json might still exit with non-zero if target is unreachable, but produce valid JSON.
        # So, prioritize parsing stdout if it exists.
        if not process.stdout:
            logging.warning(f"MTR for {target_host} produced no stdout. stderr: {process.stderr}")
            # This case might be similar to the one above if returncode is also non-zero.
            # If returncode is 0 but no stdout, that's odd.
            return None


        logging.debug(f"MTR raw output for {target_host}: {process.stdout}")
        mtr_data = json.loads(process.stdout)
        return mtr_data.get('report', {}) # The actual MTR report is under 'report' key

    except subprocess.TimeoutExpired:
        logging.error(f"MTR command timed out for {target_host}")
        # Send error metric to InfluxDB
        if influx_write_api:
            point = Point("mtr_execution_error") \
                .tag("canary_id", CANARY_ID) \
                .tag("target_host", target_host) \
                .field("return_code", -1) \
                .field("error_message", "TimeoutExpired") \
                .time(datetime.now(timezone.utc), WritePrecision.NS)
            try:
                influx_write_api.write(bucket=INFLUXDB_BUCKET, record=point)
            except Exception as e_db:
                logging.error(f"Failed to write MTR timeout error to InfluxDB: {e_db}")
        return None
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse MTR JSON output for {target_host}: {e}. Output: {process.stdout}")
        return None
    except Exception as e:
        logging.error(f"An unexpected error occurred running MTR for {target_host}: {e}")
        return None

# --- Traceroute Functions ---
def parse_traceroute_line(line: str, hop_counter_expected: int):
    # Using the simpler regex approach first, will test and refine.
    # Expected format: " 1  some.host.com (1.2.3.4)  1.000 ms  2.000 ms  3.000 ms"
    # Or:             " 1  1.2.3.4  1.000 ms  2.000 ms  3.000 ms" (older traceroute or -n)
    # Or timeouts:    " 2  * * *"
    
    # Handle fully timed out lines first
    timeout_match = re.match(r"^\s*(\d+)\s+(\*\s+\*\s+\*)", line)
    if timeout_match:
        hop_num = int(timeout_match.group(1))
        # Basic validation against expected hop number, can be made more strict
        # if hop_num != hop_counter_expected:
        #     logging.warning(f"Traceroute parse: Expected hop {hop_counter_expected} for timeout line, got {hop_num}. Line: '{line}'")
        #     return None # Or adjust logic if non-sequential hops are expected from some traceroutes
        return {
            "hop": hop_num, "hostname": "*", "ip_address": "*",
            "rtts_ms": [None, None, None], "loss_percent": 100.0
        }

    # Regex for lines with host/IP and RTTs
    # Grp1: Hop, Grp2: Hostname (opt), Grp3: IP if hostname, Grp4: IP if no hostname, Grp5: RTTs part
    line_match = re.match(
        r"^\s*(\d+)\s+"                                   # 1: Hop Number
        r"(?:([\w\.-]+)\s+\(([\d\.]+)\)|([\d\.]+))" +      # 2: Hostname (optional), 3: IP_if_hostname, 4: IP_if_no_hostname
        r"\s*(.*)",                                        # 5: Rest of the line (RTTs)
        line
    )

    if not line_match:
        return None

    hop_num = int(line_match.group(1))
    # if hop_num != hop_counter_expected:
    #     logging.warning(f"Traceroute parse: Expected hop {hop_counter_expected}, got {hop_num}. Line: '{line}'")
    #     return None

    hostname = line_match.group(2)
    ip_address = line_match.group(3) if hostname else line_match.group(4)
    
    rtt_part = line_match.group(5).strip()
    # Extracts RTT numbers or '*' or other non-numeric like 'ms' if not careful.
    # Refined regex to only capture numbers before ' ms' or '*'
    rtts_str = re.findall(r"([\d\.]+)\s+ms|\*", rtt_part) 

    rtts_ms = []
    loss_count = 0
    # Defaulting to 3 probes as per typical traceroute -q 3
    num_probes_expected = 3 

    for rtt_s in rtts_str:
        if rtt_s == '*':
            rtts_ms.append(None)
            loss_count += 1
        else:
            try:
                rtts_ms.append(float(rtt_s))
            except ValueError: # Should be rare with refined regex
                rtts_ms.append(None) 
                loss_count += 1
    
    # Pad with None if fewer RTTs found than expected (e.g. if some probes don't return)
    while len(rtts_ms) < num_probes_expected:
        rtts_ms.append(None)
        loss_count += 1
    
    # Ensure we don't exceed num_probes_expected if regex was too greedy (shouldn't be)
    rtts_ms = rtts_ms[:num_probes_expected]
    loss_count = sum(1 for rtt in rtts_ms if rtt is None)


    return {
        "hop": hop_num,
        "hostname": hostname if hostname else ip_address, # Use IP as hostname if hostname not resolved
        "ip_address": ip_address,
        "rtts_ms": rtts_ms, 
        "loss_percent": (loss_count / num_probes_expected * 100) if num_probes_expected > 0 else 100.0
    }


def run_traceroute(target_host: str, max_hops: int):
    traceroute_command = [
        'traceroute',
        '-n',      # No DNS resolution for hop addresses in output (IPs only)
        '-q', '3', # Send 3 probes per hop
        '-w', '2', # Wait 2 seconds per probe reply
        '-m', str(max_hops),
        target_host
    ]
    logging.info(f"Running Traceroute for {target_host}: {' '.join(traceroute_command)}")
    
    parsed_hops = []
    try:
        # Timeout: max_hops * probes_per_hop * wait_time_per_probe + buffer
        timeout_seconds = max_hops * 3 * 2 + 30 
        process = subprocess.run(traceroute_command, capture_output=True, text=True, timeout=timeout_seconds)
        
        logging.debug(f"Traceroute raw output for {target_host}:\nSTDOUT:\n{process.stdout}\nSTDERR:\n{process.stderr}")

        # Standard traceroute usually exits 0 if it reaches the target or completes max_hops.
        # Error conditions might be indicated by stderr or non-zero exit if e.g. host unknown.
        if process.returncode != 0 and not process.stdout.strip(): # Check stdout too, as some errors print to stderr but still output partial trace
             logging.error(f"Traceroute command failed for {target_host}. Return code: {process.returncode}, Error: {process.stderr}")
             if influx_write_api:
                point = Point("traceroute_execution_error") \
                    .tag("canary_id", CANARY_ID) \
                    .tag("target_host", target_host) \
                    .field("return_code", process.returncode) \
                    .field("error_message", process.stderr[:1024]) \
                    .time(datetime.now(timezone.utc), WritePrecision.NS)
                try:
                    influx_write_api.write(bucket=INFLUXDB_BUCKET, record=point)
                except Exception as e_db:
                    logging.error(f"Failed to write Traceroute execution error to InfluxDB: {e_db}")
             return []

        hop_counter = 1
        # Traceroute output often has a header line, e.g., "traceroute to google.com (172.217.160.142), 30 hops max, 60 byte packets"
        # And sometimes lines with only hop numbers if intermediate probes timeout before a responsive one.
        for line in process.stdout.splitlines():
            line = line.strip()
            if not line or "traceroute to" in line.lower() or "hops max" in line.lower() or "byte packets" in line.lower():
                continue 
            
            parsed_hop = parse_traceroute_line(line, hop_counter)
            if parsed_hop:
                # Ensure hop numbers are sequential from our parsing perspective
                # This handles cases where traceroute might skip printing non-responsive hops entirely
                # or if our parser misses a line.
                if parsed_hop['hop'] == hop_counter:
                    parsed_hops.append(parsed_hop)
                    hop_counter +=1
                elif parsed_hop['hop'] > hop_counter: # Jump in hop numbers
                    # Fill in intermediate hops as '*' (fully timed out)
                    for i in range(hop_counter, parsed_hop['hop']):
                        parsed_hops.append({"hop": i, "hostname": "*", "ip_address": "*", "rtts_ms": [None,None,None], "loss_percent": 100.0})
                    parsed_hops.append(parsed_hop)
                    hop_counter = parsed_hop['hop'] + 1
                # else: hop number is less than current hop_counter, might be repeated or malformed, ignoring for now.
                    
            else:
                logging.warning(f"Could not parse traceroute line for {target_host}: '{line}'")
        
        return parsed_hops

    except subprocess.TimeoutExpired:
        logging.error(f"Traceroute command timed out for {target_host}")
        if influx_write_api:
            point = Point("traceroute_execution_error") \
                .tag("canary_id", CANARY_ID) \
                .tag("target_host", target_host) \
                .field("return_code", -1) \
                .field("error_message", "TimeoutExpired") \
                .time(datetime.now(timezone.utc), WritePrecision.NS)
            try:
                influx_write_api.write(bucket=INFLUXDB_BUCKET, record=point)
            except Exception as e_db:
                logging.error(f"Failed to write Traceroute timeout error to InfluxDB: {e_db}")
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred running Traceroute for {target_host}: {e}")
        return []


def process_mtr_data_to_influxdb(mtr_report_data, target_host):
    if not influx_write_api or not mtr_report_data or 'hubs' not in mtr_report_data:
        logging.debug(f"Skipping InfluxDB submission for MTR data of {target_host} (no API, no data, or no hubs).")
        return

    points = []
    report_time = datetime.now(timezone.utc) # Changed from datetime_now (which was not defined)

    for hop_data in mtr_report_data.get('hubs', []):
        hop_index = hop_data.get('count')
        # MTR JSON output uses 'host' for IP, 'ASN' for AS (often needs mtr-tiny or full mtr for ASN)
        hop_ip = hop_data.get('host') 
        
        if hop_ip is None or hop_index is None: # Skip if essential data missing
            logging.debug(f"Skipping MTR hop due to missing IP or index: {hop_data} for {target_host}")
            continue

        point = Point("mtr_hop_stats") \
            .tag("canary_id", CANARY_ID) \
            .tag("target_host", target_host) \
            .tag("hop_index", int(hop_index)) \
            .tag("hop_ip", hop_ip) \
            .field("loss_percent", float(hop_data.get('Loss%', 0.0))) \
            .field("packets_sent", int(hop_data.get('Snt', 0))) \
            .field("last_latency_ms", float(hop_data.get('Last', 0.0))) \
            .field("avg_latency_ms", float(hop_data.get('Avg', 0.0))) \
            .field("best_latency_ms", float(hop_data.get('Best', 0.0))) \
            .field("worst_latency_ms", float(hop_data.get('Wrst', 0.0))) \
            .field("stddev_latency_ms", float(hop_data.get('StDev', 0.0))) \
            .time(report_time, WritePrecision.NS)
        
        # Optional: Add ASN if available (mtr --json output provides it as 'ASN')
        # hop_asn = hop_data.get('ASN')
        # if hop_asn:
        #    point = point.tag("hop_asn", hop_asn.replace("AS", "")) # Remove "AS" prefix

        points.append(point)

    if points:
        try:
            influx_write_api.write(bucket=INFLUXDB_BUCKET, record=points)
            logging.info(f"Successfully sent {len(points)} MTR hop data points to InfluxDB for {target_host}")
        except Exception as e:
            logging.error(f"Failed to write MTR data to InfluxDB for {target_host}: {e}")


def process_traceroute_data_to_influxdb(traceroute_hops, target_host):
    if not influx_write_api or not traceroute_hops:
        logging.debug(f"Skipping InfluxDB submission for Traceroute data of {target_host} (no API or no data).")
        return

    points = []
    report_time = datetime.now(timezone.utc) # Using the global datetime import

    for hop_data in traceroute_hops:
        point = Point("traceroute_hop_stats") \
            .tag("canary_id", CANARY_ID) \
            .tag("target_host", target_host) \
            .tag("hop_index", int(hop_data["hop"])) \
            .tag("hop_ip", str(hop_data["ip_address"])) \
            .tag("hop_hostname", str(hop_data["hostname"])) \
            .field("loss_percent", float(hop_data.get("loss_percent", 100.0))) \
            .time(report_time, WritePrecision.NS)

        # Add RTT fields if available (rtts_ms contains up to 3 values)
        rtts = hop_data.get("rtts_ms", [])
        # Ensure rtts is treated as a list, even if somehow it's not (defensive)
        if not isinstance(rtts, list): rtts = [] 
            
        for i in range(3): # Assuming up to 3 RTTs (rtt1, rtt2, rtt3)
            rtt_val = None
            if i < len(rtts):
                rtt_val = rtts[i]
            
            if rtt_val is not None:
                point = point.field(f"rtt{i+1}_ms", float(rtt_val))
            else: 
                point = point.field(f"rtt{i+1}_ms", -1.0) # Example: -1 for timeout/loss for this probe

        points.append(point)

    if points:
        try:
            influx_write_api.write(bucket=INFLUXDB_BUCKET, record=points)
            logging.info(f"Successfully sent {len(points)} Traceroute hop data points to InfluxDB for {target_host}")
        except Exception as e:
            logging.error(f"Failed to write Traceroute data to InfluxDB for {target_host}: {e}")


def main():
    # Optional Kafka Producer Initialization (if moved from global)
    # ...

    try:
        while True:
            for target in TARGET_HOSTS:
                logging.info(f"Processing target: {target}")
                
                # MTR Execution (current logic)
                mtr_report = run_mtr(target, MTR_CYCLES, MTR_MAX_HOPS)
                if mtr_report:
                    process_mtr_data_to_influxdb(mtr_report, target)
                    # Optional: Send MTR to Kafka
                    # ...
                
                # Traceroute Execution
                if TRACEROUTE_ENABLE:
                    traceroute_result_hops = run_traceroute(target, TRACEROUTE_MAX_HOPS)
                    if traceroute_result_hops: # If list is not empty
                        process_traceroute_data_to_influxdb(traceroute_result_hops, target)
                        # Optional: Send Traceroute to Kafka
                        # if kafka_producer:
                        #     try:
                        #         # Define KAFKA_TOPIC_TRACEROUTE if using
                        #         # kafka_producer.send(KAFKA_TOPIC_TRACEROUTE, {'target': target, 'hops': traceroute_result_hops})
                        #         # logging.debug(f"Traceroute data for {target} sent to Kafka topic {KAFKA_TOPIC_TRACEROUTE}")
                        #     except Exception as e:
                        #         logging.error(f"Failed to send traceroute data to Kafka for {target}: {e}")
                
                # Small delay between targets if many are configured
                time.sleep(1) 
            
            logging.info(f"Completed a cycle of checks. Waiting for {TEST_INTERVAL_SECONDS} seconds.")
            time.sleep(TEST_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logging.info("Traceroute/MTR Canary stopped by user.")
    finally:
        if influx_client:
            influx_client.close()
            logging.info("InfluxDB client closed.")
        # if kafka_producer:
        #     kafka_producer.close()
        #     logging.info("Kafka producer closed.")
        logging.info("Traceroute/MTR Canary shutting down.")


if __name__ == "__main__":
    main()
