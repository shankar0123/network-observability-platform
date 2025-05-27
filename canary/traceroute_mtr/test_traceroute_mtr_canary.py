import unittest
from unittest.mock import patch, MagicMock, ANY
import subprocess # For subprocess.CompletedProcess and subprocess.TimeoutExpired
import json
from datetime import datetime, timezone

# Assuming tests are run where canary.traceroute_mtr.traceroute_mtr_canary is importable
from canary.traceroute_mtr import traceroute_mtr_canary as উপলব্ধি # Using an alias for brevity

# Helper for datetime control
FROZEN_DATETIME_NOW = datetime.now(timezone.utc) # Should match how it's used in the canary

# Sample MTR JSON output structure (simplified for testing)
SAMPLE_MTR_JSON_OUTPUT = {
    "report": {
        "mtr": {"source": "test-source", "host": "example.com"},
        "hubs": [
            {"count": 1, "host": "192.168.1.1", "Loss%": 0.0, "Snt": 10, "Last": 1.0, "Avg": 1.1, "Best": 0.9, "Wrst": 1.5, "StDev": 0.2},
            {"count": 2, "host": "10.0.0.1", "Loss%": 10.0, "Snt": 10, "Last": 5.0, "Avg": 5.5, "Best": 4.0, "Wrst": 6.0, "StDev": 0.5},
            {"count": 3, "host": "example.com", "Loss%": 0.0, "Snt": 10, "Last": 10.0, "Avg": 10.1, "Best": 9.8, "Wrst": 10.5, "StDev": 0.3}
        ]
    }
}

EMPTY_MTR_JSON_OUTPUT = {"report": {"hubs": []}}

class TestMTRCanaryInfluxDB(unittest.TestCase):

    def setUp(self):
        # Patch module-level InfluxDB client and other globals if necessary
        self.mock_influx_write_api = MagicMock()
        উপলব্ধি.influx_write_api = self.mock_influx_write_api
        উপলব্ধি.INFLUXDB_BUCKET = "test-mtr-bucket"
        উপলব্ধি.CANARY_ID = "test-mtr-canary"

        # Mock datetime.now used for InfluxDB points
        # Assuming 'from datetime import datetime, timezone' and 'datetime.now(timezone.utc)'
        self.datetime_now_patcher = patch('canary.traceroute_mtr.traceroute_mtr_canary.datetime', MagicMock())
        mock_dt = self.datetime_now_patcher.start()
        mock_dt.now.return_value = FROZEN_DATETIME_NOW
        
        # Mock Kafka if its setup/send is called (even if placeholder)
        # উপলব্ধি.kafka_producer = MagicMock() 


    def tearDown(self):
        self.datetime_now_patcher.stop()
        # Reset any patched module globals if needed
        উপলব্ধি.influx_write_api = None 

    @patch('canary.traceroute_mtr.traceroute_mtr_canary.subprocess.run')
    def test_run_mtr_success(self, mock_subprocess_run):
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=json.dumps(SAMPLE_MTR_JSON_OUTPUT)
        )
        result = উপলব্ধি.run_mtr("example.com", 10, 30)
        self.assertEqual(result, SAMPLE_MTR_JSON_OUTPUT['report'])
        mock_subprocess_run.assert_called_once()

    @patch('canary.traceroute_mtr.traceroute_mtr_canary.subprocess.run')
    def test_run_mtr_command_fail_sends_influx_error(self, mock_subprocess_run):
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="MTR error"
        )
        result = উপলব্ধি.run_mtr("example.com", 10, 30)
        self.assertIsNone(result)
        # Check InfluxDB error metric was sent
        self.mock_influx_write_api.write.assert_called_once()
        written_point = self.mock_influx_write_api.write.call_args.kwargs['record']
        self.assertEqual(written_point._name, "mtr_execution_error")
        self.assertEqual(written_point._tags['target_host'], "example.com")
        self.assertEqual(written_point._fields['return_code'], 1)
        self.assertEqual(written_point._fields['error_message'], "MTR error")

    @patch('canary.traceroute_mtr.traceroute_mtr_canary.subprocess.run')
    def test_run_mtr_timeout_sends_influx_error(self, mock_subprocess_run):
        mock_subprocess_run.side_effect = subprocess.TimeoutExpired(cmd="mtr", timeout=60)
        result = উপলব্ধি.run_mtr("example.com", 10, 30)
        self.assertIsNone(result)
        # Check InfluxDB error metric
        self.mock_influx_write_api.write.assert_called_once()
        written_point = self.mock_influx_write_api.write.call_args.kwargs['record']
        self.assertEqual(written_point._name, "mtr_execution_error")
        self.assertEqual(written_point._fields['error_message'], "TimeoutExpired")


    @patch('canary.traceroute_mtr.traceroute_mtr_canary.subprocess.run')
    def test_run_mtr_json_decode_error(self, mock_subprocess_run):
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=[], returncode=0, stdout="invalid json"
        )
        result = উপলব্ধি.run_mtr("example.com", 10, 30)
        self.assertIsNone(result)
        # Optionally check if an InfluxDB error is sent for this too, if designed so.
        # Current canary code does not send specific Influx error for JSONDecodeError.

    def test_process_mtr_data_to_influxdb_success(self):
        উপলব্ধি.process_mtr_data_to_influxdb(SAMPLE_MTR_JSON_OUTPUT['report'], "example.com")
        
        self.mock_influx_write_api.write.assert_called_once_with(
            bucket="test-mtr-bucket", record=ANY
        )
        written_records = self.mock_influx_write_api.write.call_args.kwargs['record']
        self.assertEqual(len(written_records), len(SAMPLE_MTR_JSON_OUTPUT['report']['hubs']))
        
        first_hop_point = written_records[0]
        self.assertEqual(first_hop_point._name, "mtr_hop_stats")
        self.assertEqual(first_hop_point._tags['target_host'], "example.com")
        self.assertEqual(first_hop_point._tags['canary_id'], "test-mtr-canary")
        self.assertEqual(first_hop_point._tags['hop_index'], 1)
        self.assertEqual(first_hop_point._tags['hop_ip'], "192.168.1.1")
        self.assertAlmostEqual(first_hop_point._fields['loss_percent'], 0.0)
        self.assertEqual(first_hop_point._fields['packets_sent'], 10)
        self.assertAlmostEqual(first_hop_point._fields['avg_latency_ms'], 1.1)
        self.assertEqual(first_hop_point._time, FROZEN_DATETIME_NOW)
        # ... Add more assertions for other fields/hops if necessary

    def test_process_mtr_data_to_influxdb_no_api(self):
        # Store original mock to check it wasn't called
        original_mock_api_object = self.mock_influx_write_api
        
        # Set the canary's internal reference to None
        উপলব্ধি.influx_write_api = None 
        
        উপলব্ধি.process_mtr_data_to_influxdb(SAMPLE_MTR_JSON_OUTPUT['report'], "example.com")
        
        # Assert that the original mock object (which was replaced by None in the canary) was not called
        original_mock_api_object.write.assert_not_called()

    def test_process_mtr_data_to_influxdb_empty_data(self):
        উপলব্ধি.process_mtr_data_to_influxdb(EMPTY_MTR_JSON_OUTPUT['report'], "example.com")
        self.mock_influx_write_api.write.assert_not_called() # No points should be generated

    def test_process_mtr_data_to_influxdb_no_hubs(self):
        malformed_report = {"mtr": {"source": "test"}} # 'hubs' key is missing
        উপলব্ধি.process_mtr_data_to_influxdb(malformed_report, "example.com")
        self.mock_influx_write_api.write.assert_not_called()

# Sample Traceroute Output for testing parse_traceroute_line and run_traceroute
SAMPLE_TRACEROUTE_OUTPUT_SUCCESS = """
traceroute to google.com (142.250.180.78), 30 hops max, 60 byte packets
 1  192.168.1.1 (192.168.1.1)  1.500 ms  1.800 ms  2.100 ms
 2  router.example.com (10.0.0.1)  5.000 ms  *  5.500 ms
 3  172.16.0.1 (172.16.0.1)  10.000 ms  10.200 ms  10.500 ms
 4  * * *
 5  google-gw.example.net (142.250.180.78)  12.000 ms  12.500 ms  12.800 ms
"""

SAMPLE_TRACEROUTE_OUTPUT_TIMEOUT_ALL = """
traceroute to neverland.nonexistent (1.2.3.4), 30 hops max, 60 byte packets
 1  * * *
 2  * * *
 3  * * *
"""

# It's good practice to create a new test class for Traceroute tests
class TestTracerouteCanaryInfluxDB(unittest.TestCase):

    def setUp(self):
        # Patch module-level InfluxDB client and other globals
        self.mock_influx_write_api = MagicMock()
        উপলব্ধি.influx_write_api = self.mock_influx_write_api # Use the same alias 'উপলব্ধি'
        উপলব্ধি.INFLUXDB_BUCKET = "test-traceroute-bucket" # Use a distinct bucket for clarity if desired
        উপলব্ধি.CANARY_ID = "test-traceroute-canary"

        # Mock datetime.now used for InfluxDB points (ensure this path is correct for the module)
        self.datetime_now_patcher = patch('canary.traceroute_mtr.traceroute_mtr_canary.datetime', MagicMock())
        mock_dt = self.datetime_now_patcher.start()
        mock_dt.now.return_value = FROZEN_DATETIME_NOW # Use the same FROZEN_DATETIME_NOW
        
        # Mock Kafka if its setup/send is called
        # উপলব্ধি.kafka_producer = MagicMock()

    def tearDown(self):
        self.datetime_now_patcher.stop()
        উপলব্ধি.influx_write_api = None

    # --- Tests for parse_traceroute_line ---
    def test_parse_traceroute_line_full(self):
        line = " 1  192.168.1.1 (192.168.1.1)  1.500 ms  1.800 ms  2.100 ms"
        # Note: The traceroute_mtr_canary.py's parse_traceroute_line expects hop_counter_expected argument
        parsed = উপলব্ধি.parse_traceroute_line(line, 1)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed['hop'], 1)
        # The parser logic: hostname = line_match.group(2) or line_match.group(4)
        # if line_match.group(2) is "192.168.1.1", then ip_address becomes group(3) "192.168.1.1"
        # hostname becomes "192.168.1.1"
        # if the line was "1 192.168.1.1 1.500 ms...", then hostname would be group(4) "192.168.1.1"
        # The current test line " 1  192.168.1.1 (192.168.1.1) ..." results in hostname being "192.168.1.1" (group 2)
        # and ip_address also "192.168.1.1" (group 3)
        self.assertEqual(parsed['hostname'], "192.168.1.1") 
        self.assertEqual(parsed['ip_address'], "192.168.1.1")
        self.assertEqual(parsed['rtts_ms'], [1.5, 1.8, 2.1])
        self.assertAlmostEqual(parsed['loss_percent'], 0.0)

    def test_parse_traceroute_line_with_hostname(self):
        line = " 2  router.example.com (10.0.0.1)  5.000 ms  *  5.500 ms"
        parsed = উপলব্ধি.parse_traceroute_line(line, 2)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed['hop'], 2)
        self.assertEqual(parsed['hostname'], "router.example.com")
        self.assertEqual(parsed['ip_address'], "10.0.0.1")
        self.assertEqual(parsed['rtts_ms'], [5.0, None, 5.5])
        self.assertAlmostEqual(parsed['loss_percent'], (1/3)*100, places=1)
        
    def test_parse_traceroute_line_timeout_hop(self):
        line = " 4  * * *"
        parsed = উপলব্ধি.parse_traceroute_line(line, 4)
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed['hop'], 4)
        self.assertEqual(parsed['hostname'], "*")
        self.assertEqual(parsed['ip_address'], "*")
        self.assertEqual(parsed['rtts_ms'], [None, None, None])
        self.assertAlmostEqual(parsed['loss_percent'], 100.0)

    def test_parse_traceroute_line_malformed(self):
        line = "this is not a traceroute line"
        parsed = উপলব্ধি.parse_traceroute_line(line, 1)
        self.assertIsNone(parsed)

    # --- Tests for run_traceroute ---
    @patch('canary.traceroute_mtr.traceroute_mtr_canary.subprocess.run')
    def test_run_traceroute_success(self, mock_subprocess_run):
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=[], returncode=0, stdout=SAMPLE_TRACEROUTE_OUTPUT_SUCCESS
        )
        results = উপলব্ধি.run_traceroute("google.com", 5) # Max hops for test
        self.assertEqual(len(results), 5) # Expect 5 hops from sample
        self.assertEqual(results[0]['hop'], 1)
        self.assertEqual(results[0]['ip_address'], "192.168.1.1")
        self.assertEqual(results[1]['hostname'], "router.example.com")
        self.assertEqual(results[3]['hostname'], "*") # Hop 4 is ' * * * '
        self.assertEqual(results[4]['hostname'], "google-gw.example.net") # Check hostname for last hop
        self.assertEqual(results[4]['ip_address'], "142.250.180.78")


    @patch('canary.traceroute_mtr.traceroute_mtr_canary.subprocess.run')
    def test_run_traceroute_command_fail_sends_influx(self, mock_subprocess_run):
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=[], returncode=1, stdout="", stderr="Traceroute error"
        )
        results = উপলব্ধি.run_traceroute("error.com", 5)
        self.assertEqual(results, []) # Expect empty list on error
        # Check InfluxDB error metric (assuming it's named traceroute_execution_error)
        self.mock_influx_write_api.write.assert_called_once()
        written_point = self.mock_influx_write_api.write.call_args.kwargs['record']
        self.assertEqual(written_point._name, "traceroute_execution_error")
        self.assertEqual(written_point._tags['target_host'], "error.com")
        self.assertEqual(written_point._fields['return_code'], 1)
        self.assertEqual(written_point._fields['error_message'], "Traceroute error")

    @patch('canary.traceroute_mtr.traceroute_mtr_canary.subprocess.run')
    def test_run_traceroute_timeout_sends_influx(self, mock_subprocess_run):
        mock_subprocess_run.side_effect = subprocess.TimeoutExpired(cmd="traceroute", timeout=60)
        results = উপলব্ধি.run_traceroute("timeout.com", 5)
        self.assertEqual(results, [])
        self.mock_influx_write_api.write.assert_called_once()
        written_point = self.mock_influx_write_api.write.call_args.kwargs['record']
        self.assertEqual(written_point._name, "traceroute_execution_error")
        self.assertEqual(written_point._fields['error_message'], "TimeoutExpired")
        
    # --- Tests for process_traceroute_data_to_influxdb ---
    def test_process_traceroute_data_to_influxdb_success(self):
        # Create sample parsed data based on SAMPLE_TRACEROUTE_OUTPUT_SUCCESS
        # This would normally come from run_traceroute
        parsed_hops_sample = [
            {'hop': 1, 'hostname': '192.168.1.1', 'ip_address': '192.168.1.1', 'rtts_ms': [1.5, 1.8, 2.1], 'loss_percent': 0.0},
            {'hop': 2, 'hostname': 'router.example.com', 'ip_address': '10.0.0.1', 'rtts_ms': [5.0, None, 5.5], 'loss_percent': (1/3)*100},
            {'hop': 3, 'hostname': '172.16.0.1', 'ip_address': '172.16.0.1', 'rtts_ms': [10.0, 10.2, 10.5], 'loss_percent': 0.0},
            {'hop': 4, 'hostname': '*', 'ip_address': '*', 'rtts_ms': [None, None, None], 'loss_percent': 100.0},
            {'hop': 5, 'hostname': 'google-gw.example.net', 'ip_address': '142.250.180.78', 'rtts_ms': [12.0, 12.5, 12.8], 'loss_percent': 0.0}
        ]
        
        উপলব্ধি.process_traceroute_data_to_influxdb(parsed_hops_sample, "google.com")
        
        self.mock_influx_write_api.write.assert_called_once_with(
            bucket="test-traceroute-bucket", record=ANY
        )
        written_records = self.mock_influx_write_api.write.call_args.kwargs['record']
        self.assertEqual(len(written_records), len(parsed_hops_sample))

        first_hop_point = written_records[0]
        self.assertEqual(first_hop_point._name, "traceroute_hop_stats")
        self.assertEqual(first_hop_point._tags['target_host'], "google.com")
        self.assertEqual(first_hop_point._tags['canary_id'], "test-traceroute-canary")
        self.assertEqual(first_hop_point._tags['hop_index'], 1)
        self.assertEqual(first_hop_point._tags['hop_ip'], "192.168.1.1")
        self.assertAlmostEqual(first_hop_point._fields['loss_percent'], 0.0)
        self.assertAlmostEqual(first_hop_point._fields['rtt1_ms'], 1.5)
        self.assertAlmostEqual(first_hop_point._fields['rtt2_ms'], 1.8)
        self.assertAlmostEqual(first_hop_point._fields['rtt3_ms'], 2.1)
        self.assertEqual(first_hop_point._time, FROZEN_DATETIME_NOW)

        # Check a hop with a timeout ('*')
        second_hop_point = written_records[1]
        self.assertEqual(second_hop_point._tags['hop_ip'], "10.0.0.1")
        self.assertAlmostEqual(second_hop_point._fields['rtt1_ms'], 5.0)
        self.assertEqual(second_hop_point._fields['rtt2_ms'], -1.0) # -1.0 for None RTTs
        self.assertAlmostEqual(second_hop_point._fields['rtt3_ms'], 5.5)

        # Check a fully timed out hop
        fourth_hop_point = written_records[3]
        self.assertEqual(fourth_hop_point._tags['hop_ip'], "*")
        self.assertEqual(fourth_hop_point._fields['rtt1_ms'], -1.0)
        self.assertEqual(fourth_hop_point._fields['rtt2_ms'], -1.0)
        self.assertEqual(fourth_hop_point._fields['rtt3_ms'], -1.0)
        self.assertAlmostEqual(fourth_hop_point._fields['loss_percent'], 100.0)


    def test_process_traceroute_data_to_influxdb_no_api(self):
        # Store original mock to check it wasn't called
        original_mock_api_object = self.mock_influx_write_api
        
        উপলব্ধি.influx_write_api = None
        উপলব্ধি.process_traceroute_data_to_influxdb([], "google.com") # Empty list, but matters that api is None
        
        # Assert that the original mock object (which was replaced by None in the canary) was not called
        original_mock_api_object.write.assert_not_called()


    def test_process_traceroute_data_to_influxdb_empty_hops(self):
        # API is available (mocked in setUp)
        উপলব্ধি.process_traceroute_data_to_influxdb([], "google.com")
        self.mock_influx_write_api.write.assert_not_called() # Should not call if no points generated


if __name__ == '__main__':
    unittest.main()
