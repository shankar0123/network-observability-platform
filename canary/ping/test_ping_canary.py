import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timezone # For FROZEN_DATETIME_NOW

# Assuming tests are run where canary.ping.ping_canary is importable
from canary.ping import ping_canary
import subprocess # For subprocess.CompletedProcess

# Helper for datetime control
FROZEN_DATETIME_NOW = datetime(2023, 10, 26, 10, 0, 0, tzinfo=timezone.utc)

class TestPingCanaryInfluxDB(unittest.TestCase):

    def setUp(self):
        # Patch module-level configurations used by perform_ping and for InfluxDB points
        self.patcher_bucket = patch.object(ping_canary, 'INFLUXDB_BUCKET', "test-ping-bucket")
        self.patcher_canary_id = patch.object(ping_canary, 'CANARY_ID', "test-ping-canary")
        self.patcher_producer = patch.object(ping_canary, 'producer', MagicMock()) # Mock Kafka
        self.patcher_ping_count = patch.object(ping_canary, 'PING_COUNT', 5) # Default PING_COUNT for tests

        self.mock_influx_bucket = self.patcher_bucket.start()
        self.mock_canary_id = self.patcher_canary_id.start()
        self.mock_kafka_producer = self.patcher_producer.start()
        self.mock_ping_count = self.patcher_ping_count.start()
        
        # Patch datetime.now used for InfluxDB points
        # ping_canary.py uses: from datetime import datetime, timezone; datetime.now(timezone.utc)
        self.datetime_patcher = patch.object(ping_canary, 'datetime', MagicMock())
        self.mock_datetime = self.datetime_patcher.start()
        self.mock_datetime.now.return_value = FROZEN_DATETIME_NOW
        # Ensure the timezone attribute is also available if needed directly
        self.mock_datetime.timezone = timezone 


    def tearDown(self):
        self.patcher_bucket.stop()
        self.patcher_canary_id.stop()
        self.patcher_producer.stop()
        self.patcher_ping_count.stop()
        self.datetime_patcher.stop()

    @patch('canary.ping.ping_canary.subprocess.run')
    def test_perform_ping_success_influxdb(self, mock_subprocess_run):
        # --- Arrange ---
        mock_write_api = MagicMock()
        
        # Simulate successful fping output (matches the regex in ping_canary.py)
        # Example: google.com : xmt/rcv/%loss = 5/5/0%, min/avg/max = 10.7/11.0/11.6
        fping_output_stderr = "google.com : xmt/rcv/%loss = 5/5/0%, min/avg/max = 10.0/11.5/12.0"
        
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=['fping', '-q', '-C', str(self.mock_ping_count), '-p', '20', '-t', '500', 'google.com'], 
            returncode=0, 
            stderr=fping_output_stderr 
        )
        
        target_host = "google.com"

        # --- Act ---
        # perform_ping(target_host, write_api=None)
        ping_canary.perform_ping(target_host, mock_write_api)

        # --- Assert ---
        self.assertTrue(mock_write_api.write.called)
        args, kwargs = mock_write_api.write.call_args
        self.assertEqual(kwargs['bucket'], "test-ping-bucket")

        point = kwargs['record'] # In ping_canary.py, 'record' is a single Point object
        
        self.assertEqual(point._name, "ping_canary_stats")
        self.assertEqual(point._tags['target_host'], target_host)
        self.assertEqual(point._tags['canary_id'], "test-ping-canary")
        
        self.assertEqual(point._fields['success'], 1)
        self.assertEqual(point._fields['packets_sent'], 5)
        self.assertEqual(point._fields['packets_received'], 5)
        self.assertEqual(point._fields['packet_loss_percent'], 0.0)
        self.assertAlmostEqual(point._fields['rtt_min_ms'], 10.0)
        self.assertAlmostEqual(point._fields['rtt_avg_ms'], 11.5)
        self.assertAlmostEqual(point._fields['rtt_max_ms'], 12.0)
        self.assertNotIn('error_details', point._tags) # No error for successful ping
        self.assertEqual(point._time, FROZEN_DATETIME_NOW)

    @patch('canary.ping.ping_canary.subprocess.run')
    def test_perform_ping_total_loss_influxdb(self, mock_subprocess_run):
        # --- Arrange ---
        mock_write_api = MagicMock()
        # Example: 10.0.0.1 : xmt/rcv/%loss = 5/0/100%
        fping_output_stderr = "nonexistent.dummy : xmt/rcv/%loss = 5/0/100%"
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=['fping', '-q', '-C', '5', '-p', '20', '-t', '500', 'nonexistent.dummy'], 
            returncode=0, # fping can return 0 even if all lost (host valid, but unresponsive)
                          # ping_canary.py returncode check is `> 1` for fping command error.
            stderr=fping_output_stderr
        )
        target_host = "nonexistent.dummy"

        # --- Act ---
        ping_canary.perform_ping(target_host, mock_write_api)

        # --- Assert ---
        self.assertTrue(mock_write_api.write.called)
        point = mock_write_api.write.call_args.kwargs['record']

        self.assertEqual(point._name, "ping_canary_stats")
        self.assertEqual(point._tags['target_host'], target_host)
        self.assertEqual(point._fields['success'], 0) 
        self.assertEqual(point._fields['packets_sent'], 5)
        self.assertEqual(point._fields['packets_received'], 0)
        self.assertEqual(point._fields['packet_loss_percent'], 100.0)
        
        # RTT fields should not be present for 100% loss as per ping_canary.py logic
        self.assertNotIn('rtt_min_ms', point._fields)
        self.assertNotIn('rtt_avg_ms', point._fields)
        self.assertNotIn('rtt_max_ms', point._fields)
        # error_details tag is not added for 100% packet loss (it's a ping failure, not an execution error)
        self.assertNotIn('error_details', point._tags) 
        self.assertEqual(point._time, FROZEN_DATETIME_NOW)


    @patch('canary.ping.ping_canary.subprocess.run')
    def test_perform_ping_fping_command_error_influxdb(self, mock_subprocess_run):
        # --- Arrange ---
        mock_write_api = MagicMock()
        # Simulate fping command error (e.g. return code 2 or higher)
        fping_error_stderr = "fping: some fping execution error"
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=['fping', '-q', '-C', '5', '-p', '20', '-t', '500', 'badtarget'], 
            returncode=2, 
            stderr=fping_error_stderr
        )
        target_host = "badtarget"

        # --- Act ---
        ping_canary.perform_ping(target_host, mock_write_api)

        # --- Assert ---
        self.assertTrue(mock_write_api.write.called)
        point = mock_write_api.write.call_args.kwargs['record']
        
        self.assertEqual(point._name, "ping_canary_stats")
        self.assertEqual(point._tags['target_host'], target_host)
        self.assertEqual(point._fields['success'], 0)
        
        # For fping command errors, packet stats might be None or default to loss
        # Based on ping_canary.py, these fields are populated from regex match.
        # If match fails (likely on fping error), these fields might be None or default.
        # The current ping_canary.py initializes them and they might stay None if no match.
        self.assertIsNone(point._fields.get('packets_sent'))
        self.assertIsNone(point._fields.get('packets_received'))
        self.assertIsNone(point._fields.get('packet_loss_percent'))

        self.assertNotIn('rtt_min_ms', point._fields)
        self.assertNotIn('rtt_avg_ms', point._fields)
        self.assertNotIn('rtt_max_ms', point._fields)
        
        self.assertIn('error_details', point._tags)
        self.assertTrue("fping command error" in point._tags['error_details'])
        self.assertEqual(point._time, FROZEN_DATETIME_NOW)


    @patch('canary.ping.ping_canary.subprocess.run')
    def test_perform_ping_fping_parsing_error_influxdb(self, mock_subprocess_run):
        # --- Arrange ---
        mock_write_api = MagicMock()
        # Simulate output that doesn't match the regex
        fping_unparseable_stderr = "some unexpected output from fping"
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=['fping', '-q', '-C', '5', '-p', '20', '-t', '500', 'weirdtarget'], 
            returncode=0, # fping might succeed but output is weird
            stderr=fping_unparseable_stderr
        )
        target_host = "weirdtarget"

        # --- Act ---
        ping_canary.perform_ping(target_host, mock_write_api)

        # --- Assert ---
        self.assertTrue(mock_write_api.write.called)
        point = mock_write_api.write.call_args.kwargs['record']
        
        self.assertEqual(point._name, "ping_canary_stats")
        self.assertEqual(point._tags['target_host'], target_host)
        self.assertEqual(point._fields['success'], 0)

        self.assertIsNone(point._fields.get('packets_sent'))
        self.assertIsNone(point._fields.get('packets_received'))
        self.assertIsNone(point._fields.get('packet_loss_percent'))
        
        self.assertIn('error_details', point._tags)
        self.assertTrue("Could not parse fping output" in point._tags['error_details'])
        self.assertEqual(point._time, FROZEN_DATETIME_NOW)


    @patch('canary.ping.ping_canary.subprocess.run')
    def test_perform_ping_influx_disabled(self, mock_subprocess_run):
        # --- Arrange ---
        mock_write_api = None # InfluxDB disabled
        
        fping_output_stderr = "google.com : xmt/rcv/%loss = 1/1/0%, min/avg/max = 10.0/10.0/10.0"
        mock_subprocess_run.return_value = subprocess.CompletedProcess(
            args=['fping', '-q', '-C', '1', '-p', '20', '-t', '500', 'google.com'], 
            returncode=0, stderr=fping_output_stderr
        )
        target_host = "google.com"
        
        # Temporarily patch PING_COUNT for this specific test if its default (5) is too high for the mock output
        with patch.object(ping_canary, 'PING_COUNT', 1):
            ping_canary.perform_ping(target_host, mock_write_api)

        # --- Assert ---
        # Test passes if no AttributeError from trying to call a method on None write_api
        pass

if __name__ == '__main__':
    unittest.main()
