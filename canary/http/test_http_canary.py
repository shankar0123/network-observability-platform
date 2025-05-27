import unittest
from unittest.mock import patch, MagicMock, ANY
import time # For general time usage if any
from datetime import datetime, timedelta, timezone # For Point construction

# Assuming tests are run where canary.http.http_canary is importable
from canary.http import http_canary
import requests # For exception types (requests.Response is used for spec)
from influxdb_client import Point # For constructing expected points if needed

# Helper for datetime control
FROZEN_DATETIME_NOW = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

class TestHTTPCanaryInfluxDB(unittest.TestCase):

    def setUp(self):
        # Patch module-level configurations used by check_url and for InfluxDB points
        self.patcher_bucket = patch.object(http_canary, 'INFLUXDB_BUCKET', "test-http-bucket")
        self.patcher_canary_id = patch.object(http_canary, 'CANARY_ID', "test-http-canary")
        self.patcher_producer = patch.object(http_canary, 'producer', MagicMock()) # Mock Kafka
        self.patcher_user_agent = patch.object(http_canary, 'USER_AGENT', "test-agent")
        self.patcher_expected_string = patch.object(http_canary, 'EXPECTED_STRING', "") # Default no content check

        self.mock_influx_bucket = self.patcher_bucket.start()
        self.mock_canary_id = self.patcher_canary_id.start()
        self.mock_kafka_producer = self.patcher_producer.start()
        self.mock_user_agent = self.patcher_user_agent.start()
        self.mock_expected_string = self.patcher_expected_string.start()

        # Mock datetime.now specifically for InfluxDB points within the http_canary module
        self.datetime_patcher = patch.object(http_canary, 'datetime', MagicMock())
        self.mock_datetime = self.datetime_patcher.start()
        self.mock_datetime.now.return_value = FROZEN_DATETIME_NOW
        # Also patch timezone if it's used directly, though now(timezone.utc) covers it.
        # self.mock_datetime.timezone = timezone 


    def tearDown(self):
        self.patcher_bucket.stop()
        self.patcher_canary_id.stop()
        self.patcher_producer.stop()
        self.patcher_user_agent.stop()
        self.patcher_expected_string.stop()
        self.datetime_patcher.stop()

    @patch('time.monotonic')
    @patch('canary.http.http_canary.requests.get')
    def test_check_url_success_influxdb(self, mock_requests_get, mock_monotonic):
        # --- Arrange ---
        mock_write_api = MagicMock()
        
        mock_monotonic.side_effect = [1000.0, 1000.5] # 0.5s duration

        mock_response = MagicMock(spec=requests.Response)
        mock_response.ok = True # Indicates status_code < 400
        mock_response.status_code = 200
        mock_response.text = "OK"
        mock_response.content = b"OK" 
        mock_response.headers = {'Content-Type': 'text/plain'}
        # http_canary.py calculates latency using time.monotonic, not response.elapsed
        mock_requests_get.return_value = mock_response

        target_url = "http://example.com"
        method = "GET" # check_url defaults to GET
        
        # --- Act ---
        # check_url(url, method, timeout, expected_string, user_agent, write_api)
        # EXPECTED_STRING is patched to "" (no content check by default)
        http_canary.check_url(target_url, method=method, timeout=10, 
                              expected_string=self.mock_expected_string, # using patched global
                              user_agent=self.mock_user_agent, 
                              write_api=mock_write_api)

        # --- Assert ---
        self.assertTrue(mock_write_api.write.called)
        args, kwargs = mock_write_api.write.call_args
        self.assertEqual(kwargs['bucket'], "test-http-bucket")

        written_points = kwargs['record']
        # SSL expiry is not implemented, so ssl_expiry_days_left is None, only 1 point expected.
        self.assertEqual(len(written_points), 1) 
        
        status_point = written_points[0]
        self.assertEqual(status_point._name, "http_canary_status")
        self.assertEqual(status_point._tags['target_url'], target_url)
        self.assertEqual(status_point._tags['method'], method)
        self.assertEqual(status_point._tags['canary_id'], "test-http-canary")
        
        self.assertAlmostEqual(status_point._fields['duration_seconds'], 0.5)
        self.assertEqual(status_point._fields['success'], 1) 
        self.assertEqual(status_point._fields['status_code'], 200)
        self.assertEqual(status_point._fields['response_size_bytes'], 2)
        # Since EXPECTED_STRING is "", content_match remains None, and content_match_status field is not added.
        self.assertNotIn('content_match_status', status_point._fields)
        self.assertEqual(status_point._time, FROZEN_DATETIME_NOW)
        self.assertNotIn('error_type', status_point._tags)


    @patch('time.monotonic')
    @patch('canary.http.http_canary.requests.get')
    def test_check_url_success_with_content_match_influxdb(self, mock_requests_get, mock_monotonic):
        # --- Arrange ---
        mock_write_api = MagicMock()
        http_canary.EXPECTED_STRING = "OK" # Override default patch for this test

        mock_monotonic.side_effect = [1000.0, 1000.5]
        mock_response = MagicMock(spec=requests.Response)
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.text = "Everything is OK"
        mock_response.content = b"Everything is OK"
        mock_requests_get.return_value = mock_response
        target_url = "http://example.com"
        
        # --- Act ---
        http_canary.check_url(target_url, write_api=mock_write_api) # Using patched EXPECTED_STRING

        # --- Assert ---
        status_point = mock_write_api.write.call_args.kwargs['record'][0]
        self.assertEqual(status_point._fields['success'], 1)
        self.assertEqual(status_point._fields['content_match_status'], 1) # content_match is True -> 1
        self.assertNotIn('error_type', status_point._tags)


    @patch('time.monotonic')
    @patch('canary.http.http_canary.requests.get')
    def test_check_url_failure_content_mismatch_influxdb(self, mock_requests_get, mock_monotonic):
        # --- Arrange ---
        mock_write_api = MagicMock()
        http_canary.EXPECTED_STRING = "RequiredText" # Override

        mock_monotonic.side_effect = [1000.0, 1000.5]
        mock_response = MagicMock(spec=requests.Response)
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.text = "Different text"
        mock_response.content = b"Different text"
        mock_requests_get.return_value = mock_response
        target_url = "http://example.com/content_fail"
        
        # --- Act ---
        http_canary.check_url(target_url, write_api=mock_write_api)

        # --- Assert ---
        status_point = mock_write_api.write.call_args.kwargs['record'][0]
        self.assertEqual(status_point._fields['success'], 0) # Overall success is 0 due to content mismatch
        self.assertEqual(status_point._fields['content_match_status'], 0) # content_match is False -> 0
        self.assertNotIn('error_type', status_point._tags) # Not a request error, but a check failure


    @patch('time.monotonic')
    @patch('canary.http.http_canary.requests.get')
    def test_check_url_failure_404_influxdb(self, mock_requests_get, mock_monotonic):
        # --- Arrange ---
        mock_write_api = MagicMock()
        mock_monotonic.side_effect = [2000.0, 2000.1] # 0.1s duration

        mock_response = MagicMock(spec=requests.Response)
        mock_response.ok = False
        mock_response.status_code = 404
        mock_response.reason = "Not Found"
        mock_response.text = "Not Found"
        mock_response.content = b"Not Found"
        mock_requests_get.return_value = mock_response

        target_url = "http://example.com/notfound"
        
        # --- Act ---
        http_canary.check_url(target_url, write_api=mock_write_api) 

        # --- Assert ---
        self.assertTrue(mock_write_api.write.called)
        status_point = mock_write_api.write.call_args.kwargs['record'][0]
        
        self.assertEqual(status_point._name, "http_canary_status")
        self.assertEqual(status_point._fields['success'], 0) 
        self.assertEqual(status_point._fields['status_code'], 404)
        self.assertAlmostEqual(status_point._fields['duration_seconds'], 0.1)
        self.assertNotIn('content_match_status', status_point._fields) # No check if not success
        self.assertNotIn('error_type', status_point._tags) # HTTP 404 is not a "request exception" error_type, it's a status code


    @patch('time.monotonic')
    @patch('canary.http.http_canary.requests.get')
    def test_check_url_requests_timeout_influxdb(self, mock_requests_get, mock_monotonic):
        # --- Arrange ---
        mock_write_api = MagicMock()
        # time.monotonic is called before requests.get, and then after (if it doesn't timeout).
        # If it times out, the second monotonic call might not happen in the same way or its result is used differently.
        # In http_canary.py, for Timeout, latency_ms is calculated using monotonic calls around the exception block.
        mock_monotonic.side_effect = [3000.0, 3005.0] # Simulate 5s until timeout caught

        mock_requests_get.side_effect = requests.exceptions.Timeout("Test Timeout")

        target_url = "http://example.com/timeout"
        
        # --- Act ---
        http_canary.check_url(target_url, timeout=5, write_api=mock_write_api)

        # --- Assert ---
        self.assertTrue(mock_write_api.write.called)
        status_point = mock_write_api.write.call_args.kwargs['record'][0]
        
        self.assertEqual(status_point._name, "http_canary_status")
        self.assertEqual(status_point._fields['success'], 0)
        self.assertAlmostEqual(status_point._fields['duration_seconds'], 5.0) # Latency recorded up to timeout
        self.assertNotIn('status_code', status_point._fields) # No status code on timeout
        self.assertEqual(status_point._tags['error_type'], 'TIMEOUT')


    @patch('time.monotonic')
    @patch('canary.http.http_canary.requests.get')
    def test_check_url_requests_ssl_error_influxdb(self, mock_requests_get, mock_monotonic):
        # --- Arrange ---
        mock_write_api = MagicMock()
        mock_monotonic.side_effect = [4000.0, 4000.2] 

        mock_requests_get.side_effect = requests.exceptions.SSLError("Test SSL Error")
        target_url = "https://expired.example.com"
        
        # --- Act ---
        http_canary.check_url(target_url, write_api=mock_write_api)

        # --- Assert ---
        self.assertTrue(mock_write_api.write.called)
        status_point = mock_write_api.write.call_args.kwargs['record'][0]
        self.assertEqual(status_point._name, "http_canary_status")
        self.assertEqual(status_point._fields['success'], 0)
        self.assertAlmostEqual(status_point._fields['duration_seconds'], 0.2)
        self.assertNotIn('status_code', status_point._fields)
        self.assertEqual(status_point._tags['error_type'], 'SSL_ERROR')


    @patch('time.monotonic')
    @patch('canary.http.http_canary.requests.get')
    def test_check_url_influx_disabled(self, mock_requests_get, mock_monotonic):
        # --- Arrange ---
        mock_write_api = None # InfluxDB disabled
        mock_monotonic.side_effect = [5000.0, 5000.1]
        
        mock_response = MagicMock(spec=requests.Response)
        mock_response.ok = True
        mock_response.status_code = 200
        mock_response.text = "OK"
        mock_response.content = b"OK"
        mock_requests_get.return_value = mock_response
        
        target_url = "http://example.com"
        
        # --- Act ---
        # This call should not raise an error even with write_api=None
        http_canary.check_url(target_url, write_api=mock_write_api)

        # --- Assert ---
        # Test passes if no AttributeError from trying to call a method on None write_api
        # (and that mock_write_api was not called, but it's None, so can't check .called)
        pass


if __name__ == '__main__':
    unittest.main()
