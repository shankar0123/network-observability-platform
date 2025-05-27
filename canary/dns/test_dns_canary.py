import unittest
from unittest.mock import patch, MagicMock, call
import time # Keep for general time usage if any, but monotonic is key for duration
from datetime import datetime, timezone # For Point construction and checking _time attribute

# Assuming tests are run in an environment where canary.dns.dns_canary is importable
from canary.dns import dns_canary
import dns.resolver # For dns.resolver.NXDOMAIN, dns.resolver.Timeout, etc.
from influxdb_client import Point, WritePrecision # Useful for constructing expected points if needed

class TestDNSCanaryInfluxDB(unittest.TestCase):

    def setUp(self):
        # Set InfluxDB bucket and Canary ID as they are used by perform_dns_query
        # These are module-level in dns_canary, so we patch them there.
        self.patcher_bucket = patch.object(dns_canary, 'INFLUXDB_BUCKET', "test-bucket")
        self.patcher_canary_id = patch.object(dns_canary, 'CANARY_ID', "test-canary-id")
        self.patcher_producer = patch.object(dns_canary, 'producer', MagicMock())

        self.mock_influx_bucket = self.patcher_bucket.start()
        self.mock_canary_id = self.patcher_canary_id.start()
        self.mock_kafka_producer = self.patcher_producer.start()
        
        # Ensure TARGET_RESOLVER is set as it's used in tags (target_server)
        self.patcher_target_resolver = patch.object(dns_canary, 'TARGET_RESOLVER', '8.8.8.8')
        self.mock_target_resolver = self.patcher_target_resolver.start()


    def tearDown(self):
        self.patcher_bucket.stop()
        self.patcher_canary_id.stop()
        self.patcher_producer.stop()
        self.patcher_target_resolver.stop()

    @patch.object(dns_canary, 'datetime', MagicMock()) # Mock datetime within dns_canary module
    @patch('time.monotonic')
    @patch('dns.resolver.Resolver') 
    def test_perform_dns_query_success(self, mock_dns_resolver_constructor, mock_monotonic, mock_datetime_dns_canary):
        # --- Arrange ---
        mock_write_api = MagicMock()
        
        # Mock time.monotonic for duration calculation
        mock_monotonic.side_effect = [1000.0, 1001.5] # Start time, End time (1.5s duration)
        
        # Mock datetime.now for InfluxDB point timestamping
        fixed_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_datetime_dns_canary.now.return_value = fixed_now

        mock_resolver_instance = mock_dns_resolver_constructor.return_value
        
        # dns.resolver.resolve returns an Answer object which is iterable
        # and has an 'rrset' attribute which can be converted to text (used by str(rdata))
        mock_rdata = MagicMock() 
        mock_rdata.to_text.return_value = "1.2.3.4" # This is how it's used in dns_canary
        # The actual answer object is iterable, yielding rdata objects
        mock_answer = [mock_rdata] # Make it a list to be iterable
        
        mock_resolver_instance.resolve.return_value = mock_answer # Changed from query to resolve
        mock_resolver_instance.timeout = 2.0 
        mock_resolver_instance.lifetime = 2.0 

        # TARGET_RESOLVER is used for the 'target_server' tag. It's patched in setUp.
        # server_ip = "8.8.8.8" # This is passed as resolver_ip argument
        domain = "example.com"
        query_type = "A"

        # --- Act ---
        # The resolver_ip argument to perform_dns_query is 'TARGET_RESOLVER' from the module,
        # or if it's empty, 'System Default'. We've patched TARGET_RESOLVER to '8.8.8.8'.
        result = dns_canary.perform_dns_query(domain, query_type, dns_canary.TARGET_RESOLVER, 5, mock_write_api)


        # --- Assert ---
        self.assertTrue(mock_write_api.write.called)
        
        args, kwargs = mock_write_api.write.call_args
        self.assertEqual(kwargs['bucket'], "test-bucket")
        
        written_points = kwargs['record']
        self.assertEqual(len(written_points), 2) 

        # Check for dns_query_time_seconds point
        time_point = next((p for p in written_points if p._name == "dns_query_time_seconds"), None)
        self.assertIsNotNone(time_point, "dns_query_time_seconds point not found")
        self.assertEqual(time_point._tags['target_server'], dns_canary.TARGET_RESOLVER)
        self.assertEqual(time_point._tags['domain'], domain)
        self.assertEqual(time_point._tags['query_type'], query_type)
        self.assertEqual(time_point._tags['canary_id'], "test-canary-id")
        # latency_ms is (end_time - start_time) * 1000
        # (1001.5 - 1000.0) * 1000 = 1.5 * 1000 = 1500.0 ms
        # The field in Influx is 'duration' in seconds.
        self.assertAlmostEqual(time_point._fields['duration'], 1.5, places=1) 
        self.assertEqual(time_point._time, fixed_now)


        # Check for dns_query_success point
        success_point = next((p for p in written_points if p._name == "dns_query_success"), None)
        self.assertIsNotNone(success_point, "dns_query_success point not found")
        self.assertEqual(success_point._tags['target_server'], dns_canary.TARGET_RESOLVER)
        self.assertEqual(success_point._tags['domain'], domain)
        self.assertEqual(success_point._tags['query_type'], query_type)
        self.assertEqual(success_point._tags['canary_id'], "test-canary-id")
        self.assertEqual(success_point._fields['status'], 1) # Success is 1
        self.assertNotIn('error_type', success_point._tags)
        self.assertEqual(success_point._time, fixed_now)

    @patch.object(dns_canary, 'datetime', MagicMock())
    @patch('time.monotonic')
    @patch('dns.resolver.Resolver')
    def test_perform_dns_query_failure_nxdomain(self, mock_dns_resolver_constructor, mock_monotonic, mock_datetime_dns_canary):
        # --- Arrange ---
        mock_write_api = MagicMock()
        mock_monotonic.side_effect = [1000.0, 1000.5] # 0.5s duration
        
        fixed_now = datetime(2023, 1, 1, 12, 0, 1, tzinfo=timezone.utc)
        mock_datetime_dns_canary.now.return_value = fixed_now

        mock_resolver_instance = mock_dns_resolver_constructor.return_value
        mock_resolver_instance.resolve.side_effect = dns.resolver.NXDOMAIN() # No message needed for type
        mock_resolver_instance.timeout = 1.0
        mock_resolver_instance.lifetime = 1.0

        domain = "nonexistent.example.com"
        query_type = "A"

        # --- Act ---
        dns_canary.perform_dns_query(domain, query_type, dns_canary.TARGET_RESOLVER, 1, mock_write_api)

        # --- Assert ---
        self.assertTrue(mock_write_api.write.called)
        args, kwargs = mock_write_api.write.call_args
        self.assertEqual(kwargs['bucket'], "test-bucket")
        
        written_points = kwargs['record']
        self.assertEqual(len(written_points), 2) # Time point and failure point

        success_point = next((p for p in written_points if p._name == "dns_query_success"), None)
        self.assertIsNotNone(success_point)
        self.assertEqual(success_point._fields['status'], 0) # Failure is 0
        self.assertEqual(success_point._tags['error_type'], 'NXDOMAIN')
        self.assertEqual(success_point._tags['target_server'], dns_canary.TARGET_RESOLVER)
        self.assertEqual(success_point._tags['domain'], domain)
        self.assertEqual(success_point._tags['query_type'], query_type)
        self.assertEqual(success_point._tags['canary_id'], "test-canary-id")
        self.assertEqual(success_point._time, fixed_now)

        time_point = next((p for p in written_points if p._name == "dns_query_time_seconds"), None)
        self.assertIsNotNone(time_point) 
        self.assertAlmostEqual(time_point._fields['duration'], 0.5, places=1)
        self.assertEqual(time_point._time, fixed_now)

    @patch.object(dns_canary, 'datetime', MagicMock())
    @patch('time.monotonic') # time.monotonic is NOT called by dns.resolver.Timeout directly
    @patch('dns.resolver.Resolver')
    def test_perform_dns_query_failure_timeout(self, mock_dns_resolver_constructor, mock_monotonic, mock_datetime_dns_canary):
        # --- Arrange ---
        mock_write_api = MagicMock()
        # For timeout, time.monotonic is not used to calculate duration inside the try block's success path
        # Latency is not recorded in the same way for Timeout.
        # The dns_canary.py code for Timeout does not calculate latency_ms.
        
        fixed_now = datetime(2023, 1, 1, 12, 0, 2, tzinfo=timezone.utc)
        mock_datetime_dns_canary.now.return_value = fixed_now

        mock_resolver_instance = mock_dns_resolver_constructor.return_value
        mock_resolver_instance.resolve.side_effect = dns.resolver.Timeout()
        mock_resolver_instance.timeout = 0.5 
        mock_resolver_instance.lifetime = 0.5 

        domain = "example.com"
        query_type = "A"

        # --- Act ---
        dns_canary.perform_dns_query(domain, query_type, dns_canary.TARGET_RESOLVER, 0.5, mock_write_api)

        # --- Assert ---
        self.assertTrue(mock_write_api.write.called)
        args, kwargs = mock_write_api.write.call_args
        written_points = kwargs['record']
        # For Timeout, only one point (dns_query_success with status 0) is written
        # because latency_ms is None, so dns_query_time_seconds is not written.
        self.assertEqual(len(written_points), 1) 

        success_point = next((p for p in written_points if p._name == "dns_query_success"), None)
        self.assertIsNotNone(success_point)
        self.assertEqual(success_point._fields['status'], 0)
        self.assertEqual(success_point._tags['error_type'], 'TIMEOUT')
        self.assertEqual(success_point._tags['target_server'], dns_canary.TARGET_RESOLVER)
        self.assertEqual(success_point._tags['domain'], domain)
        self.assertEqual(success_point._tags['query_type'], query_type)
        self.assertEqual(success_point._tags['canary_id'], "test-canary-id")
        self.assertEqual(success_point._time, fixed_now)
        
        time_point = next((p for p in written_points if p._name == "dns_query_time_seconds"), None)
        self.assertIsNone(time_point) # No time point for timeout as per current dns_canary.py logic


    @patch.object(dns_canary, 'datetime', MagicMock())
    @patch('time.monotonic') 
    @patch('dns.resolver.Resolver') 
    def test_perform_dns_query_influx_disabled(self, mock_dns_resolver_constructor, mock_monotonic, mock_datetime_dns_canary):
        # --- Arrange ---
        mock_write_api = None # Simulate InfluxDB not configured
        
        mock_monotonic.side_effect = [1000.0, 1001.0] 
        fixed_now = datetime(2023, 1, 1, 12, 0, 3, tzinfo=timezone.utc)
        mock_datetime_dns_canary.now.return_value = fixed_now


        mock_resolver_instance = mock_dns_resolver_constructor.return_value
        mock_rdata = MagicMock()
        mock_rdata.to_text.return_value = "1.2.3.4"
        mock_answer = [mock_rdata]
        mock_resolver_instance.resolve.return_value = mock_answer
        mock_resolver_instance.timeout = 2.0
        mock_resolver_instance.lifetime = 2.0

        domain = "example.com"
        query_type = "A"

        # --- Act ---
        # This call should not raise an error.
        dns_canary.perform_dns_query(domain, query_type, dns_canary.TARGET_RESOLVER, 5, mock_write_api)

        # --- Assert ---
        # No specific assertion that write wasn't called on None, as that would be an AttributeError
        # if the code tried. Test passes if no exception is raised.
        # And the mock_write_api (which is None) should not have a 'write' attribute.
        pass

if __name__ == '__main__':
    unittest.main()
