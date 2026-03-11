import unittest

from generators.schema import EventSources, REQUIRED_FIELDS, has_required_fields
from generators.synthetic_payments import generate_synthetic_payments


class TestAll(unittest.TestCase):
    def test_event_sources_schema(self):
        """Schema defines and exports all required fields."""
        required_fields = {"user_id", "amount", "timestamp", "device_id", "location"}
        self.assertEqual(set(REQUIRED_FIELDS), required_fields)
        self.assertTrue(all(field in EventSources.__annotations__ for field in required_fields))

    def test_has_required_fields(self):
        payload = {
            "user_id": "user_001",
            "amount": 12.34,
            "timestamp": "2026-01-01T00:00:00+00:00",
            "device_id": "ios_phone",
            "location": "NYC",
        }
        self.assertTrue(has_required_fields(payload))
        payload.pop("location")
        self.assertFalse(has_required_fields(payload))

    def test_synthetic_generator_conforms_to_schema(self):
        events = list(generate_synthetic_payments(25, seed=7))
        self.assertEqual(len(events), 25)

        for event in events:
            self.assertTrue(all(key in event for key in REQUIRED_FIELDS))
            self.assertIsInstance(event["user_id"], str)
            self.assertIsInstance(event["amount"], float)
            self.assertIsInstance(event["timestamp"], str)
            self.assertIsInstance(event["device_id"], str)
            self.assertTrue(isinstance(event["location"], str) or isinstance(event["location"], dict))
