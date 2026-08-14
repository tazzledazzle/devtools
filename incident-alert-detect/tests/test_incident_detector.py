import pytest
from incident_detector import IncidentDetector


def test_no_events_no_incident():
    detector = IncidentDetector(error_threshold=3)
    assert detector.service_states == {}

def test_incident_triggers_exactly_at_threshold():
    detector = IncidentDetector(error_threshold=3)
    result = []
    for _ in range(3):
        result.append(detector.process_event(1, "api", "ERROR"))
    assert result == [None, None, "INCIDENT_START"]