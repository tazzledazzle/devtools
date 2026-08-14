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

def test_no_retrigger_after_incident_active():
    detector = IncidentDetector(error_threshold=3)
    result = []
    for _ in range(5):
        result.append(detector.process_event(1, "api", "ERROR"))
    assert result == [None, None, "INCIDENT_START", None, None]


def test_success_events_dont_affect_error_threshold():
    detector = IncidentDetector(error_threshold=3)
    result = []
    result.append(detector.process_event(1, "api", "SUCCESS"))
    result.append(detector.process_event(1, "api", "ERROR"))
    result.append(detector.process_event(1, "api", "SUCCESS"))
    result.append(detector.process_event(1, "api", "SUCCESS"))
    result.append(detector.process_event(1, "api", "ERROR"))
    result.append(detector.process_event(1, "api", "SUCCESS"))
    result.append(detector.process_event(1, "api", "ERROR"))
    assert result == [None, None, None, None, None, None, "INCIDENT_START"]

def test_incidents_are_per_service():
    detector = IncidentDetector(error_threshold=3)

    api_results = []
    for _ in range(3):
        api_results.append(detector.process_event(1, "api", "ERROR"))

    db_results = []
    for _ in range(2):
        db_results.append(detector.process_event(1, "db", "ERROR"))

    # Check that the API service is in an incident state
    assert api_results == [None, None, "INCIDENT_START"]

    # Check that the DB service is in an incident state
    assert db_results == [None, None]
    assert detector.service_states["api"]["incident_active"] is True
    assert detector.service_states["db"]["incident_active"] is False