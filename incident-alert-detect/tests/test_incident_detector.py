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

def test_rate_based_trigger_fires_below_count_threshold():
    detector = IncidentDetector(error_threshold=100, window_size=4, rate_threshold=0.5)
    results = []
    results.append(detector.process_event(1, "api", "ERROR"))  # 1 error, 0 success, total 1 < window
    results.append(detector.process_event(1, "api", "SUCCESS"))  # 1 error, 1 success, total 2 < window
    results.append(detector.process_event(1, "api", "ERROR"))  # 2 error, 1 success, total 3 < window
    results.append(detector.process_event(1, "api", "SUCCESS"))  # 2 error, 2 success, total 4 == window, error rate = 0.5, should trigger
    assert results == [None, None, None, "INCIDENT_START"]



def test_rate_below_threshold_never_triggers():
    detector = IncidentDetector(error_threshold=100, window_size=4, rate_threshold=0.5)
    results = []
    results.append(detector.process_event(1, "api", "SUCCESS"))  # 1 error, 0 success, total 1 < window
    results.append(detector.process_event(2, "api", "SUCCESS"))  # 1 error, 1 success, total 2 < window
    results.append(detector.process_event(3, "api", "SUCCESS"))  # 2 error, 2 success, total 4 == window, error rate = 0.5, should trigger
    results.append(detector.process_event(4, "api", "ERROR"))  # 2 error, 1 success, total 3 < window
    assert results == [None, None, None, None]
    assert detector.service_states["api"]["incident_active"] is False

def test_count_trigger_works_without_rate_params():
    detector = IncidentDetector(error_threshold=2) # no window_size or rate_threshold
    results = []
    results.append(detector.process_event(1, "api", "ERROR"))
    results.append(detector.process_event(2, "api", "ERROR"))
    assert results == [None, "INCIDENT_START"]


def test_incident_resolves_when_rate_drops():
    detector = IncidentDetector(error_threshold=100, window_size=2, rate_threshold=0.5)
    results = []
    results.append(detector.process_event(1, "api", "ERROR"))  # 1 error, 0 success, total 1 < window
    results.append(detector.process_event(2, "api", "ERROR"))  # 2 error, 0 success, total 2 < window
    results.append(detector.process_event(3, "api", "SUCCESS"))  # 2 error, 1 success, total 3 < window
    results.append(detector.process_event(4, "api", "SUCCESS"))  # 2 error, 2 success, total 4 == window, error rate = 0.5, should trigger
    results.append(detector.process_event(5, "api", "SUCCESS"))  # 2 error, 3 success, total 5 > window, error rate = 0.4 < threshold -> should resolve
    assert results == [None, "INCIDENT_START", None, None,  "INCIDENT_RESOLVED"]
    assert detector.service_states["api"]["incident_active"] is False


def test_count_triggered_incident_never_resolves_without_rate_config():
    detector = IncidentDetector(error_threshold=2) # no window_size or rate_threshold
    results = []
    results.append(detector.process_event(1, "api", "ERROR"))
    results.append(detector.process_event(2, "api", "ERROR"))  # starts incident
    results.append(detector.process_event(3, "api", "SUCCESS"))
    results.append(detector.process_event(4, "api", "SUCCESS"))
    results.append(detector.process_event(5, "api", "SUCCESS"))
    assert results == [None, "INCIDENT_START", None, None, None]
    assert detector.service_states["api"]["incident_active"] is True