
"""
Here's the plan for Pattern 1 — Incident/Alert Detection System:

Part 1: raw count threshold — trigger an alert when error count crosses a threshold
Part 2: rate-based trigger — errors as % of total volume, needs a "success window" declared even though Part 1 doesn't use it
Part 3: resolution — alert clears when conditions drop back below threshold
Part 4: persistence/re-triggering — don't spam duplicate alerts, but do re-alert after a resolution
"""


class IncidentDetector:
    def __init__(self, error_threshold: int, window_size: int=None):
        """
        error_threshold: int, raw count of errors that triggers an incident
        window_size: declared now, unused in Part 1, but will be used in Part 2 for rate-based detection
        """
        self.error_threshold = error_threshold
        self.window_size = window_size
        # per-service state goes here
        self.service_states = {}

    def _increment_success_count(self, service):
        self._ensure_service_state(service)
        self.service_states[service]["success_count"] += 1

    def _ensure_service_state(self, service):
        if service not in self.service_states:
            self.service_states[service] = {
                "error_count": 0,
                "success_count": 0,
                "incident_active": False,
            }
    def _increment_error_count(self, service):
        self._ensure_service_state(service)
        self.service_states[service]["error_count"] += 1


    def _check_for_incident_start(self, service):
        state = self.service_states[service]
        if not state["incident_active"] and state["error_count"] >= self.error_threshold:
            state["incident_active"] = True
            return "INCIDENT_START"
        return None
    def process_event(self, timestamp, service, status: str):
        """
        Returns 'INCIDENT_START', None, or later 'INCIDENT_RESOLVED'
        """
        if status == "ERROR":
            self._increment_error_count(service)
            return self._check_for_incident_start(service)
        elif status == "SUCCESS":
            self._increment_success_count(service)
            return None
        return None
