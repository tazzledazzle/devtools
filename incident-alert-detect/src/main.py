
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

    def process_event(self, timestamp, service, status):
        """
        Returns 'INCIDENT_START', None, or later 'INCIDENT_RESOLVED'
        """
        pass
