# add local backend
# JSONL append model using pathlib, deterministic replay in
# write order, and list returns stable event identifiers
# (e.g. line numbers or UUIDs) for testing and replay purposes.
import json
from pathlib import Path

class LocalArchiveStorage:
    def __init__(self, file_path):
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, event):
        """Append an event to the archive as a JSON line."""
        with self.file_path.open('a', encoding='utf-8') as f:
            json_line = json.dumps(event)
            f.write(json_line + '\n')

    def list(self):
        """List all events in the archive with their line numbers as identifiers."""
        if not self.file_path.exists():
            return []

        events = []
        with self.file_path.open('r', encoding='utf-8') as f:
            for line_number, line in enumerate(f, start=1):
                try:
                    event = json.loads(line.strip())
                    events.append((line_number, event))
                except json.JSONDecodeError:
                    continue  # skip malformed lines
        return events

    def replace(self, events):
        """Replace all events in the archive with the given list of events."""
        with self.file_path.open('w', encoding='utf-8') as f:
            for event in events:
                json_line = json.dumps(event)
                f.write(json_line + '\n')
