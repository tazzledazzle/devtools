# Local filesystem backend: JSONL append, list returns identifiers, replay in order.
import json
from pathlib import Path
from typing import Any, Iterator, List

from archive.storage import ArchiveStorage


class LocalArchiveStorage(ArchiveStorage):
    """Archive backend that appends events to a single JSONL file."""

    def __init__(self, file_path: str | Path) -> None:
        self.file_path = Path(file_path)
        self.file_path.parent.mkdir(parents=True, exist_ok=True)

    def write(self, event: dict[str, Any]) -> None:
        """Append an event to the archive as a JSON line."""
        with self.file_path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(event) + "\n")

    def list(self) -> List[str]:
        """Return event identifiers (line numbers as strings) in replay order."""
        if not self.file_path.exists():
            return []
        keys: List[str] = []
        with self.file_path.open("r", encoding="utf-8") as f:
            for line_number, line in enumerate(f, start=1):
                line = line.strip()
                if line:
                    try:
                        json.loads(line)
                        keys.append(str(line_number))
                    except json.JSONDecodeError:
                        continue
        return keys

    def replay(self) -> Iterator[dict[str, Any]]:
        """Yield stored events in write order."""
        if not self.file_path.exists():
            return
        with self.file_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
