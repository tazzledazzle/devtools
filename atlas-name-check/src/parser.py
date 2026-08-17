"""
Input format (one line per event):

  account_id|proposed_name            ← name-check request
  RECLAIM,account_id,proposed_name    ← reclamation request (Part 3)

Lines are distinguished by their separator: '|' for checks, ',' for RECLAIM.
"""
from dataclasses import dataclass


@dataclass
class CheckRequest:
    account_id: str
    proposed_name: str


@dataclass
class ReclaimRequest:
    account_id: str
    original_proposed_name: str


Event = CheckRequest | ReclaimRequest


def parse_input(text: str) -> list[Event]:
    events: list[Event] = []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("RECLAIM,"):
            parts = line.split(",", 2)
            events.append(ReclaimRequest(
                account_id=parts[1].strip(),
                original_proposed_name=parts[2].strip(),
            ))
        else:
            account_id, proposed_name = line.split("|", 1)
            events.append(CheckRequest(
                account_id=account_id.strip(),
                proposed_name=proposed_name.strip(),
            ))
    return events
