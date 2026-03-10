'''

 Define the minimal event schema as the single source of truth
 for payment-like events.

 Use a TypedDict or dataclass with
  required fields: user_id (str), amount (float),
  timestamp (str, ISO8601 or numeric), device_id (str),
  location (str or dict with lat/lon).

  Export the type and a list of required field names (or a validation
  helper) so the generator and EventSources can reference the same contract.

  Do not hand-roll rich validation; keep it to "required keys present"
   level.

    Document the schema in docstring or comment.
'''

from typing import TypedDict, TypeAlias

Location: TypeAlias = str | dict[str, float]

class EventSources(TypedDict):
    """defines the schema for payment-like events.

    Each event must include the following fields:
        - user_id (str): A unique identifier for the user.
        - amount (float): The amount of the transaction.
        - timestamp (str): The time of the event in ISO8601 format or as a numeric timestamp.
        - device_id (str): A unique identifier for the device used in the transaction.
        - location (str or dict): The location of the event, either as a string or a dictionary with latitude and longitude.
    """
    user_id: str
    amount: float
    timestamp: str # ISO8601 or numeric
    device_id: str
    location: Location


# source of truth for required fields across generator + sources
REQUIRED_FIELDS: tuple[str, ...] = (
    "user_id",
    "amount",
    "timestamp",
    "device_id",
    "location",
)


def has_required_fields(payload: dict[str, object]) -> bool:
    """Return True when all required event keys are present."""
    return all(field in payload for field in REQUIRED_FIELDS)