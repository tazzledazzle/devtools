from .normalizer import normalize
from .parser import CheckRequest, Event, ReclaimRequest


class NameChecker:
    def __init__(self) -> None:
        # normalized_name → (registrant_account_id, original_proposed_name)
        self._registry: dict[str, tuple[str, str]] = {}

    def check(self, account_id: str, proposed_name: str) -> str:
        normalized = normalize(proposed_name)
        if not normalized or normalized in self._registry:
            return f"{account_id}|Name Not Available"
        self._registry[normalized] = (account_id, proposed_name)
        return f"{account_id}|Name Available"

    def reclaim(self, account_id: str, original_proposed_name: str) -> None:
        # TODO: implement reclamation logic here.
        #
        # Requirements:
        #   - Normalize original_proposed_name the same way as during registration.
        #   - Only remove the entry if it exists AND the registrant matches account_id.
        #   - Silently ignore requests that don't meet the above conditions.
        #
        # Hint: self._registry maps normalized_name → (registrant_id, original_name).
        # You'll need ~5 lines.
        pass

    def run(self, events: list[Event]) -> list[str]:
        output: list[str] = []
        for event in events:
            if isinstance(event, CheckRequest):
                output.append(self.check(event.account_id, event.proposed_name))
            elif isinstance(event, ReclaimRequest):
                self.reclaim(event.account_id, event.original_proposed_name)
        return output
