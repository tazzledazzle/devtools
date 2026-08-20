

@dataclass(frozen=True)
class LedgerEntry:
    id: str
    account_id: str
    amount_cents: int

@dataclass
class OutboxEvent:
    id: str
    type: str
    payload: str
    published: bool = False # mutable - only update after broker confirms