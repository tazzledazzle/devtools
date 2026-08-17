from dataclasses import dataclass, field


@dataclass
class Charge:
    charge_id: str
    account_id: str
    amount: int
    code: str
    is_fraud: bool
    disputed: bool = False


@dataclass
class MerchantStats:
    account_id: str
    mcc: str
    charge_ids: list[str] = field(default_factory=list)
    is_fraudulent: bool = False

    def total_count(self, charges: dict[str, Charge]) -> int:
        return len(self.charge_ids)

    def fraud_count(self, charges: dict[str, Charge]) -> int:
        return sum(
            1 for cid in self.charge_ids
            if charges[cid].is_fraud and not charges[cid].disputed
        )
