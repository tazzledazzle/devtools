from abc import ABC, abstractmethod
from collections import defaultdict

from .models import Charge, MerchantStats


class FraudDetectorBase(ABC):
    def __init__(
        self,
        non_fraud_codes: set[str],
        fraud_codes: set[str],
        mcc_thresholds: dict[str, float],
        merchant_mcc: dict[str, str],
        min_transactions: int,
    ):
        self.fraud_codes = fraud_codes
        self.mcc_thresholds = mcc_thresholds
        self.merchant_mcc = merchant_mcc
        self.min_transactions = min_transactions

        self.charges: dict[str, Charge] = {}
        self.merchants: dict[str, MerchantStats] = {
            account_id: MerchantStats(account_id=account_id, mcc=mcc)
            for account_id, mcc in merchant_mcc.items()
        }

    def process_charge(self, charge_id: str, account_id: str, amount: int, code: str) -> None:
        charge = Charge(
            charge_id=charge_id,
            account_id=account_id,
            amount=amount,
            code=code,
            is_fraud=code in self.fraud_codes,
        )
        self.charges[charge_id] = charge

        if account_id not in self.merchants:
            return

        merchant = self.merchants[account_id]
        merchant.charge_ids.append(charge_id)
        self._evaluate(merchant)

    def process_dispute(self, charge_id: str) -> None:
        if charge_id not in self.charges:
            return
        charge = self.charges[charge_id]
        charge.disputed = True
        if charge.account_id in self.merchants:
            self._on_dispute(self.merchants[charge.account_id])

    def fraudulent_merchants(self) -> list[str]:
        return sorted(m.account_id for m in self.merchants.values() if m.is_fraudulent)

    def _evaluate(self, merchant: MerchantStats) -> None:
        total = merchant.total_count(self.charges)
        if total < self.min_transactions:
            return
        fraud = merchant.fraud_count(self.charges)
        threshold = self.mcc_thresholds.get(merchant.mcc)
        if threshold is None:
            return
        if self._exceeds_threshold(fraud, total, threshold):
            merchant.is_fraudulent = True

    @abstractmethod
    def _exceeds_threshold(self, fraud: int, total: int, threshold: float) -> bool:
        ...

    @abstractmethod
    def _on_dispute(self, merchant: MerchantStats) -> None:
        ...


class CountBasedDetector(FraudDetectorBase):
    """Part 1: fraudulent when fraud_count >= threshold (integer). Sticky."""

    def _exceeds_threshold(self, fraud: int, total: int, threshold: float) -> bool:
        return fraud >= threshold

    def _on_dispute(self, merchant: MerchantStats) -> None:
        # Part 1 does not support disputes; no-op.
        pass


class PercentageBasedDetector(FraudDetectorBase):
    """Part 2: fraudulent when fraud_count / total >= threshold (fraction). Sticky."""

    def _exceeds_threshold(self, fraud: int, total: int, threshold: float) -> bool:
        return (fraud / total) >= threshold

    def _on_dispute(self, merchant: MerchantStats) -> None:
        # Part 2 does not support disputes; no-op.
        pass


class DisputeAwareDetector(FraudDetectorBase):
    """Part 3: percentage-based with disputes. Fraudulent status is re-evaluated
    after each dispute — merchants can return to non-fraudulent if disputed
    transactions were the sole cause."""

    def _exceeds_threshold(self, fraud: int, total: int, threshold: float) -> bool:
        return (fraud / total) >= threshold

    def _on_dispute(self, merchant: MerchantStats) -> None:
        # Recompute from current (post-dispute) state instead of using the
        # sticky flag, allowing status to be reversed.
        merchant.is_fraudulent = False
        self._evaluate(merchant)


def run(parsed: dict, detector_class: type[FraudDetectorBase]) -> list[str]:
    detector = detector_class(
        non_fraud_codes=parsed["non_fraud_codes"],
        fraud_codes=parsed["fraud_codes"],
        mcc_thresholds=parsed["mcc_thresholds"],
        merchant_mcc=parsed["merchant_mcc"],
        min_transactions=parsed["min_transactions"],
    )
    for event in parsed["events"]:
        if event["type"] == "CHARGE":
            detector.process_charge(
                event["charge_id"],
                event["account_id"],
                event["amount"],
                event["code"],
            )
        elif event["type"] == "DISPUTE":
            detector.process_dispute(event["charge_id"])

    return detector.fraudulent_merchants()
