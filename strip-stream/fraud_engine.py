from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class EvaluationResult:
    time: int
    unique_id: str
    amount: int
    decision: str
    triggered_rules: list[dict] = field(default_factory=list)

    def __str__(self) -> str:
        return f"{self.time} {self.unique_id} {self.amount} {self.decision}"


FIELDS = ("unique_id", "amount", "card_number", "merchant")


def process_fraud_detection(
    requests: list[dict],
    rules: list[dict],
    include_audit: bool = False,
) -> list[str] | list[EvaluationResult]:
    # Build (field, value) -> earliest activation time
    min_rule_time: dict[tuple[str, str], int] = defaultdict(lambda: 2**63)
    for rule in rules:
        key = (rule["field"], rule["value"])
        if rule["time"] < min_rule_time[key]:
            min_rule_time[key] = rule["time"]

    sorted_requests = sorted(
        enumerate(requests),
        key=lambda pair: (pair[1]["time"], pair[0]),
    )

    results: list[EvaluationResult] = []
    for _, req in sorted_requests:
        t = req["time"]

        # Collect every field that triggered a rule (full scan, no short-circuit)
        triggered = [
            {"field": f, "value": str(req[f]), "rule_activated_at": min_rule_time[(f, str(req[f]))]}
            for f in FIELDS
            if min_rule_time[(f, str(req[f]))] <= t
        ]

        results.append(
            EvaluationResult(
                time=t,
                unique_id=req["unique_id"],
                amount=req["amount"],
                decision="REJECT" if triggered else "APPROVE",
                triggered_rules=triggered,
            )
        )

    if include_audit:
        return results

    return [str(r) for r in results]
