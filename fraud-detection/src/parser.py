"""
Expected input format (lines in order):

  approved,invalid_pin,...          ← non-fraud codes (comma-separated)
  do_not_honor,stolen_card,...      ← fraud codes (comma-separated)
  MCC_A,threshold                   ← MCC rows (repeat until blank line)
                                    ← blank line
  account1,MCC_A                    ← merchant rows (repeat until blank line)
                                    ← blank line
  5                                 ← minimum transactions
  CHARGE,id,account,amount,code     ← events (CHARGE or DISPUTE)
  DISPUTE,id
  ...
"""


def parse_input(text: str) -> dict:
    lines = [l.rstrip() for l in text.splitlines()]
    it = iter(lines)

    non_fraud_codes = {c.strip() for c in next(it).split(",")}
    fraud_codes = {c.strip() for c in next(it).split(",")}

    mcc_thresholds: dict[str, float] = {}
    for line in it:
        if not line:
            break
        mcc, threshold = line.split(",")
        mcc_thresholds[mcc.strip()] = float(threshold.strip())

    merchant_mcc: dict[str, str] = {}
    for line in it:
        if not line:
            break
        account_id, mcc = line.split(",")
        merchant_mcc[account_id.strip()] = mcc.strip()

    min_transactions = int(next(it).strip())

    events: list[dict] = []
    for line in it:
        if not line:
            continue
        parts = line.split(",")
        kind = parts[0].strip()
        if kind == "CHARGE":
            events.append({
                "type": "CHARGE",
                "charge_id": parts[1].strip(),
                "account_id": parts[2].strip(),
                "amount": int(parts[3].strip()),
                "code": parts[4].strip(),
            })
        elif kind == "DISPUTE":
            events.append({"type": "DISPUTE", "charge_id": parts[1].strip()})

    return {
        "non_fraud_codes": non_fraud_codes,
        "fraud_codes": fraud_codes,
        "mcc_thresholds": mcc_thresholds,
        "merchant_mcc": merchant_mcc,
        "min_transactions": min_transactions,
        "events": events,
    }
