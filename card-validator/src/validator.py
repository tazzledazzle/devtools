"""
Card validation dispatching for Parts 1–4.

  Plain card (no special chars) → classify()           → single result string
  Card with '*'                 → classify_redacted()  → list of "NETWORK,count" lines
  Card ending with '?'          → classify_corrupted() → list of "card,NETWORK" lines
"""

from itertools import product

from .luhn import luhn_check
from .networks import detect_network

DIGITS = "0123456789"


# ---------------------------------------------------------------------------
# Parts 1 & 2
# ---------------------------------------------------------------------------

def classify(card: str) -> str:
    network = detect_network(card)
    if network is None:
        return "UNKNOWN_NETWORK"
    if not luhn_check(card):
        return "INVALID_CHECKSUM"
    return network


# ---------------------------------------------------------------------------
# Part 3: Redacted ('*' wildcards)
# ---------------------------------------------------------------------------

def classify_redacted(card: str) -> list[str]:
    star_positions = [i for i, ch in enumerate(card) if ch == "*"]
    counts: dict[str, int] = {}

    for combo in product(DIGITS, repeat=len(star_positions)):
        chars = list(card)
        for pos, digit in zip(star_positions, combo):
            chars[pos] = digit
        candidate = "".join(chars)
        network = detect_network(candidate)
        if network and luhn_check(candidate):
            counts[network] = counts.get(network, 0) + 1

    return [f"{net},{cnt}" for net, cnt in sorted(counts.items())]


# ---------------------------------------------------------------------------
# Part 4: Corrupted ('?' suffix)
# ---------------------------------------------------------------------------

def classify_corrupted(card_q: str) -> list[str]:
    """Find all valid originals of a card corrupted by one digit change or swap.

    The '?' suffix is a marker — the card to analyse is everything before it.
    We generate candidates by:
      1. Replacing each position with every digit 0–9 (covers single-digit errors
         AND the identity, so the original card is included if it's already valid).
      2. Swapping every adjacent pair (reverses a swap error).

    Candidates are deduplicated by card number and sorted numerically.
    """
    card = card_q[:-1]
    found: dict[str, str] = {}  # card_str → network

    def _try(candidate: str) -> None:
        network = detect_network(candidate)
        if network and luhn_check(candidate):
            found[candidate] = network

    # Single-digit substitutions (16 positions × 10 digits = 160 candidates)
    for i in range(len(card)):
        for d in DIGITS:
            _try(card[:i] + d + card[i + 1 :])

    # Adjacent-digit swaps (15 pairs for a 16-digit card)
    for i in range(len(card) - 1):
        chars = list(card)
        chars[i], chars[i + 1] = chars[i + 1], chars[i]
        _try("".join(chars))

    return [f"{c},{net}" for c, net in sorted(found.items(), key=lambda x: int(x[0]))]


# ---------------------------------------------------------------------------
# Unified entry point
# ---------------------------------------------------------------------------

def validate_card(card_str: str) -> list[str]:
    card_str = card_str.strip()
    if card_str.endswith("?"):
        return classify_corrupted(card_str)
    if "*" in card_str:
        return classify_redacted(card_str)
    return [classify(card_str)]
