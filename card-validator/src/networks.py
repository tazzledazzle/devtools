"""
Card network detection by length and prefix.

VISA:       16 digits, first digit = 4
MASTERCARD: 16 digits, first two digits in 51–55
AMEX:       15 digits, first two digits in 34 or 37
"""

_MASTERCARD_PREFIXES = frozenset({"51", "52", "53", "54", "55"})
_AMEX_PREFIXES = frozenset({"34", "37"})


def detect_network(card: str) -> str | None:
    """Return network name if length+prefix match, None otherwise."""
    n = len(card)
    if n == 16 and card[0] == "4":
        return "VISA"
    if n == 16 and card[:2] in _MASTERCARD_PREFIXES:
        return "MASTERCARD"
    if n == 15 and card[:2] in _AMEX_PREFIXES:
        return "AMEX"
    return None
