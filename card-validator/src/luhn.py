"""
Luhn algorithm (mod-10 checksum).

Positions are counted from the right (0-indexed):
  - Position 0  (rightmost / check digit): not doubled.
  - Odd positions (1, 3, 5, …): doubled; if result > 9, subtract 9.
  - Even positions ≥ 2: not doubled.

Sum of all values must be divisible by 10 for a valid card.
"""


def luhn_check(card: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(card)):
        n = int(ch)
        if i % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return total % 10 == 0


def luhn_sum(card: str) -> int:
    """Return the raw Luhn sum (for testing)."""
    total = 0
    for i, ch in enumerate(reversed(card)):
        n = int(ch)
        if i % 2 == 1:
            n *= 2
            if n > 9:
                n -= 9
        total += n
    return total
