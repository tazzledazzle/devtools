import textwrap

import pytest

from src.detector import (
    CountBasedDetector,
    DisputeAwareDetector,
    PercentageBasedDetector,
    run,
)
from src.parser import parse_input

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def run_input(text: str, detector_class):
    return run(parse_input(textwrap.dedent(text).strip()), detector_class)


# ---------------------------------------------------------------------------
# Part 1 — count-based
# ---------------------------------------------------------------------------

PART1_BASE = """\
    approved,invalid_pin,expired_card
    do_not_honor,stolen_card,lost_card
    retail,3
    airline,2

    merchant_a,retail
    merchant_b,airline
    merchant_c,retail

    2
"""


def test_part1_no_fraud():
    result = run_input(
        PART1_BASE + "CHARGE,ch1,merchant_a,100,approved\n"
                     "CHARGE,ch2,merchant_a,200,approved\n",
        CountBasedDetector,
    )
    assert result == []


def test_part1_exact_threshold_triggers():
    # retail threshold = 3; 3 fraud charges should trigger
    result = run_input(
        PART1_BASE
        + "CHARGE,ch1,merchant_a,100,stolen_card\n"
          "CHARGE,ch2,merchant_a,100,stolen_card\n"
          "CHARGE,ch3,merchant_a,100,stolen_card\n",
        CountBasedDetector,
    )
    assert result == ["merchant_a"]


def test_part1_below_threshold_not_flagged():
    # airline threshold = 2; only 1 fraud charge
    result = run_input(
        PART1_BASE
        + "CHARGE,ch1,merchant_b,100,do_not_honor\n"
          "CHARGE,ch2,merchant_b,100,approved\n"
          "CHARGE,ch3,merchant_b,100,approved\n",
        CountBasedDetector,
    )
    assert result == []


def test_part1_min_transactions_gate():
    # min = 2; only 1 charge processed before threshold hit — should not flag
    result = run_input(
        PART1_BASE
        + "CHARGE,ch1,merchant_a,100,stolen_card\n",
        CountBasedDetector,
    )
    assert result == []


def test_part1_sticky_after_threshold():
    # merchant_a crosses threshold then gets an approved charge — stays fraudulent
    result = run_input(
        PART1_BASE
        + "CHARGE,ch1,merchant_a,100,stolen_card\n"
          "CHARGE,ch2,merchant_a,100,stolen_card\n"
          "CHARGE,ch3,merchant_a,100,stolen_card\n"
          "CHARGE,ch4,merchant_a,100,approved\n",
        CountBasedDetector,
    )
    assert result == ["merchant_a"]


def test_part1_multiple_fraudulent_sorted():
    result = run_input(
        PART1_BASE
        + "CHARGE,ch1,merchant_b,100,do_not_honor\n"
          "CHARGE,ch2,merchant_b,100,do_not_honor\n"
          "CHARGE,ch3,merchant_a,100,stolen_card\n"
          "CHARGE,ch4,merchant_a,100,stolen_card\n"
          "CHARGE,ch5,merchant_a,100,stolen_card\n",
        CountBasedDetector,
    )
    assert result == ["merchant_a", "merchant_b"]


# ---------------------------------------------------------------------------
# Part 2 — percentage-based
# ---------------------------------------------------------------------------

PART2_BASE = """\
    approved,invalid_pin
    do_not_honor,stolen_card
    retail,0.5
    airline,0.4

    merchant_a,retail
    merchant_b,airline

    3
"""


def test_part2_at_threshold_triggers():
    # 2 fraud / 4 total = 0.5 >= 0.5
    result = run_input(
        PART2_BASE
        + "CHARGE,ch1,merchant_a,100,approved\n"
          "CHARGE,ch2,merchant_a,100,approved\n"
          "CHARGE,ch3,merchant_a,100,do_not_honor\n"
          "CHARGE,ch4,merchant_a,100,do_not_honor\n",
        PercentageBasedDetector,
    )
    assert result == ["merchant_a"]


def test_part2_below_threshold_not_flagged():
    # 1 fraud / 4 total = 0.25 < 0.5
    result = run_input(
        PART2_BASE
        + "CHARGE,ch1,merchant_a,100,approved\n"
          "CHARGE,ch2,merchant_a,100,approved\n"
          "CHARGE,ch3,merchant_a,100,approved\n"
          "CHARGE,ch4,merchant_a,100,do_not_honor\n",
        PercentageBasedDetector,
    )
    assert result == []


def test_part2_sticky_even_if_percentage_drops():
    # Crosses threshold, then more approved charges drop percentage — still fraudulent
    result = run_input(
        PART2_BASE
        + "CHARGE,ch1,merchant_a,100,do_not_honor\n"
          "CHARGE,ch2,merchant_a,100,do_not_honor\n"
          "CHARGE,ch3,merchant_a,100,approved\n"     # 2/3 = 0.66 → FLAGGED
          "CHARGE,ch4,merchant_a,100,approved\n"     # 2/4 = 0.5 still >= threshold (sticky)
          "CHARGE,ch5,merchant_a,100,approved\n",    # 2/5 = 0.4 < threshold (but sticky)
        PercentageBasedDetector,
    )
    assert result == ["merchant_a"]


# ---------------------------------------------------------------------------
# Part 3 — dispute-aware
# ---------------------------------------------------------------------------

PART3_BASE = """\
    approved,invalid_pin
    do_not_honor,stolen_card
    retail,0.5

    merchant_a,retail

    2
"""


def test_part3_dispute_reverses_fraudulent():
    # Crosses threshold via 2 fraud / 2 total = 1.0, then both disputed → reverts
    result = run_input(
        PART3_BASE
        + "CHARGE,ch1,merchant_a,100,do_not_honor\n"
          "CHARGE,ch2,merchant_a,100,do_not_honor\n"
          "DISPUTE,ch1\n"
          "DISPUTE,ch2\n",
        DisputeAwareDetector,
    )
    assert result == []


def test_part3_partial_dispute_stays_fraudulent():
    # 2 fraud / 3 total = 0.66; dispute one → 1/3 = 0.33 < 0.5 → reverts
    # Then a new fraud charge → 2/4 = 0.5 → flagged again
    result = run_input(
        PART3_BASE
        + "CHARGE,ch1,merchant_a,100,do_not_honor\n"
          "CHARGE,ch2,merchant_a,100,do_not_honor\n"
          "CHARGE,ch3,merchant_a,100,approved\n"    # 2/3 → flagged
          "DISPUTE,ch1\n"                            # 1/3 = 0.33 → reverts
          "CHARGE,ch4,merchant_a,100,do_not_honor\n",  # 2/4 = 0.5 → flagged again
        DisputeAwareDetector,
    )
    assert result == ["merchant_a"]


def test_part3_dispute_nonexistent_charge_ignored():
    result = run_input(
        PART3_BASE
        + "CHARGE,ch1,merchant_a,100,approved\n"
          "CHARGE,ch2,merchant_a,100,approved\n"
          "DISPUTE,ch_ghost\n",
        DisputeAwareDetector,
    )
    assert result == []


def test_part3_unknown_merchant_charge_ignored():
    result = run_input(
        PART3_BASE
        + "CHARGE,ch1,unknown_merchant,100,do_not_honor\n"
          "CHARGE,ch2,unknown_merchant,100,do_not_honor\n",
        DisputeAwareDetector,
    )
    assert result == []
