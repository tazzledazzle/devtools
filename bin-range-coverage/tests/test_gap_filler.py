import textwrap

import pytest

from src.gap_filler import fill_gaps, format_output
from src.parser import parse_input

BIN = "777777"
PREFIX = int(BIN) * 10**10  # 7777770000000000


def iv(start_offset: int, end_offset: int, brand: str) -> tuple[int, int, str]:
    return (PREFIX + start_offset, PREFIX + end_offset, brand)


def run(bin_str: str, intervals: list[tuple[int, int, str]]) -> list[str]:
    return format_output(fill_gaps(bin_str, intervals))


# ---------------------------------------------------------------------------
# Problem statement example
# ---------------------------------------------------------------------------

def test_example_from_problem():
    """Leading gap is absorbed into VISA; no middle gap; trailing gap absorbed into MASTERCARD."""
    intervals = [
        (1_000_000_000, 3_999_999_999, "VISA"),
        (4_000_000_000, 5_999_999_999, "MASTERCARD"),
    ]
    result = run(BIN, intervals)
    assert result == [
        "7777770000000000,7777773999999999,VISA",
        "7777774000000000,7777779999999999,MASTERCARD",
    ]


# ---------------------------------------------------------------------------
# Full coverage — no changes needed
# ---------------------------------------------------------------------------

def test_already_full_coverage():
    intervals = [(0, 9_999_999_999, "VISA")]
    result = run(BIN, intervals)
    assert result == ["7777770000000000,7777779999999999,VISA"]


def test_full_coverage_two_intervals():
    intervals = [
        (0, 4_999_999_999, "VISA"),
        (5_000_000_000, 9_999_999_999, "MASTERCARD"),
    ]
    result = run(BIN, intervals)
    assert result == [
        "7777770000000000,7777774999999999,VISA",
        "7777775000000000,7777779999999999,MASTERCARD",
    ]


# ---------------------------------------------------------------------------
# Leading gap only
# ---------------------------------------------------------------------------

def test_leading_gap_only():
    """Single interval not starting at offset 0 — extend it backward."""
    intervals = [(5_000_000_000, 9_999_999_999, "AMEX")]
    result = run(BIN, intervals)
    assert result == ["7777770000000000,7777779999999999,AMEX"]


# ---------------------------------------------------------------------------
# Trailing gap only
# ---------------------------------------------------------------------------

def test_trailing_gap_only():
    """Single interval not ending at offset 9999999999 — extend it forward."""
    intervals = [(0, 4_999_999_999, "AMEX")]
    result = run(BIN, intervals)
    assert result == ["7777770000000000,7777779999999999,AMEX"]


# ---------------------------------------------------------------------------
# Middle gap
# ---------------------------------------------------------------------------

def test_middle_gap_filled_by_previous_brand():
    """Gap between two intervals is filled by extending the preceding interval."""
    intervals = [
        (0, 2_999_999_999, "VISA"),
        (5_000_000_000, 9_999_999_999, "MASTERCARD"),
    ]
    result = run(BIN, intervals)
    assert result == [
        "7777770000000000,7777774999999999,VISA",
        "7777775000000000,7777779999999999,MASTERCARD",
    ]


def test_multiple_middle_gaps():
    """Each gap is absorbed by the preceding interval:
    VISA  absorbs leading gap + first middle gap (extends to 3_999_999_999),
    MC    absorbs second middle gap (extends to 6_999_999_999),
    AMEX  absorbs trailing gap (extends to 9_999_999_999).
    """
    intervals = [
        (1_000_000_000, 2_000_000_000, "VISA"),
        (4_000_000_000, 5_000_000_000, "MASTERCARD"),
        (7_000_000_000, 8_000_000_000, "AMEX"),
    ]
    result = run(BIN, intervals)
    assert result == [
        "7777770000000000,7777773999999999,VISA",
        "7777774000000000,7777776999999999,MASTERCARD",
        "7777777000000000,7777779999999999,AMEX",
    ]


def test_all_gaps_leading_middle_trailing():
    """Leading gap extends VISA backward; middle gap extends VISA forward;
    trailing gap extends MASTERCARD forward — full BIN range always covered."""
    intervals = [
        (2_000_000_000, 3_999_999_999, "VISA"),
        (6_000_000_000, 7_999_999_999, "MASTERCARD"),
    ]
    result = run(BIN, intervals)
    assert result == [
        "7777770000000000,7777775999999999,VISA",
        "7777776000000000,7777779999999999,MASTERCARD",
    ]


# ---------------------------------------------------------------------------
# Sorting
# ---------------------------------------------------------------------------

def test_unsorted_input_sorted_output():
    """Input intervals provided out of order — output must be sorted by start."""
    intervals = [
        (5_000_000_000, 9_999_999_999, "MASTERCARD"),
        (0, 4_999_999_999, "VISA"),
    ]
    result = run(BIN, intervals)
    assert result == [
        "7777770000000000,7777774999999999,VISA",
        "7777775000000000,7777779999999999,MASTERCARD",
    ]


# ---------------------------------------------------------------------------
# Single interval
# ---------------------------------------------------------------------------

def test_single_interval_full_coverage():
    intervals = [(0, 9_999_999_999, "DISCOVER")]
    result = run(BIN, intervals)
    assert result == ["7777770000000000,7777779999999999,DISCOVER"]


def test_single_interval_interior():
    """Interior interval: both leading and trailing gaps filled by same brand."""
    intervals = [(3_000_000_000, 6_999_999_999, "UNIONPAY")]
    result = run(BIN, intervals)
    assert result == ["7777770000000000,7777779999999999,UNIONPAY"]


# ---------------------------------------------------------------------------
# Parser round-trip
# ---------------------------------------------------------------------------

def test_parse_and_fill():
    text = textwrap.dedent("""\
        777777
        2
        1000000000,3999999999,VISA
        4000000000,5999999999,MASTERCARD
    """)
    bin_str, intervals = parse_input(text)
    result = format_output(fill_gaps(bin_str, intervals))
    assert result[0].startswith("7777770000000000")
    assert result[-1].endswith("9999999999,MASTERCARD")
