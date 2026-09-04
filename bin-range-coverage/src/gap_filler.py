"""
Fill gaps in BIN range coverage.

A BIN range spans offsets 0000000000 through 9999999999 (10 digits).
Full card numbers are 16 digits: the 6-digit BIN followed by the 10-digit offset.

Gap-filling strategy:
  - Leading gap  (before first interval): extend first interval's start backward.
  - Trailing gap (after last interval):  extend last interval's end forward.
  - Middle gap   (between two intervals): extend the preceding interval's end
                                          to fill up to the next interval's start.
"""

OFFSET_MIN = 0
OFFSET_MAX = 9_999_999_999  # 10-digit max


def fill_gaps(bin_str: str, intervals: list[tuple[int, int, str]]) -> list[tuple[int, int, str]]:
    bin_prefix = int(bin_str) * (10 ** 10)
    full_start = bin_prefix + OFFSET_MIN
    full_end = bin_prefix + OFFSET_MAX

    # Convert offsets to full 16-digit card numbers and sort by start.
    converted = sorted(
        [(bin_prefix + s, bin_prefix + e, brand) for s, e, brand in intervals],
        key=lambda iv: iv[0],
    )

    result: list[tuple[int, int, str]] = []
    cursor = full_start

    for i, (start, end, brand) in enumerate(converted):
        if start > cursor:
            if i == 0:
                # Leading gap: absorb it into the first interval by extending start.
                result.append((cursor, end, brand))
            else:
                # Middle gap: extend the previous interval's end to fill the gap.
                prev_start, _, prev_brand = result[-1]
                result[-1] = (prev_start, start - 1, prev_brand)
                result.append((start, end, brand))
        else:
            result.append((start, end, brand))

        cursor = end + 1

    # Trailing gap: extend the last interval's end to the full BIN range limit.
    if cursor <= full_end:
        last_start, _, last_brand = result[-1]
        result[-1] = (last_start, full_end, last_brand)

    return result


def format_output(intervals: list[tuple[int, int, str]]) -> list[str]:
    return [f"{start},{end},{brand}" for start, end, brand in intervals]
