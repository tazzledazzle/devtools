"""
Input format:

  777777            ← 6-digit BIN
  2                 ← number of intervals (n >= 1)
  1000000000,3999999999,VISA    ← n lines: offset_start,offset_end,brand
  4000000000,5999999999,MASTERCARD
"""


def parse_input(text: str) -> tuple[str, list[tuple[int, int, str]]]:
    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]
    it = iter(lines)

    bin_str = next(it).strip()
    n = int(next(it).strip())

    intervals: list[tuple[int, int, str]] = []
    for _ in range(n):
        parts = next(it).split(",")
        start = int(parts[0].strip())
        end = int(parts[1].strip())
        brand = parts[2].strip()
        intervals.append((start, end, brand))

    return bin_str, intervals
