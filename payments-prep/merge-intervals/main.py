from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Interval:
    start: int
    end: int

def merge_intervals(intervals: list[Interval]) -> list[Interval]:
    if not intervals:
        return []

    sorted_intervals = sorted(intervals, key=lambda i: i.start)
    merged = [sorted_intervals[0]]


    for current in sorted_intervals[1:]:
        last = merged[-1]
        if current.start <= last.end:
            merged[-1] = Interval(last.start, max(last.end, current.end))
        else:
            merged.append(current)

    return merged
