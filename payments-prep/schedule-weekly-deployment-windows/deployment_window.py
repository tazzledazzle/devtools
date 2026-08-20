import csv
from dataclasses import dataclass
from io import StringIO

MINUTES_PER_WEEK = 7 * 24 * 60  # 10,080
MAX_WEEKS_LOOKAHEAD = 52


@dataclass
class Window:
    start: int
    end: int
    type: str  # "allowed" or "freeze"


def merge_intervals(intervals: list[tuple[int, int]]) -> list[tuple[int, int]]:
    """Merge overlapping or touching half-open intervals [start, end)."""
    if not intervals:
        return []
    sorted_intervals = sorted(intervals, key=lambda x: (x[0], x[1]))
    merged = [sorted_intervals[0]]
    for current_start, current_end in sorted_intervals[1:]:
        last_start, last_end = merged[-1]
        if current_start <= last_end:
            merged[-1] = (last_start, max(last_end, current_end))
        else:
            merged.append((current_start, current_end))
    return merged


def subtract_intervals(
    allowed: list[tuple[int, int]],
    freeze: list[tuple[int, int]],
) -> list[tuple[int, int]]:
    """Subtract freeze intervals from allowed intervals."""
    allowed = merge_intervals(allowed)
    freeze = merge_intervals(freeze)
    result: list[tuple[int, int]] = []
    f_idx, n_freeze = 0, len(freeze)

    for a_start, a_end in allowed:
        curr_start = a_start
        while f_idx < n_freeze and freeze[f_idx][1] <= curr_start:
            f_idx += 1
        tmp_f = f_idx
        while tmp_f < n_freeze and freeze[tmp_f][0] < a_end:
            f_start, f_end = freeze[tmp_f]
            if curr_start < f_start:
                result.append((curr_start, f_start))
            curr_start = max(curr_start, f_end)
            if f_end >= a_end:
                break
            tmp_f += 1
        if curr_start < a_end:
            result.append((curr_start, a_end))
    return result


class DeploymentWindowScheduler:
    def __init__(self) -> None:
        self.windows: list[Window] = []

    def parse_csv_input(self, csv_data: str, offset_minutes: int = 0) -> None:
        """Parse inline CSV with header ``start,end,type`` (local minutes).

        Args:
            csv_data: Raw CSV string.
            offset_minutes: Local UTC offset in minutes (UTC-5 → -300).
        """
        f = StringIO(csv_data.strip())
        reader = csv.DictReader(f)
        for row in reader:
            clean = {k.strip().lower(): v.strip() for k, v in row.items()}
            local_start = int(clean["start"])
            local_end = int(clean["end"])
            window_type = clean["type"].lower()

            utc_start = (local_start - offset_minutes) % MINUTES_PER_WEEK
            utc_end = (local_end - offset_minutes) % MINUTES_PER_WEEK

            if utc_start == utc_end:
                self.windows.append(Window(0, MINUTES_PER_WEEK, window_type))
            elif utc_start < utc_end:
                self.windows.append(Window(utc_start, utc_end, window_type))
            else:
                self.windows.append(Window(utc_start, MINUTES_PER_WEEK, window_type))
                self.windows.append(Window(0, utc_end, window_type))

    def get_weekly_schedule(self) -> list[tuple[int, int]]:
        """Return clean allowed windows for a single UTC week."""
        allowed = [(w.start, w.end) for w in self.windows if w.type == "allowed"]
        freeze = [(w.start, w.end) for w in self.windows if w.type == "freeze"]
        return subtract_intervals(allowed, freeze)

    def get_absolute_schedule(
        self,
        utc_now: int,
        lead_time: int,
        min_continuous: int,
        k: int,
    ) -> list[tuple[int, int]]:
        """Project the cyclic schedule onto absolute time within one week.

        Args:
            utc_now: Current absolute UTC minute (epoch-relative).
            lead_time: Mandatory prep time before first deploy (minutes).
            min_continuous: Minimum required window duration (minutes).
            k: Maximum windows to return.

        Returns:
            Up to ``k`` ``(start, end)`` tuples clipped to a one-week horizon.
        """
        weekly_utc = self.get_weekly_schedule()
        if not weekly_utc:
            return []

        earliest_start = utc_now + lead_time
        latest_end = earliest_start + MINUTES_PER_WEEK

        start_week_idx = earliest_start // MINUTES_PER_WEEK
        end_week_idx = latest_end // MINUTES_PER_WEEK

        candidates: list[tuple[int, int]] = []
        for week_idx in range(start_week_idx, end_week_idx + 1):
            base = week_idx * MINUTES_PER_WEEK
            for w_start, w_end in weekly_utc:
                clipped_start = max(base + w_start, earliest_start)
                clipped_end = min(base + w_end, latest_end)
                if clipped_start < clipped_end:
                    candidates.append((clipped_start, clipped_end))

        merged = merge_intervals(candidates)
        valid = [(s, e) for s, e in merged if (e - s) >= min_continuous]
        return valid[:k]

    def find_next_k_windows(
        self,
        utc_now: int,
        lead_time: int,
        min_continuous: int,
        k: int,
        max_weeks: int = MAX_WEEKS_LOOKAHEAD,
    ) -> list[tuple[int, int]]:
        """Find the next k qualifying windows across as many weeks as needed.

        Expands the search week-by-week until k windows are collected or
        ``max_weeks`` is exhausted, whichever comes first. Each week's
        candidate windows are evaluated independently so partially-clipped
        windows at week boundaries are still considered.

        Args:
            utc_now: Current absolute UTC minute (epoch-relative).
            lead_time: Mandatory prep time before the first deployment.
            min_continuous: Minimum window duration in minutes.
            k: Number of qualifying windows to find.
            max_weeks: Hard cap on how far ahead to search (default 52).

        Returns:
            Up to ``k`` ``(abs_start, abs_end)`` tuples in ascending order.
        """
        weekly_utc = self.get_weekly_schedule()
        if not weekly_utc:
            return []

        earliest_start = utc_now + lead_time
        start_week_idx = earliest_start // MINUTES_PER_WEEK

        collected: list[tuple[int, int]] = []

        for week_offset in range(max_weeks):
            week_idx = start_week_idx + week_offset
            base = week_idx * MINUTES_PER_WEEK

            for w_start, w_end in weekly_utc:
                abs_start = base + w_start
                abs_end = base + w_end

                # clip only the leading edge of the very first week
                clipped_start = max(abs_start, earliest_start)
                if clipped_start >= abs_end:
                    continue

                if (abs_end - clipped_start) >= min_continuous:
                    collected.append((clipped_start, abs_end))

            if len(collected) >= k:
                break

        return collected[:k]


# ======================================================================
# Usage
# ======================================================================
if __name__ == "__main__":
    csv_input = """start,end,type
    1260, 6780, allowed
    5040, 5160, freeze
    9000, 500, allowed
    """

    scheduler = DeploymentWindowScheduler()
    scheduler.parse_csv_input(csv_input, offset_minutes=-300)

    print("--- Weekly UTC Schedule ---")
    for s, e in scheduler.get_weekly_schedule():
        print(f"  {s:>5} → {e:>5}  ({e - s} min)")

    print("\n--- get_absolute_schedule (one-week horizon, k=3) ---")
    for s, e in scheduler.get_absolute_schedule(utc_now=0, lead_time=60, min_continuous=120, k=3):
        print(f"  abs {s:>6} → {e:>6}  ({e - s} min)")

    print("\n--- find_next_k_windows (multi-week, k=6) ---")
    for s, e in scheduler.find_next_k_windows(utc_now=0, lead_time=60, min_continuous=120, k=6):
        week = s // MINUTES_PER_WEEK
        print(f"  week {week}  abs {s:>6} → {e:>6}  ({e - s} min)")
