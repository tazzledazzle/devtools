import csv
from dataclasses import dataclass
from io import StringIO

MINUTES_PER_WEEK = 7 * 24 * 60  # 10,080


@dataclass
class Window:
    """Discrete time interval with a classification type."""
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
    """Subtract freeze intervals from allowed intervals.

    Both lists are merged internally before subtraction so callers
    don't need to pre-process them.

    Note: f_idx is intentionally NOT reset between outer iterations
    because both lists are sorted — once a freeze window ends before
    the current allowed start, it can never affect a later allowed
    window either.
    """
    allowed = merge_intervals(allowed)
    freeze = merge_intervals(freeze)
    result: list[tuple[int, int]] = []
    f_idx, n_freeze = 0, len(freeze)

    for a_start, a_end in allowed:
        curr_start = a_start
        # skip freeze windows that end before this allowed window starts
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
        """Parse an inline CSV string and append normalized Window objects.

        Args:
            csv_data: CSV string with header ``start,end,type``.
                      Values are in local minutes-since-week-start.
            offset_minutes: UTC_offset of the local timezone in minutes
                            (e.g. UTC-5 → -300, UTC+5:30 → 330).
                            UTC = Local - offset_minutes.
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
                # interval spans the full week
                self.windows.append(Window(0, MINUTES_PER_WEEK, window_type))
            elif utc_start < utc_end:
                self.windows.append(Window(utc_start, utc_end, window_type))
            else:
                # wraps midnight — split into two segments
                self.windows.append(Window(utc_start, MINUTES_PER_WEEK, window_type))
                self.windows.append(Window(0, utc_end, window_type))

    # ------------------------------------------------------------------
    # Part 1 — Cyclic weekly UTC schedule
    # ------------------------------------------------------------------

    def get_weekly_schedule(self) -> list[tuple[int, int]]:
        """Return merged allowed windows with freeze windows removed.

        Returns:
            Sorted list of non-overlapping ``(start, end)`` tuples
            in UTC minutes-since-week-start, representing valid
            deployment slots within a single 10,080-minute week.
        """
        allowed = [(w.start, w.end) for w in self.windows if w.type == "allowed"]
        freeze = [(w.start, w.end) for w in self.windows if w.type == "freeze"]
        return subtract_intervals(allowed, freeze)

    # ------------------------------------------------------------------
    # Part 2 — Absolute epoch projection
    # ------------------------------------------------------------------

    def get_absolute_schedule(
        self,
        utc_now: int,
        lead_time: int,
        min_continuous: int,
        k: int,
    ) -> list[tuple[int, int]]:
        """Project the cyclic weekly schedule onto an absolute time horizon.

        Scans exactly one week's worth of time starting at
        ``utc_now + lead_time`` and returns the first ``k`` deployment
        windows that satisfy the minimum continuous-duration requirement.

        Args:
            utc_now: Current time in absolute UTC minutes (epoch-relative).
            lead_time: Mandatory preparation time before first deploy (minutes).
            min_continuous: Minimum required window length in minutes.
            k: Maximum number of windows to return.

        Returns:
            Up to ``k`` ``(abs_start, abs_end)`` tuples clipped to the
            one-week horizon, each at least ``min_continuous`` minutes long.
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
                abs_start = base + w_start
                abs_end = base + w_end

                clipped_start = max(abs_start, earliest_start)
                clipped_end = min(abs_end, latest_end)

                if clipped_start < clipped_end:
                    candidates.append((clipped_start, clipped_end))

        merged = merge_intervals(candidates)
        valid = [(s, e) for s, e in merged if (e - s) >= min_continuous]
        return valid[:k]


# ======================================================================
# Usage / smoke-test
# ======================================================================
if __name__ == "__main__":
    csv_input = """start,end,type
    1260, 6780, allowed
    5040, 5160, freeze
    9000, 500, allowed
    """

    scheduler = DeploymentWindowScheduler()
    scheduler.parse_csv_input(csv_input, offset_minutes=-300)

    print("--- Hydrated Window Objects ---")
    for i, w in enumerate(scheduler.windows):
        print(f"  [{i}] start={w.start:>5}  end={w.end:>5}  type={w.type}")

    print("\n--- Part 1: Weekly UTC Schedule ---")
    weekly = scheduler.get_weekly_schedule()
    for s, e in weekly:
        print(f"  {s:>5} → {e:>5}  (duration: {e - s} min)")

    print("\n--- Part 2: Next 3 Absolute Windows ---")
    # utc_now=0, lead_time=60 min, min window=120 min, top-3
    absolute = scheduler.get_absolute_schedule(
        utc_now=0, lead_time=60, min_continuous=120, k=3
    )
    for s, e in absolute:
        print(f"  abs {s:>6} → {e:>6}  (duration: {e - s} min)")
