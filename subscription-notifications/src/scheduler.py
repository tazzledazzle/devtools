"""
Subscription notification scheduler.

send_schedule maps offset keys to email type strings:
  "start"       → event on subscription start day
  "end"         → event on subscription end day
  -N (int < 0)  → event N days before end day

Event output format:
  <day>: [<Email Type>] Subscription for <name> (<plan>)
"""

from dataclasses import dataclass


@dataclass
class EmailEvent:
    day: int
    email_type: str
    name: str
    plan: str

    def __str__(self) -> str:
        return f"{self.day}: [{self.email_type}] Subscription for {self.name} ({self.plan})"


SendSchedule = dict[str | int, str]


def _event_day(offset: str | int, start: int, end: int) -> int:
    if offset == "start":
        return start
    if offset == "end":
        return end
    return end + offset  # offset is a negative int


class SubscriptionScheduler:
    def __init__(self, send_schedule: SendSchedule) -> None:
        self.send_schedule = send_schedule

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def schedule(
        self,
        users: list[dict],
        changes: list[dict] | None = None,
    ) -> list[str]:
        """Return all email event lines sorted by day (stable on ties)."""
        all_events: list[EmailEvent] = []
        for user in users:
            all_events.extend(self._process_user(user, changes or []))
        all_events.sort(key=lambda e: e.day)
        return [str(e) for e in all_events]

    # ------------------------------------------------------------------
    # Per-user processing
    # ------------------------------------------------------------------

    def _process_user(self, user: dict, changes: list[dict]) -> list[EmailEvent]:
        name = user["name"]
        start = user["account_date"]
        current_plan = user["plan"]
        current_end = start + user["duration"]

        # Seed with the full initial schedule.
        events = self._gen_initial_events(name, current_plan, start, current_end)

        user_changes = sorted(
            [c for c in changes if c["name"] == name],
            key=lambda c: c["change_date"],
        )

        for change in user_changes:
            events, current_plan, current_end = self._apply_change_or_renewal(
                events=events,
                change=change,
                cd=change["change_date"],
                name=name,
                current_plan=current_plan,
                current_end=current_end,
                start=start,
            )

        return events

    # ------------------------------------------------------------------
    # Event generation helpers
    # ------------------------------------------------------------------

    def _gen_initial_events(
        self, name: str, plan: str, start: int, end: int
    ) -> list[EmailEvent]:
        return [
            EmailEvent(_event_day(offset, start, end), email_type, name, plan)
            for offset, email_type in self.send_schedule.items()
        ]

    def _apply_change_or_renewal(
        self,
        events: list[EmailEvent],
        change: dict,
        cd: int,
        name: str,
        current_plan: str,
        current_end: int,
        start: int,
    ) -> tuple[list[EmailEvent], str, int]:
        """Apply one plan-change or renewal event.

        TODO: implement this method (~10 lines).

        A change at day `cd` must:
          1. Cancel all events with day >= cd (they are now stale).
          2. Derive the new state:
               - Plan change  → new_plan = change["new_plan"],  end stays the same.
               - Renewal      → plan stays,  new_end = current_end + change["extension"].
          3. Append a marker event at day cd:
               - "[Changed]"  for a plan change (use the NEW plan name).
               - "[Renewed]"  for a renewal   (use the current plan name).
          4. Re-schedule future events: for every offset/type pair in
             self.send_schedule, SKIP "start" (already happened), compute the
             event day using new_end, and append it only if day >= cd.
          5. Return (updated_events, new_plan, new_end).

        Parameters
        ----------
        events       Current list of EmailEvents for this user.
        change       Dict with "change_date" and either "new_plan" or "extension".
        cd           change["change_date"] — the day the change takes effect.
        name         User name for constructing EmailEvent objects.
        current_plan Plan active before this change.
        current_end  Subscription end day before this change.
        start        Original subscription start day (used by _event_day).
        """
        events = [e for e in events if e.day < cd]  # Cancel stale events
        if "new_plan" in change:
            new_plan = change["new_plan"]
            new_end = current_end
            events.append(EmailEvent(cd, "Changed", name, new_plan))
        elif "extension" in change:
            new_plan = current_plan
            new_end = current_end + change["extension"]
            events.append(EmailEvent(cd, "Renewed", name, new_plan))
        else:
            raise ValueError("Change must have either 'new_plan' or 'extension'.")

        # Re-schedule future events
        for offset, email_type in self.send_schedule.items():
            if offset == "start":
                continue  # Skip start events
            event_day = _event_day(offset, start, new_end)
            if event_day >= cd:
                events.append(EmailEvent(event_day, email_type, name, new_plan))
        return events, new_plan, new_end
