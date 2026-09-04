import pytest

from src.scheduler import EmailEvent, SubscriptionScheduler

# ---------------------------------------------------------------------------
# Shared fixture: the canonical send_schedule used across most tests
# ---------------------------------------------------------------------------

SCHEDULE = {
    "start": "Welcome",
    -15: "Expiry Warning",
    "end": "Expired",
}


def make_scheduler(schedule=None):
    return SubscriptionScheduler(schedule or SCHEDULE)


# ---------------------------------------------------------------------------
# Part 1: Basic scheduling (no changes)
# ---------------------------------------------------------------------------

class TestPart1:
    def test_single_user_all_events(self):
        s = make_scheduler()
        users = [{"name": "Alice", "plan": "Basic", "account_date": 1, "duration": 30}]
        # start=1, end=31, warning=31-15=16
        result = s.schedule(users)
        assert result == [
            "1: [Welcome] Subscription for Alice (Basic)",
            "16: [Expiry Warning] Subscription for Alice (Basic)",
            "31: [Expired] Subscription for Alice (Basic)",
        ]

    def test_start_only_schedule(self):
        s = make_scheduler({"start": "Welcome"})
        users = [{"name": "Bob", "plan": "Pro", "account_date": 5, "duration": 20}]
        result = s.schedule(users)
        assert result == ["5: [Welcome] Subscription for Bob (Pro)"]

    def test_end_only_schedule(self):
        s = make_scheduler({"end": "Expired"})
        users = [{"name": "Carol", "plan": "Basic", "account_date": 1, "duration": 10}]
        result = s.schedule(users)
        assert result == ["11: [Expired] Subscription for Carol (Basic)"]

    def test_multiple_users_sorted_by_day(self):
        s = make_scheduler({"start": "Welcome", "end": "Expired"})
        users = [
            {"name": "Alice", "plan": "Basic", "account_date": 10, "duration": 5},
            {"name": "Bob",   "plan": "Pro",   "account_date": 1,  "duration": 5},
        ]
        result = s.schedule(users)
        assert result[0].startswith("1:")   # Bob starts first
        assert result[1].startswith("6:")   # Bob ends
        assert result[2].startswith("10:")  # Alice starts
        assert result[3].startswith("15:")  # Alice ends

    def test_same_day_preserves_user_order(self):
        s = make_scheduler({"start": "Welcome"})
        users = [
            {"name": "Alice", "plan": "Basic", "account_date": 1, "duration": 10},
            {"name": "Bob",   "plan": "Pro",   "account_date": 1, "duration": 10},
        ]
        result = s.schedule(users)
        assert result[0].endswith("(Basic)")
        assert result[1].endswith("(Pro)")

    def test_negative_offset_computed_from_end(self):
        s = make_scheduler({-7: "Almost Done"})
        users = [{"name": "Alice", "plan": "Basic", "account_date": 1, "duration": 20}]
        # end=21, warning=21-7=14
        result = s.schedule(users)
        assert result == ["14: [Almost Done] Subscription for Alice (Basic)"]

    def test_empty_users(self):
        assert make_scheduler().schedule([]) == []


# ---------------------------------------------------------------------------
# Part 2: Plan changes
# ---------------------------------------------------------------------------

class TestPart2:
    def test_plan_change_updates_plan_name(self):
        s = make_scheduler({"start": "Welcome", "end": "Expired"})
        users = [{"name": "Alice", "plan": "Basic", "account_date": 1, "duration": 30}]
        changes = [{"name": "Alice", "new_plan": "Premium", "change_date": 10}]
        result = s.schedule(users, changes)
        assert "1: [Welcome] Subscription for Alice (Basic)" in result
        assert "10: [Changed] Subscription for Alice (Premium)" in result
        assert "31: [Expired] Subscription for Alice (Premium)" in result
        # Old expired (Basic) must NOT appear
        assert not any("Expired" in line and "Basic" in line for line in result)

    def test_plan_change_cancels_future_warnings(self):
        """Warning scheduled before change is kept; none duplicated after."""
        s = make_scheduler(SCHEDULE)
        users = [{"name": "Alice", "plan": "Basic", "account_date": 1, "duration": 30}]
        # end=31, warning=16; change at 20 → warning at 16 already past, not rescheduled
        changes = [{"name": "Alice", "new_plan": "Premium", "change_date": 20}]
        result = s.schedule(users, changes)
        expiry_lines = [l for l in result if "Expiry Warning" in l]
        assert len(expiry_lines) == 1
        assert "16:" in expiry_lines[0]
        assert "Basic" in expiry_lines[0]  # from original schedule, before change

    def test_plan_change_reschedules_warning_when_still_future(self):
        """If warning day > change day, it is rescheduled under the new plan."""
        s = make_scheduler(SCHEDULE)
        users = [{"name": "Alice", "plan": "Basic", "account_date": 1, "duration": 30}]
        # end=31, warning=16; change at 5 → warning still future → rescheduled as Premium
        changes = [{"name": "Alice", "new_plan": "Premium", "change_date": 5}]
        result = s.schedule(users, changes)
        expiry_lines = [l for l in result if "Expiry Warning" in l]
        assert len(expiry_lines) == 1
        assert "Premium" in expiry_lines[0]

    def test_changed_event_not_in_original_schedule(self):
        s = make_scheduler(SCHEDULE)
        users = [{"name": "Alice", "plan": "Basic", "account_date": 1, "duration": 30}]
        changes = [{"name": "Alice", "new_plan": "Premium", "change_date": 10}]
        result = s.schedule(users, changes)
        changed = [l for l in result if "[Changed]" in l]
        assert len(changed) == 1

    def test_multiple_changes_applied_in_order(self):
        s = make_scheduler({"start": "Welcome", "end": "Expired"})
        users = [{"name": "Alice", "plan": "Basic", "account_date": 1, "duration": 50}]
        changes = [
            {"name": "Alice", "new_plan": "Pro",     "change_date": 10},
            {"name": "Alice", "new_plan": "Premium", "change_date": 20},
        ]
        result = s.schedule(users, changes)
        changed = [l for l in result if "[Changed]" in l]
        assert len(changed) == 2
        # Final expired should be under Premium
        expired = [l for l in result if "[Expired]" in l]
        assert len(expired) == 1
        assert "Premium" in expired[0]

    def test_change_does_not_affect_other_users(self):
        s = make_scheduler({"start": "Welcome", "end": "Expired"})
        users = [
            {"name": "Alice", "plan": "Basic", "account_date": 1, "duration": 30},
            {"name": "Bob",   "plan": "Pro",   "account_date": 1, "duration": 30},
        ]
        changes = [{"name": "Alice", "new_plan": "Premium", "change_date": 10}]
        result = s.schedule(users, changes)
        bob_lines = [l for l in result if "Bob" in l]
        assert all("Pro" in l for l in bob_lines)
        assert not any("[Changed]" in l for l in bob_lines)


# ---------------------------------------------------------------------------
# Part 3: Renewals
# ---------------------------------------------------------------------------

class TestPart3:
    def test_renewal_extends_end_date(self):
        s = make_scheduler({"start": "Welcome", "end": "Expired"})
        users = [{"name": "Alice", "plan": "Basic", "account_date": 1, "duration": 30}]
        # original end=31; renew +10 on day 20 → new end=41
        changes = [{"name": "Alice", "extension": 10, "change_date": 20}]
        result = s.schedule(users, changes)
        expired = [l for l in result if "[Expired]" in l]
        assert len(expired) == 1
        assert "41:" in expired[0]

    def test_renewal_adds_renewed_event(self):
        s = make_scheduler(SCHEDULE)
        users = [{"name": "Alice", "plan": "Basic", "account_date": 1, "duration": 30}]
        changes = [{"name": "Alice", "extension": 10, "change_date": 20}]
        result = s.schedule(users, changes)
        renewed = [l for l in result if "[Renewed]" in l]
        assert len(renewed) == 1
        assert "20:" in renewed[0]

    def test_renewal_reschedules_future_warning(self):
        s = make_scheduler(SCHEDULE)
        users = [{"name": "Alice", "plan": "Basic", "account_date": 1, "duration": 30}]
        # end=31, warning=16; renew +20 on day 20 → new end=51, new warning=51-15=36
        changes = [{"name": "Alice", "extension": 20, "change_date": 20}]
        result = s.schedule(users, changes)
        warnings = [l for l in result if "[Expiry Warning]" in l]
        # Old warning at 16 (before renewal) is kept; new warning at 36 added
        days = [int(l.split(":")[0]) for l in warnings]
        assert 16 in days
        assert 36 in days

    def test_renewal_keeps_same_plan(self):
        s = make_scheduler({"start": "Welcome", "end": "Expired"})
        users = [{"name": "Alice", "plan": "Basic", "account_date": 1, "duration": 30}]
        changes = [{"name": "Alice", "extension": 10, "change_date": 5}]
        result = s.schedule(users, changes)
        assert all("Basic" in l for l in result if "Alice" in l)

    def test_plan_change_then_renewal(self):
        """Plan change followed by renewal: plan is Premium, end extended."""
        s = make_scheduler({"start": "Welcome", "end": "Expired"})
        users = [{"name": "Alice", "plan": "Basic", "account_date": 1, "duration": 30}]
        changes = [
            {"name": "Alice", "new_plan": "Premium", "change_date": 10},
            {"name": "Alice", "extension": 10,       "change_date": 20},
        ]
        result = s.schedule(users, changes)
        expired = [l for l in result if "[Expired]" in l]
        assert len(expired) == 1
        assert "Premium" in expired[0]
        assert "41:" in expired[0]  # 31 + 10

    def test_renewal_then_plan_change(self):
        """Renewal followed by plan change: end extended, then plan updated."""
        s = make_scheduler({"start": "Welcome", "end": "Expired"})
        users = [{"name": "Alice", "plan": "Basic", "account_date": 1, "duration": 30}]
        changes = [
            {"name": "Alice", "extension": 10,       "change_date": 10},
            {"name": "Alice", "new_plan": "Premium", "change_date": 20},
        ]
        result = s.schedule(users, changes)
        expired = [l for l in result if "[Expired]" in l]
        assert len(expired) == 1
        assert "Premium" in expired[0]
        assert "41:" in expired[0]  # 31 + 10
