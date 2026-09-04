import pytest

from src.checker import NameChecker
from src.normalizer import normalize
from src.parser import CheckRequest, ReclaimRequest, parse_input


# ---------------------------------------------------------------------------
# Normalization unit tests
# ---------------------------------------------------------------------------

class TestNormalize:
    def test_lowercase(self):
        assert normalize("LLAMA Inc.") == normalize("llama Inc.")

    def test_ampersand_as_space(self):
        assert normalize("Foo & Bar") == "foo bar"

    def test_comma_as_space(self):
        assert normalize("Foo, Bar") == "foo bar"

    def test_collapses_spaces(self):
        assert normalize("Foo   Bar") == "foo bar"

    def test_suffix_inc(self):
        assert normalize("Llama, Inc.") == "llama"

    def test_suffix_corp(self):
        assert normalize("Llama Corp.") == "llama"

    def test_suffix_llc(self):
        assert normalize("Llama LLC") == "llama"

    def test_suffix_llc_dot(self):
        assert normalize("Llama LLC.") == "llama"

    def test_suffix_llc_periods(self):
        assert normalize("Llama L.L.C.") == "llama"

    def test_stacked_suffixes(self):
        assert normalize("Llama Inc. LLC") == "llama"

    def test_leading_the(self):
        assert normalize("The Llama, Inc.") == normalize("Llama, Inc.")

    def test_leading_an(self):
        assert normalize("An Llama Corp.") == normalize("Llama Corp.")

    def test_leading_a(self):
        assert normalize("A Llama LLC") == normalize("Llama LLC")

    def test_and_in_middle_removed(self):
        assert normalize("Llama And Friend, Inc.") == normalize("Llama Friend, Inc.")

    def test_and_at_start_kept(self):
        assert normalize("And Llama Friend, Inc.") != normalize("Llama Friend, Inc.")
        assert normalize("And Llama Friend, Inc.") == "and llama friend"

    def test_the_then_and_at_start(self):
        # "The And Llama" → strip "The" → "And Llama" → "And" is now first → keep
        assert normalize("The And Llama, Inc.") == "and llama"

    def test_multiple_and_in_name(self):
        assert normalize("Llama And And Friend") == "llama friend"

    def test_empty_after_normalization(self):
        assert normalize("Inc.") == ""
        assert normalize("LLC") == ""
        assert normalize("The, Inc.") == ""

    def test_empty_string(self):
        assert normalize("") == ""

    def test_only_spaces(self):
        assert normalize("   ") == ""

    def test_case_insensitive_suffix(self):
        assert normalize("Llama INC.") == "llama"
        assert normalize("Llama inc.") == "llama"

    def test_case_insensitive_article(self):
        assert normalize("THE Llama") == "llama"

    def test_case_insensitive_and(self):
        assert normalize("Llama AND Friend") == "llama friend"

    def test_problem_statement_example(self):
        assert normalize("Llama, Inc.") == normalize("LLAMA, Inc.")
        assert normalize("Llama, Inc.") == normalize("The Llama, Inc.")
        assert normalize("Llama And Friend, Inc.") == normalize("Llama Friend, Inc.")
        assert normalize("And Llama Friend, Inc.") != normalize("Llama Friend, Inc.")


# ---------------------------------------------------------------------------
# NameChecker — availability and registration
# ---------------------------------------------------------------------------

class TestNameChecker:
    def setup_method(self):
        self.checker = NameChecker()

    def _check(self, account_id, name):
        return self.checker.check(account_id, name)

    def test_new_name_available(self):
        assert self._check("acct_1", "Llama Inc.") == "acct_1|Name Available"

    def test_same_name_unavailable_after_registration(self):
        self._check("acct_1", "Llama Inc.")
        assert self._check("acct_2", "Llama Inc.") == "acct_2|Name Not Available"

    def test_normalized_equivalent_unavailable(self):
        self._check("acct_1", "Llama Inc.")
        assert self._check("acct_2", "LLAMA, Inc.") == "acct_2|Name Not Available"

    def test_the_prefix_equivalent(self):
        self._check("acct_1", "Llama Inc.")
        assert self._check("acct_2", "The Llama Inc.") == "acct_2|Name Not Available"

    def test_and_removal_equivalent(self):
        self._check("acct_1", "Llama Friend Inc.")
        assert self._check("acct_2", "Llama And Friend Inc.") == "acct_2|Name Not Available"

    def test_and_at_start_is_distinct(self):
        self._check("acct_1", "Llama Friend Inc.")
        assert self._check("acct_2", "And Llama Friend Inc.") == "acct_2|Name Available"

    def test_empty_normalized_name_unavailable(self):
        assert self._check("acct_1", "Inc.") == "acct_1|Name Not Available"

    def test_empty_string_unavailable(self):
        assert self._check("acct_1", "") == "acct_1|Name Not Available"

    def test_same_account_cannot_reregister(self):
        self._check("acct_1", "Llama Inc.")
        assert self._check("acct_1", "Llama Inc.") == "acct_1|Name Not Available"

    def test_different_normalized_names_are_independent(self):
        self._check("acct_1", "Alpha Inc.")
        assert self._check("acct_2", "Beta Inc.") == "acct_2|Name Available"

    def test_sequential_registrations(self):
        results = [
            self._check("a1", "Alpha Corp."),
            self._check("a2", "Alpha LLC"),
            self._check("a3", "Beta Inc."),
        ]
        assert results == [
            "a1|Name Available",
            "a2|Name Not Available",  # same as Alpha Corp. after normalization
            "a3|Name Available",
        ]


# ---------------------------------------------------------------------------
# Part 3: Reclamation
# ---------------------------------------------------------------------------

class TestReclaim:
    def setup_method(self):
        self.checker = NameChecker()

    def test_reclaim_makes_name_available_again(self):
        self.checker.check("acct_1", "Llama Inc.")
        self.checker.reclaim("acct_1", "Llama Inc.")
        assert self.checker.check("acct_2", "Llama Inc.") == "acct_2|Name Available"

    def test_wrong_account_cannot_reclaim(self):
        self.checker.check("acct_1", "Llama Inc.")
        self.checker.reclaim("acct_2", "Llama Inc.")  # wrong account — should be ignored
        assert self.checker.check("acct_3", "Llama Inc.") == "acct_3|Name Not Available"

    def test_reclaim_nonexistent_name_ignored(self):
        self.checker.reclaim("acct_1", "Ghost Corp.")  # never registered — no error

    def test_reclaim_uses_normalized_name(self):
        self.checker.check("acct_1", "Llama Inc.")
        # Reclaim with a normalized-equivalent name
        self.checker.reclaim("acct_1", "THE LLAMA, INC.")
        assert self.checker.check("acct_2", "Llama Inc.") == "acct_2|Name Available"

    def test_re_registration_after_reclaim(self):
        self.checker.check("acct_1", "Llama Inc.")
        self.checker.reclaim("acct_1", "Llama Inc.")
        assert self.checker.check("acct_1", "Llama Inc.") == "acct_1|Name Available"

    def test_reclaim_then_block_new_requester(self):
        self.checker.check("acct_1", "Llama Inc.")
        self.checker.reclaim("acct_1", "Llama Inc.")
        self.checker.check("acct_2", "Llama Inc.")
        assert self.checker.check("acct_3", "Llama Inc.") == "acct_3|Name Not Available"


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

class TestParser:
    def test_check_request(self):
        events = parse_input("acct_1|Llama Inc.\n")
        assert len(events) == 1
        assert isinstance(events[0], CheckRequest)
        assert events[0].account_id == "acct_1"
        assert events[0].proposed_name == "Llama Inc."

    def test_reclaim_request(self):
        events = parse_input("RECLAIM,acct_1,Llama Inc.\n")
        assert len(events) == 1
        assert isinstance(events[0], ReclaimRequest)
        assert events[0].account_id == "acct_1"
        assert events[0].original_proposed_name == "Llama Inc."

    def test_mixed_events(self):
        text = "acct_1|Llama Inc.\nRECLAIM,acct_1,Llama Inc.\nacct_2|Llama Inc.\n"
        events = parse_input(text)
        assert len(events) == 3
        assert isinstance(events[0], CheckRequest)
        assert isinstance(events[1], ReclaimRequest)
        assert isinstance(events[2], CheckRequest)
