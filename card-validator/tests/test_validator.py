import pytest

from src.luhn import luhn_check, luhn_sum
from src.networks import detect_network
from src.validator import classify, classify_corrupted, classify_redacted


# ---------------------------------------------------------------------------
# Luhn algorithm
# ---------------------------------------------------------------------------

class TestLuhn:
    def test_known_valid_visa(self):
        assert luhn_check("4532015112830366") is True

    def test_problem_example_sum_is_50(self):
        assert luhn_sum("4532015112830366") == 50

    def test_invalid_checksum(self):
        assert luhn_check("4242424242424243") is False

    def test_known_valid_amex(self):
        assert luhn_check("378282246310005") is True

    def test_known_valid_mastercard(self):
        assert luhn_check("5105105105105100") is True

    def test_all_zeros_passes_luhn(self):
        # All-zero sum = 0, 0 % 10 == 0 → Luhn passes (network detection rejects it)
        assert luhn_check("0000000000000000") is True

    def test_single_digit_zero(self):
        assert luhn_check("0") is True

    def test_off_by_one_fails(self):
        valid = "4532015112830366"
        # Change last digit by 1
        bad = valid[:-1] + str((int(valid[-1]) + 1) % 10)
        assert luhn_check(bad) is False


# ---------------------------------------------------------------------------
# Network detection
# ---------------------------------------------------------------------------

class TestDetectNetwork:
    def test_visa_16_digits(self):
        assert detect_network("4532015112830366") == "VISA"

    def test_mastercard_51(self):
        assert detect_network("5105105105105100") == "MASTERCARD"

    def test_mastercard_55(self):
        assert detect_network("5500005555555559") == "MASTERCARD"

    def test_amex_34(self):
        assert detect_network("341111111111111") == "AMEX"

    def test_amex_37(self):
        assert detect_network("378282246310005") == "AMEX"

    def test_wrong_length_visa_prefix(self):
        assert detect_network("453201511283036") is None  # 15 digits, starts with 4

    def test_prefix_56_unknown(self):
        assert detect_network("562523343010901") is None

    def test_13_digits_unknown(self):
        assert detect_network("5482334509943") is None

    def test_mastercard_50_prefix_unknown(self):
        assert detect_network("5042424242424242") is None

    def test_mastercard_56_prefix_unknown(self):
        assert detect_network("5642424242424242") is None


# ---------------------------------------------------------------------------
# Parts 1 & 2: classify
# ---------------------------------------------------------------------------

class TestClassify:
    def test_valid_visa(self):
        assert classify("4532015112830366") == "VISA"

    def test_invalid_checksum_visa_prefix(self):
        assert classify("4242424242424243") == "INVALID_CHECKSUM"

    def test_valid_mastercard(self):
        assert classify("5105105105105100") == "MASTERCARD"

    def test_valid_amex(self):
        assert classify("378282246310005") == "AMEX"

    def test_unknown_network_13_digits(self):
        assert classify("5482334509943") == "UNKNOWN_NETWORK"

    def test_unknown_network_prefix_56(self):
        assert classify("562523343010901") == "UNKNOWN_NETWORK"

    def test_invalid_checksum_mastercard(self):
        # Valid prefix/length, bad Luhn
        assert classify("5105105105105101") == "INVALID_CHECKSUM"

    def test_invalid_checksum_amex(self):
        assert classify("378282246310006") == "INVALID_CHECKSUM"

    def test_16_digit_non_visa_non_mc(self):
        assert classify("6011111111111117") == "UNKNOWN_NETWORK"


# ---------------------------------------------------------------------------
# Part 3: Redacted ('*')
# ---------------------------------------------------------------------------

class TestClassifyRedacted:
    def test_single_star_visa_one_valid(self):
        # "4242424242424*42" — only one digit at position 13 makes it valid
        result = classify_redacted("4242424242424*42")
        assert result == ["VISA,1"]

    def test_amex_star_in_prefix(self):
        # "3*8282246310005" — only 34 and 37 are valid AMEX prefixes
        result = classify_redacted("3*8282246310005")
        # Both "348282246310005" and "378282246310005" need luhn check
        assert "AMEX" in result[0]
        count = int(result[0].split(",")[1])
        assert 1 <= count <= 2

    def test_no_valid_cards_returns_empty(self):
        # Impossible to make valid: all-zero 16-digit with one star can't produce
        # a network match if none of the prefixes match
        result = classify_redacted("6000000000000*00")
        assert result == []

    def test_multiple_stars_count_unique_valids(self):
        # Two stars — 100 combinations, count should be deterministic
        result = classify_redacted("4242424242424**2")
        assert len(result) >= 1
        network, count = result[0].split(",")
        assert network == "VISA"
        assert int(count) > 0

    def test_sorted_alphabetically_by_network(self):
        # Cards that might produce multiple networks — check sort order
        result = classify_redacted("**42424242424242")
        network_names = [r.split(",")[0] for r in result]
        assert network_names == sorted(network_names)

    def test_star_count_consistency(self):
        # One star → at most 10 valid cards
        result = classify_redacted("453201511283036*")
        if result:
            for line in result:
                count = int(line.split(",")[1])
                assert count <= 10


# ---------------------------------------------------------------------------
# Part 4: Corrupted ('?')
# ---------------------------------------------------------------------------

class TestClassifyCorrupted:
    def test_output_sorted_numerically(self):
        results = classify_corrupted("4532015112830366?")
        card_numbers = [r.split(",")[0] for r in results]
        assert card_numbers == sorted(card_numbers, key=int)

    def test_output_format(self):
        results = classify_corrupted("4532015112830366?")
        for line in results:
            parts = line.split(",")
            assert len(parts) == 2
            card, network = parts
            assert card.isdigit()
            assert network in {"VISA", "MASTERCARD", "AMEX"}

    def test_valid_original_included(self):
        # Known-valid card: if input is already valid, it should appear in output
        valid = "4532015112830366"
        results = classify_corrupted(valid + "?")
        cards = [r.split(",")[0] for r in results]
        assert valid in cards

    def test_all_results_are_valid(self):
        results = classify_corrupted("4532015112830366?")
        for line in results:
            card, network = line.split(",")
            assert classify(card) == network

    def test_adjacent_swap_recovered(self):
        # Swap two adjacent digits in a known-valid card, then check it can be recovered
        original = "4532015112830366"
        # Swap positions 3 and 4 (digits '2' and '0')
        corrupted = original[:3] + original[4] + original[3] + original[5:] + "?"
        results = classify_corrupted(corrupted)
        cards = [r.split(",")[0] for r in results]
        assert original in cards

    def test_single_digit_error_recovered(self):
        # Change one digit, ensure original is among recovered candidates
        original = "4532015112830366"
        corrupted = original[:5] + "9" + original[6:] + "?"
        results = classify_corrupted(corrupted)
        cards = [r.split(",")[0] for r in results]
        assert original in cards

    def test_amex_corrupted(self):
        valid_amex = "378282246310005"
        results = classify_corrupted(valid_amex + "?")
        cards = [r.split(",")[0] for r in results]
        assert valid_amex in cards
        for line in results:
            _, network = line.split(",")
            assert network == "AMEX"

    def test_no_results_for_unsalvageable_card(self):
        # A card where no single change or swap produces a valid card
        # All-zeros with wrong length → no network match → empty
        results = classify_corrupted("00000000000000000?")  # 17 digits before ?
        assert results == []
