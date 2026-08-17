import sys

from .gap_filler import fill_gaps, format_output
from .parser import parse_input


def main() -> None:
    text = sys.stdin.read()
    bin_str, intervals = parse_input(text)
    result = fill_gaps(bin_str, intervals)
    print("\n".join(format_output(result)))


if __name__ == "__main__":
    main()
