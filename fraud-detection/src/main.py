import sys

from .detector import CountBasedDetector, DisputeAwareDetector, PercentageBasedDetector, run
from .parser import parse_input

PARTS = {
    "1": CountBasedDetector,
    "2": PercentageBasedDetector,
    "3": DisputeAwareDetector,
}


def main() -> None:
    if len(sys.argv) < 2 or sys.argv[1] not in PARTS:
        print("Usage: python -m src.main <part>  (part = 1, 2, or 3)", file=sys.stderr)
        sys.exit(1)

    text = sys.stdin.read()
    parsed = parse_input(text)
    result = run(parsed, PARTS[sys.argv[1]])
    print(",".join(result))


if __name__ == "__main__":
    main()
