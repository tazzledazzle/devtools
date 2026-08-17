import sys

from .checker import NameChecker
from .parser import parse_input


def main() -> None:
    text = sys.stdin.read()
    events = parse_input(text)
    checker = NameChecker()
    results = checker.run(events)
    print("\n".join(results))


if __name__ == "__main__":
    main()
