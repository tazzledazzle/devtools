import sys

from .validator import validate_card


def main() -> None:
    for line in sys.stdin:
        results = validate_card(line.strip())
        for result in results:
            print(result)


if __name__ == "__main__":
    main()
