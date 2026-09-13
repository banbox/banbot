#!/usr/bin/env python3
"""Write a stable CSV representation without splitting quoted records."""

import csv
import sys


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} ORDERS_CSV", file=sys.stderr)
        return 2
    with open(sys.argv[1], newline="", encoding="utf-8") as source:
        rows = list(csv.reader(source))
    if not rows:
        return 0
    writer = csv.writer(sys.stdout, lineterminator="\n")
    writer.writerow(rows[0])
    writer.writerows(sorted(rows[1:]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
