#!/usr/bin/env python3
import csv
import io
import pathlib
import subprocess
import sys
import tempfile


ROOT = pathlib.Path(__file__).resolve().parent
NORMALIZER = ROOT / "normalize_backtest_orders.py"


def main() -> int:
    with tempfile.TemporaryDirectory() as temp_dir:
        source = pathlib.Path(temp_dir) / "orders.csv"
        with source.open("w", newline="", encoding="utf-8") as output:
            writer = csv.writer(output)
            writer.writerow(["id", "note"])
            writer.writerow(["2", "quoted, field"])
            writer.writerow(["1", "two lines\nretain one record"])
        result = subprocess.run(
            [sys.executable, str(NORMALIZER), str(source)],
            check=True,
            capture_output=True,
            text=True,
        )
    rows = list(csv.reader(io.StringIO(result.stdout)))
    assert rows == [
        ["id", "note"],
        ["1", "two lines\nretain one record"],
        ["2", "quoted, field"],
    ], rows
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
