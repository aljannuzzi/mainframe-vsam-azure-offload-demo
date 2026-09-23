from __future__ import annotations

import argparse
from pathlib import Path

from .copybook import pack_comp3, parse_copybook, record_length


SAMPLE_ROWS = [
    {"account_id": "000000100001", "branch": 1601, "current_balance": "12450.70", "available_limit": "8000.00", "last_txn_date": 20260923, "sequence_number": 1000000001, "status": "A"},
    {"account_id": "000000100002", "branch": 1601, "current_balance": "50.25", "available_limit": "1200.00", "last_txn_date": 20260923, "sequence_number": 1000000002, "status": "A"},
    {"account_id": "000000100003", "branch": 2304, "current_balance": "-15.10", "available_limit": "300.00", "last_txn_date": 20260923, "sequence_number": 1000000003, "status": "B"},
    {"account_id": "000000100004", "branch": 4412, "current_balance": "982345.99", "available_limit": "0.00", "last_txn_date": 20260923, "sequence_number": 1000000004, "status": "A"},
]


def build_record(row: dict[str, object]) -> bytes:
    """Build the sample layout without truncation or lossy numeric coercion.

    Text must be nonblank cp037 strings fitting its width (status: one
    character). Zoned fields accept unsigned integers or ASCII digit strings
    within their PIC widths; date is the raw eight-digit source value.
    """
    required = {
        "account_id", "branch", "current_balance", "available_limit",
        "last_txn_date", "sequence_number", "status",
    }
    missing = required - row.keys()
    if missing:
        raise ValueError(f"Missing sample fields: {', '.join(sorted(missing))}")

    def text(name: str, width: int) -> bytes:
        value = row[name]
        if not isinstance(value, str) or not value.strip() or len(value) > width:
            raise ValueError(f"{name} must be nonblank text of at most {width} characters")
        try:
            return value.ljust(width).encode("cp037")
        except UnicodeEncodeError as exc:
            raise ValueError(f"{name} must be encodable in cp037") from exc

    def zoned(name: str, width: int) -> bytes:
        value = row[name]
        if type(value) is not int and not isinstance(value, str):
            raise ValueError(f"{name} must be an unsigned integer or ASCII digit string")
        value = str(value)
        if not value or len(value) > width or any(char not in "0123456789" for char in value):
            raise ValueError(f"{name} must contain at most {width} unsigned ASCII digits")
        return value.zfill(width).encode("cp037")

    parts = [
        text("account_id", 12),
        zoned("branch", 4),
        pack_comp3(str(row["current_balance"]), digits=15, scale=2),
        pack_comp3(str(row["available_limit"]), digits=15, scale=2),
        zoned("last_txn_date", 8),
        zoned("sequence_number", 10),
        text("status", 1),
    ]
    return b"".join(parts)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a VSAM-like fixed-length EBCDIC sample file.")
    parser.add_argument("--copybook", default="samples/copybooks/ACCOUNT_BALANCE.cbl")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    fields = parse_copybook(args.copybook)
    expected_length = record_length(fields)
    records = [build_record(row) for row in SAMPLE_ROWS]
    for index, record in enumerate(records, start=1):
        if len(record) != expected_length:
            raise ValueError(f"Generated record {index} has {len(record)} bytes; expected {expected_length}")

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_bytes(b"".join(records))
    print(f"Wrote {len(records)} records ({expected_length} bytes each) to {output}")


if __name__ == "__main__":
    main()
