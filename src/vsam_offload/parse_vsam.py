from __future__ import annotations

import argparse
import json
from pathlib import Path

from .copybook import parse_copybook, parse_record, record_length


def iter_records(input_path: str | Path, record_size: int):
    with Path(input_path).open("rb") as stream:
        sequence = 0
        while chunk := stream.read(record_size):
            sequence += 1
            if len(chunk) != record_size:
                raise ValueError(f"Trailing partial record at sequence {sequence}: {len(chunk)} bytes")
            yield sequence, chunk


def normalize_document(record: dict[str, object], source_sequence: int) -> dict[str, object]:
    account_id = str(record["account_id"])
    return {
        "id": account_id,
        "accountId": account_id,
        "branch": f"{int(record['branch']):04d}",
        "currentBalance": record["current_balance"],
        "availableLimit": record["available_limit"],
        "lastTransactionDate": str(record["last_txn_date"]),
        "mainframeSequence": int(record["sequence_number"]),
        "sourceRecordNumber": source_sequence,
        "status": record["status"],
        "source": "VSAM_ACCOUNT_BALANCE",
    }


def convert_file(copybook: str | Path, input_path: str | Path, output_path: str | Path) -> int:
    fields = parse_copybook(copybook)
    size = record_length(fields)
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)

    count = 0
    with output.open("w", encoding="utf-8") as writer:
        for source_sequence, raw_record in iter_records(input_path, size):
            document = normalize_document(parse_record(raw_record, fields), source_sequence)
            writer.write(json.dumps(document, separators=(",", ":"), ensure_ascii=False) + "\n")
            count += 1

    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse a VSAM-like binary export to Cosmos-ready JSONL.")
    parser.add_argument("--copybook", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    count = convert_file(args.copybook, args.input, args.output)
    print(f"Wrote {count} documents to {args.output}")


if __name__ == "__main__":
    main()

