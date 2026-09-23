from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from .copybook import parse_copybook, parse_record, record_length
from .sync_contract import make_event


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Legacy synthetic snapshot splitter, not VSAM CDC. Use sync_demo for failure/replay scenarios."
    )
    parser.add_argument("--copybook", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--events", required=True)
    parser.add_argument("--landing", required=True)
    parser.add_argument("--batch-size", type=int, default=2)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    args = parser.parse_args()
    if args.batch_size <= 0 or args.interval_seconds < 0:
        parser.error("batch-size must be positive and interval-seconds nonnegative")

    fields = parse_copybook(args.copybook)
    size = record_length(fields)
    data = Path(args.input).read_bytes()
    if len(data) % size:
        raise ValueError(f"Input length {len(data)} is not divisible by record length {size}")

    landing = Path(args.landing)
    landing.mkdir(parents=True, exist_ok=True)
    events = Path(args.events)
    events.parent.mkdir(parents=True, exist_ok=True)

    records = [data[index : index + size] for index in range(0, len(data), size)]
    with events.open("w", encoding="utf-8") as writer:
        for batch_start in range(0, len(records), args.batch_size):
            batch = records[batch_start : batch_start + args.batch_size]
            batch_id = (batch_start // args.batch_size) + 1
            batch_path = landing / f"vsam-batch-{batch_id:04d}.bin"
            batch_path.write_bytes(b"".join(batch))

            for offset, raw_record in enumerate(batch, start=1):
                source_sequence = batch_start + offset
                row = parse_record(raw_record, fields)
                event = make_event(row, int(row["sequence_number"]), 0)
                writer.write(json.dumps(event, separators=(",", ":"), ensure_ascii=False) + "\n")
                writer.flush()

            print(f"Published batch {batch_id} with {len(batch)} records to {batch_path}")
            if batch_start + args.batch_size < len(records):
                time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
