"""Exercise synchronization controls with a synthetic capture adapter and local SQLite."""
from __future__ import annotations

import argparse
import base64
import json
import math
import time
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from .generate_sample import SAMPLE_ROWS
from .parse_vsam import normalize_document
from .sync_contract import canonical, fingerprint, make_event
from .sync_engine import SimulatedCrash, SyncEngine
from .sync_store import SQLiteStore


def financial_view(document: dict) -> dict:
    view = {key: document[key] for key in ("accountId", "sourceVersion", "sourceEpoch", "deleted")}
    if not document["deleted"]:
        for key in ("branch", "lastTransactionDate", "status"):
            view[key] = document[key]
        for key in ("currentBalance", "availableLimit"):
            amount = Decimal(document[key])
            if not amount.is_finite() or amount != amount.quantize(Decimal("0.01")):
                raise ValueError(f"{key} is not an exact finite cents amount")
            view[key] = format(amount if amount else abs(amount), ".2f")
    return view


def reconcile(source: dict[str, dict], documents: list[dict], watermark: str) -> dict:
    """Compare an independently maintained source image with a quiesced sink at one boundary."""
    expected = {key: financial_view(value) for key, value in source.items()}
    actual = {doc["accountId"]: financial_view(doc) for doc in documents}

    def summary(rows):
        ordered = [rows[key] for key in sorted(rows)]
        return {"count": sum(not row["deleted"] for row in ordered), "keyCountIncludingTombstones": len(ordered),
                "hash": fingerprint(ordered), "perKey": {key: rows[key] for key in sorted(rows)}}

    mismatches = [key for key in sorted(expected.keys() | actual.keys()) if expected.get(key) != actual.get(key)]
    return {"sourceWatermark": watermark, "consistentBoundary": "synthetic source paused; sink drained",
            "equal": not mismatches, "mismatches": mismatches, "source": summary(expected), "sink": summary(actual)}


class DemoStore(SQLiteStore):
    fail_next = False

    def apply(self, key, transition):
        if self.fail_next:
            self.fail_next = False
            raise ConnectionError("injected transient destination failure")
        return super().apply(key, transition)


def run_demo(output: Path, interval_seconds: float = 0.2) -> dict:
    if not math.isfinite(interval_seconds) or interval_seconds < 0:
        raise ValueError("interval-seconds must be finite and nonnegative")
    output = Path(output)
    output.mkdir(parents=True, exist_ok=False)
    store = DemoStore(output / "sync.sqlite")
    engine = SyncEngine(store)
    source, checks, steps = {}, [], []
    offset = 0
    stream = (output / "events.jsonl").open("w", encoding="utf-8")
    raw_stream = (output / "source-records.bin").open("wb")

    def check(label, passed):
        checks.append({"invariant": label, "passed": bool(passed)})

    def capture(row, version, previous, operation="UPSERT"):
        event = make_event(row, version, previous, operation=operation)
        if operation == "UPSERT":
            raw_stream.write(base64.b64decode(event["recordBase64"]))
            image = normalize_document(dict(row, sequence_number=version), version)
        else:
            image = {"accountId": row["account_id"]}
        image.update(sourceVersion=version, sourceEpoch=event["sourceEpoch"], deleted=operation == "DELETE")
        source[event["accountId"]] = image
        return event

    def deliver(label, event, expected, crash=False, redelivery=False):
        nonlocal offset
        if not redelivery:
            offset += 1
        stream.write(canonical(event) + "\n")
        stream.flush()
        time.sleep(interval_seconds)
        before = store.checkpoint("0")
        try:
            result = engine.process(event, "0", offset, fail_after_apply=crash)
            outcome = result.outcome
        except (SimulatedCrash, ConnectionError) as exc:
            outcome = "crash" if isinstance(exc, SimulatedCrash) else "transient-failure"
            check(f"{label}: checkpoint unchanged", before == store.checkpoint("0"))
            store.audit({"phase": "failure", "outcome": outcome, "checkpointBefore": before,
                         "checkpointAfter": store.checkpoint("0"), "transportOffset": str(offset)})
        check(label, outcome == expected)
        steps.append({"step": label, "outcome": outcome, "checkpointBefore": before,
                      "checkpointAfter": store.checkpoint("0"), "transportOffset": str(offset)})
        print(f"{label}: {outcome}")

    try:
        a, b = dict(SAMPLE_ROWS[0]), dict(SAMPLE_ROWS[1])
        seed = capture(a, 1, 0)
        deliver("snapshot seed A (synthetic captured boundary)", seed, "applied")
        deliver("snapshot seed B (same synthetic boundary)", capture(b, 1, 0), "applied")
        a["current_balance"] = "12451.71"
        update = capture(a, 3, 1)
        deliver("update with non-contiguous numeric version", update, "applied")
        deliver("duplicate", update, "duplicate")
        deliver("stale", seed, "stale")
        a["available_limit"] = "7999.99"
        interrupted = capture(a, 4, 3)
        deliver("crash after sink apply before checkpoint", interrupted, "crash", crash=True)
        store.close()
        store = DemoStore(output / "sync.sqlite")
        engine = SyncEngine(store)
        deliver("reopen and resume replay", interrupted, "duplicate", redelivery=True)
        predecessor = capture(a, 7, 4)
        a["current_balance"] = "-15.10"
        gap = capture(a, 8, 7)
        deliver("out-of-order gap", gap, "quarantined")
        deliver("missing predecessor", predecessor, "applied")
        deliver("gap replay after predecessor", gap, "applied")
        store.resolve_quarantine(gap)
        corrupt = make_event(a, 9, 8)
        record = bytearray(base64.b64decode(corrupt["recordBase64"]))
        record[23] = 0xA0 | (record[23] & 0x0F)
        corrupt["recordBase64"] = base64.b64encode(record).decode("ascii")
        deliver("corrupt COMP-3 quarantine", corrupt, "quarantined")
        deliver("schema quarantine", dict(make_event(a, 9, 8), schemaVersion=2), "quarantined")
        b["current_balance"] = "51.26"
        transient = capture(b, 3, 1)
        store.fail_next = True
        deliver("transient sink failure", transient, "transient-failure")
        deliver("retry transient sink failure", transient, "applied", redelivery=True)
        deliver("delete tombstone", capture(a, 10, 8, "DELETE"), "applied")
        deliver("stale resurrection blocked", update, "stale")
        b["available_limit"] = "1250.05"
        dropped = capture(b, 5, 3)
        # Retain the intentionally undelivered capture for operator replay.
        (output / "dropped-event.json").write_text(canonical(dropped), encoding="utf-8")
        boundary = "synthetic-paused-boundary-A10-B5"
        before_replay = reconcile(source, store.documents(), boundary)
        check("reconciliation detects dropped event", before_replay["mismatches"] == [b["account_id"]])
        deliver("reconciliation repair replay", dropped, "applied")
        after_replay = reconcile(source, store.documents(), boundary)
        check("canonical financial count/hash and per-key versions equal", after_replay["equal"])
        pending = [item for item in store.quarantines() if not item["resolved"]]
        check("poison remains visible, gap explicitly resolved", len(pending) == 2)
        report = {
            "invariantsPassed": all(item["passed"] for item in checks),
            "status": "attention-required" if pending else "reconciled",
            "scope": "balances only; synthetic committed capture, not native VSAM CDC",
            "latencyScope": "local processing measurements only; excludes z/OS capture and network; no SLA asserted",
            "captureBoundary": boundary, "checks": checks, "steps": steps,
            "reconciliationBeforeReplay": before_replay, "reconciliationAfterReplay": after_replay,
            "unresolvedQuarantine": pending, "quarantineCount": len(store.quarantines()),
            "transportCheckpoint": store.checkpoint("0"),
            "guarantees": "at-least-once replay with per-key version CAS; no exactly-once or distributed transaction",
        }
        (output / "timeline.jsonl").write_text("".join(canonical(item) + "\n" for item in store.timeline()), encoding="utf-8")
        (output / "report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
        return report
    finally:
        raw_stream.close()
        stream.close()
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=Path("out") / "sync-demo")
    parser.add_argument("--interval-seconds", type=float, default=0.2)
    parser.add_argument("--new-run", action="store_true", help="Create a unique timestamped child, never overwrite")
    args = parser.parse_args()
    output = args.output
    if args.new_run:
        output /= datetime.now(timezone.utc).strftime("run-%Y%m%dT%H%M%S%fZ")
    try:
        report = run_demo(output, args.interval_seconds)
    except (FileExistsError, ValueError) as exc:
        parser.error(str(exc))
    print(f"Report: {output / 'report.json'}; {report['status']}")
    raise SystemExit(0 if report["invariantsPassed"] else 1)


if __name__ == "__main__":
    main()
