"""Synthetic capture-adapter contract, NOT native VSAM CDC or VSAM sequence numbers."""
from __future__ import annotations

import base64
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from .generate_sample import build_record

DEFAULT_COPYBOOK = Path(__file__).resolve().parents[2] / "samples" / "copybooks" / "ACCOUNT_BALANCE.cbl"
SOURCE = "VSAM_ACCOUNT_BALANCE"
EPOCH = "demo-epoch-1"


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def fingerprint(value: object) -> str:
    return hashlib.sha256(canonical(value).encode("utf-8")).hexdigest()


def validate_envelope(event: object, expected_epoch: str) -> None:
    if not isinstance(event, dict):
        raise ValueError("envelope must be a JSON object")
    if type(event.get("schemaVersion")) is not int or event["schemaVersion"] != 1:
        raise ValueError("unsupported schemaVersion")
    for key, expected in (("source", SOURCE), ("sourceEpoch", expected_epoch),
                          ("copybookId", "ACCOUNT_BALANCE_V1"), ("codePage", "cp037")):
        if event.get(key) != expected:
            raise ValueError(f"unsupported {key}")
    if event.get("committed") is not True:
        raise ValueError("capture adapter must assert committed=true")
    for key in ("eventId", "sourcePosition", "sourceEpoch", "accountId", "committedAt"):
        if not isinstance(event.get(key), str) or not event[key].strip():
            raise ValueError(f"missing or invalid {key}")
    account = event["accountId"]
    if len(account) > 12 or account != account.strip() or any(c in account for c in "/\\?#"):
        raise ValueError("invalid accountId")
    for key, minimum in (("sourceVersion", 1), ("previousVersion", 0)):
        if type(event.get(key)) is not int or event[key] < minimum:
            raise ValueError(f"invalid {key}")
    if event["previousVersion"] >= event["sourceVersion"]:
        raise ValueError("sourceVersion must exceed previousVersion")
    try:
        stamp = datetime.fromisoformat(event["committedAt"].replace("Z", "+00:00"))
        if stamp.utcoffset() is None or stamp.utcoffset().total_seconds() != 0:
            raise ValueError("not UTC")
    except ValueError as exc:
        raise ValueError("committedAt must be a UTC ISO timestamp") from exc
    if event.get("operation") not in ("UPSERT", "DELETE"):
        raise ValueError("unsupported operation")
    if "recordBase64" not in event:
        raise ValueError("missing recordBase64")
    raw = event["recordBase64"]
    if event["operation"] == "DELETE":
        if raw not in (None, ""):
            raise ValueError("DELETE must not contain a record")
    elif not isinstance(raw, str) or not raw:
        raise ValueError("UPSERT requires recordBase64")
    canonical(event)


def make_event(row: dict, version: int, previous_version: int, *, operation: str = "UPSERT",
               source_epoch: str = EPOCH, event_id: str | None = None,
               committed_at: str | None = None) -> dict:
    """Build once, then retain/retry the entire envelope, including its commit timestamp.

    Versions and sourcePosition are supplied/synthesized by an adapter in a fixed epoch.
    Neither is an Event Hubs offset. A real adapter must provide a committed source cursor.
    """
    copied = dict(row, sequence_number=version)
    event = {
        "schemaVersion": 1, "source": SOURCE, "sourceEpoch": source_epoch,
        "sourcePosition": f"synthetic:{source_epoch}:{row['account_id']}:{version}",
        "accountId": str(row["account_id"]), "operation": operation,
        "sourceVersion": version, "previousVersion": previous_version,
        "committed": True, "copybookId": "ACCOUNT_BALANCE_V1", "codePage": "cp037",
        "recordBase64": None if operation == "DELETE" else base64.b64encode(build_record(copied)).decode("ascii"),
    }
    event["eventId"] = event_id if event_id is not None else fingerprint(event)
    event["committedAt"] = committed_at if committed_at is not None else datetime.now(timezone.utc).isoformat()
    validate_envelope(event, source_epoch)
    return event
