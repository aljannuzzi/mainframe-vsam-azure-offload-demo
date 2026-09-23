from __future__ import annotations

import base64
import binascii
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from .copybook import parse_copybook, parse_record
from .parse_vsam import normalize_document
from .sync_contract import DEFAULT_COPYBOOK, EPOCH, SOURCE, fingerprint, validate_envelope
from .sync_store import SQLiteStore, Store


class SimulatedCrash(RuntimeError):
    pass


@dataclass(frozen=True)
class ProcessResult:
    outcome: str
    reason: str | None = None
    quarantine_id: str | None = None
    checkpoint_safe: bool = True


class SyncEngine:
    """Serial per-partition delivery; per-key CAS protects concurrent sink writers.

    SQLite checkpoints emulate a local transport. Cosmos processing never writes an
    Event Hubs checkpoint: the consumer must do that only after this call succeeds.
    """

    def __init__(self, store: Store, copybook=DEFAULT_COPYBOOK, expected_epoch: str = EPOCH,
                 *, stream: str = "local-demo", group: str = "sync-demo"):
        self.store, self.fields, self.expected_epoch = store, parse_copybook(copybook), expected_epoch
        self.stream = store.stream if isinstance(store, SQLiteStore) else stream
        self.group = store.group if isinstance(store, SQLiteStore) else group

    def _transport(self, partition, offset) -> dict:
        return {"stream": self.stream, "group": self.group, "partition": str(partition), "offset": str(offset)}

    def _finish(self, result, event, partition, offset, started, fail_after_apply=False):
        if isinstance(self.store, SQLiteStore):
            before = self.store.checkpoint(str(partition))
            self.store.audit({"eventId": event.get("eventId") if isinstance(event, dict) else None,
                              "transport": self._transport(partition, offset), "outcome": result.outcome,
                              "reason": result.reason, "checkpointBefore": before, "checkpointAfter": before,
                              "phase": "sink-durable", "localProcessingSeconds": time.perf_counter() - started})
        if fail_after_apply:
            raise SimulatedCrash("sink durable; transport checkpoint not written")
        if isinstance(self.store, SQLiteStore):
            self.store.save_checkpoint(str(partition), offset)
            self.store.audit({"transport": self._transport(partition, offset), "outcome": result.outcome,
                              "phase": "checkpoint", "checkpointBefore": before,
                              "checkpointAfter": self.store.checkpoint(str(partition))})
        return result

    def poison(self, payload: object, partition: str, offset: int | str, reason: str) -> ProcessResult:
        started = time.perf_counter()
        identity = self.store.quarantine(payload, reason, self._transport(partition, offset))
        return self._finish(ProcessResult("quarantined", reason, identity), payload, partition, offset, started)

    def process(self, event: dict, partition: str, offset: int | str, *,
                fail_after_apply: bool = False) -> ProcessResult:
        started = time.perf_counter()
        try:
            validate_envelope(event, self.expected_epoch)
            digest = fingerprint(event)
            if event["operation"] == "UPSERT":
                raw = base64.b64decode(event["recordBase64"], validate=True)
                row = parse_record(raw, self.fields)
                if row["account_id"] != event["accountId"]:
                    raise ValueError("record/envelope accountId mismatch")
                if row["sequence_number"] != event["sourceVersion"]:
                    raise ValueError("record/envelope sourceVersion mismatch")
                document = normalize_document(row, event["sourceVersion"])
                # A capture cursor is not a file ordinal. Do not manufacture one.
                document.pop("sourceRecordNumber", None)
            else:
                document = {"id": event["accountId"], "accountId": event["accountId"], "source": SOURCE}
        except (ValueError, TypeError, KeyError, UnicodeError, binascii.Error) as exc:
            return self.poison(event, partition, offset, f"invalid event: {exc}")

        version = event["sourceVersion"]
        document.update({"deleted": event["operation"] == "DELETE", "sourceVersion": version,
                         "sourceEpoch": event["sourceEpoch"],
                         "_sync": {"schemaVersion": 1, "source": SOURCE, "sourceEpoch": event["sourceEpoch"],
                                   "version": version, "eventHash": digest, "eventId": event["eventId"],
                                   "sourcePosition": event["sourcePosition"],
                                   "sourceCommittedAt": event["committedAt"]}})

        def transition(current):
            current_version = 0
            if current is not None:
                state = current.get("_sync")
                if (not isinstance(state, dict) or type(state.get("schemaVersion")) is not int
                        or state["schemaVersion"] != 1
                        or state.get("source") != SOURCE or state.get("sourceEpoch") != self.expected_epoch
                        or type(state.get("version")) is not int or state["version"] <= 0
                        or not isinstance(state.get("eventHash"), str) or len(state["eventHash"]) != 64
                        or any(c not in "0123456789abcdef" for c in state["eventHash"])
                        or any(not isinstance(state.get(key), str) or not state[key]
                               for key in ("eventId", "sourcePosition"))
                        or current.get("id") != event["accountId"] or current.get("accountId") != event["accountId"]
                        or type(current.get("sourceVersion")) is not int or current["sourceVersion"] != state["version"]
                        or current.get("sourceEpoch") != self.expected_epoch or type(current.get("deleted")) is not bool):
                    return None, "quarantined", "destination lacks trustworthy source state"
                current_version = state["version"]
                if version < current_version:
                    return None, "stale", None
                if version == current_version:
                    if state["eventHash"] == digest:
                        return None, "duplicate", None
                    return None, "quarantined", "same-version content conflict"
            if event["previousVersion"] != current_version:
                return None, "quarantined", f"gap: expected previousVersion {current_version}"
            document["_sync"]["appliedAt"] = datetime.now(timezone.utc).isoformat()
            return document, "applied", None

        outcome, reason = self.store.apply(event["accountId"], transition)
        identity = None
        if outcome == "quarantined":
            identity = self.store.quarantine(event, reason, self._transport(partition, offset))
        return self._finish(ProcessResult(outcome, reason, identity), event, partition, offset,
                            started, fail_after_apply)
