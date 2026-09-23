import base64
import copy
import json
import shutil
from pathlib import Path
from uuid import uuid4

import pytest
from azure.cosmos.exceptions import CosmosHttpResponseError

from vsam_offload.copybook import parse_copybook, parse_record
from vsam_offload.generate_sample import SAMPLE_ROWS
from vsam_offload.sync_contract import DEFAULT_COPYBOOK, canonical, make_event
from vsam_offload.sync_demo import financial_view, run_demo
from vsam_offload.sync_engine import SimulatedCrash, SyncEngine
from vsam_offload.sync_store import CosmosStore, SQLiteStore


@pytest.fixture
def workspace():
    path = Path("out") / ("sync-tests-" + uuid4().hex)
    path.mkdir(parents=True)
    try:
        yield path
    finally:
        shutil.rmtree(path)


@pytest.fixture
def store(workspace):
    instance = SQLiteStore(workspace / "sink.sqlite")
    yield instance
    instance.close()


def event(version=1, previous=0, **kwargs):
    return make_event(dict(SAMPLE_ROWS[0]), version, previous,
                      committed_at="2026-09-23T10:00:00Z", **kwargs)


def test_contract_copies_row_and_stable_identity():
    row = dict(SAMPLE_ROWS[0])
    first, second = make_event(row, 5, 2), make_event(row, 5, 2)
    assert first["eventId"] == second["eventId"]
    assert row == SAMPLE_ROWS[0]
    parsed = parse_record(base64.b64decode(first["recordBase64"]), parse_copybook(DEFAULT_COPYBOOK))
    assert parsed["sequence_number"] == 5
    assert parsed["current_balance"] == "12450.70"


@pytest.mark.parametrize("version,previous", [(True, 0), (0, 0), (-1, 0), (1, -1), (1, 1), (2, True)])
def test_contract_invalid_versions(version, previous):
    with pytest.raises(ValueError):
        event(version, previous)


def test_ordering_conflict_gap_tombstone(store):
    engine = SyncEngine(store)
    first = event()
    assert engine.process(first, "0", 1).outcome == "applied"
    assert engine.process(first, "0", 2).outcome == "duplicate"
    assert engine.process(dict(first, sourcePosition="other"), "0", 3).outcome == "quarantined"
    later = event(8, 5)
    assert engine.process(later, "0", 4).reason.startswith("gap:")
    assert engine.process(event(5, 1), "0", 5).outcome == "applied"
    assert engine.process(later, "0", 6).outcome == "applied"
    assert engine.process(event(10, 8, operation="DELETE"), "0", 7).outcome == "applied"
    assert engine.process(first, "0", 8).outcome == "stale"
    assert store.get(first["accountId"])["deleted"]
    assert store.get(first["accountId"])["_sync"]["version"] == 10
    assert engine.process(event(11, 10), "0", 9).outcome == "applied"
    assert not store.get(first["accountId"])["deleted"]


@pytest.mark.parametrize("change", [
    {"sourceEpoch": "old"}, {"committed": False}, {"committed": 1}, {"schemaVersion": True},
    {"schemaVersion": 2}, {"source": "STATEMENT"}, {"codePage": "cp500"}, {"copybookId": "OTHER"},
    {"sourceVersion": True}, {"previousVersion": True}, {"eventId": ""}, {"sourcePosition": ""},
    {"operation": "INSERT"}, {"committedAt": "2026-01-01"}, {"committedAt": "2026-01-01T10:00:00+01:00"},
    {"accountId": "different"}, {"recordBase64": "not base64!"}, {"sourceVersion": 2},
])
def test_bad_envelope_or_record_quarantines(store, change):
    engine = SyncEngine(store)
    result = engine.process(dict(event(), **change), "0", "opaque")
    assert result.outcome == "quarantined"
    assert result.checkpoint_safe
    assert store.checkpoint("0") == "opaque"
    assert not store.documents()
    assert store.quarantines()[0]["payload"] == dict(event(), **change)


def test_stale_epoch_is_quarantined_before_ordering(store):
    engine = SyncEngine(store)
    engine.process(event(10, 0), "0", 1)
    assert engine.process(event(source_epoch="old"), "0", 2).outcome == "quarantined"


@pytest.mark.parametrize("payload", [None, [], 4, "bad", {}, {"committed": False}])
def test_malformed_json_value(store, payload):
    result = SyncEngine(store).process(payload, "p", 10)
    assert result.outcome == "quarantined"
    assert store.quarantines()[0]["payload"] == payload


def test_invalid_comp3_and_delete_body(store):
    bad = event()
    raw = bytearray(base64.b64decode(bad["recordBase64"]))
    raw[23] = 0x1A
    bad["recordBase64"] = base64.b64encode(raw).decode()
    engine = SyncEngine(store)
    assert "COMP-3" in engine.process(bad, "0", 1).reason
    assert engine.process(dict(event(operation="DELETE"), recordBase64="AA=="), "0", 2).outcome == "quarantined"


def test_legacy_bootstrap_not_overwritten(store):
    original = {"id": SAMPLE_ROWS[0]["account_id"], "accountId": SAMPLE_ROWS[0]["account_id"], "currentBalance": "9.00"}
    store.apply(original["id"], lambda _: (original, "applied", None))
    assert "trustworthy" in SyncEngine(store).process(event(), "0", 1).reason
    assert store.get(original["id"]) == original


def test_crash_reopen_replay_and_quarantine_idempotence(workspace):
    path = workspace / "durable.sqlite"
    store = SQLiteStore(path)
    try:
        engine = SyncEngine(store)
        with pytest.raises(SimulatedCrash):
            engine.process(event(), "0", 1, fail_after_apply=True)
        assert store.checkpoint("0") is None
    finally:
        store.close()
    store = SQLiteStore(path)
    try:
        engine = SyncEngine(store)
        assert engine.process(event(), "0", 1).outcome == "duplicate"
        assert store.checkpoint("0") == "1"
        first = engine.process({}, "0", 2)
        second = engine.process({}, "0", 2)
        assert first.quarantine_id == second.quarantine_id
        assert len(store.quarantines()) == 1
    finally:
        store.close()


@pytest.mark.parametrize("method,payload", [("apply", event()), ("quarantine", {})])
def test_sink_failure_never_checkpoints(store, monkeypatch, method, payload):
    def fail(*args):
        raise ConnectionError("unavailable")
    monkeypatch.setattr(store, method, fail)
    with pytest.raises(ConnectionError):
        SyncEngine(store).process(payload, "0", 1)
    assert store.checkpoint("0") is None


def test_checkpoint_scopes(workspace):
    path = workspace / "scopes.sqlite"
    for stream, group, expected in [("s", "g", None), ("s", "other", None), ("other", "g", None), ("s", "g", "42")]:
        store = SQLiteStore(path, stream=stream, group=group)
        try:
            assert store.checkpoint("0") == expected
            assert store.checkpoint("1") is None
            store.save_checkpoint("0", "42")
        finally:
            store.close()


class HttpError(CosmosHttpResponseError):
    def __init__(self, status):
        super().__init__(status_code=status, message="injected SDK error")


class Container:
    def __init__(self):
        self.items, self.failures, self.writes = {}, [], []

    def read_item(self, item, partition_key):
        assert item == partition_key
        if item not in self.items:
            raise HttpError(404)
        return copy.deepcopy(self.items[item])

    def _write(self, body, kind):
        self.writes.append(kind)
        if self.failures:
            raise HttpError(self.failures.pop(0))
        self.items[body["id"]] = dict(copy.deepcopy(body), _etag=str(len(self.writes)))

    def create_item(self, body):
        if body["id"] in self.items:
            raise HttpError(409)
        self._write(body, "create")

    def replace_item(self, item, body, etag, match_condition):
        from azure.core import MatchConditions
        assert etag == self.items[item]["_etag"]
        assert match_condition == MatchConditions.IfNotModified
        self._write(body, "replace")


def test_cosmos_cas_retries_and_poison_idempotence():
    container, poison = Container(), Container()
    store = CosmosStore(container, poison)
    engine = SyncEngine(store, stream="namespace/hub")
    container.failures = [409]
    assert engine.process(event(), "0", 1).outcome == "applied"
    container.failures = [412]
    assert engine.process(event(2, 1), "0", 2).outcome == "applied"
    assert container.writes == ["create", "create", "replace", "replace"]
    engine.process({}, "0", 3)
    engine.process({}, "0", 3)
    assert len(poison.items) == 1
    assert next(iter(poison.items.values()))["transport"]["stream"] == "namespace/hub"


@pytest.mark.parametrize("status", [403, 429, 500])
def test_cosmos_non_conflict_errors_surface(status):
    container = Container()
    container.failures = [status]
    with pytest.raises(HttpError):
        SyncEngine(CosmosStore(container, Container())).process(event(), "0", 1)
    assert len(container.writes) == 1


def test_cosmos_bounded_retries():
    container = Container()
    container.failures = [409] * 10
    with pytest.raises(HttpError):
        SyncEngine(CosmosStore(container, Container(), max_attempts=3)).process(event(), "0", 1)
    assert len(container.writes) == 3


@pytest.mark.parametrize("incoming,winner,expected", [
    (event(2, 1), event(2, 1), "duplicate"),
    (event(2, 1), event(3, 1), "stale"),
    (event(2, 1), dict(event(2, 1), eventId="competitor"), "quarantined"),
    (event(4, 1), event(2, 1), "quarantined"),
])
def test_cosmos_rereads_and_redecides_after_concurrent_writer(monkeypatch, incoming, winner, expected):
    destination, competing = Container(), Container()
    for target in (destination, competing):
        SyncEngine(CosmosStore(target, Container())).process(event(), "0", 1)
    SyncEngine(CosmosStore(competing, Container())).process(winner, "1", 2)
    saved = copy.deepcopy(competing.items)
    def race(**kwargs):
        destination.items = copy.deepcopy(saved)
        raise HttpError(412)
    monkeypatch.setattr(destination, "replace_item", race)
    assert SyncEngine(CosmosStore(destination, Container())).process(incoming, "0", 2).outcome == expected
    assert destination.items == saved


def test_consumer_non_object_is_durable_quarantine(store):
    from types import SimpleNamespace
    from vsam_offload.eventhub_consumer import ConsumerHandler
    checkpoints = []
    context = SimpleNamespace(partition_id="0", update_checkpoint=checkpoints.append)
    delivery = SimpleNamespace(body=[b"[]"], offset=1)
    ConsumerHandler(SyncEngine(store)).on_event(context, delivery)
    assert checkpoints == [delivery]
    assert store.quarantines()[0]["payload"] == []


def test_quarantine_checkpoint_failure_replay_is_idempotent(store, monkeypatch):
    engine = SyncEngine(store)
    with monkeypatch.context() as patch:
        def fail(*args):
            raise ConnectionError("checkpoint unavailable")
        patch.setattr(store, "save_checkpoint", fail)
        with pytest.raises(ConnectionError):
            engine.process({}, "0", 1)
    assert store.checkpoint("0") is None
    assert len(store.quarantines()) == 1
    assert engine.process({}, "0", 1).outcome == "quarantined"
    assert len(store.quarantines()) == 1
    assert store.checkpoint("0") == "1"


def test_reconciliation_does_not_round_away_corruption():
    document = {"accountId": "1", "sourceVersion": 1, "sourceEpoch": "demo-epoch-1", "deleted": False,
                "branch": "0001", "lastTransactionDate": "20260923", "status": "A",
                "currentBalance": "1.001", "availableLimit": "0.00"}
    with pytest.raises(ValueError, match="exact"):
        financial_view(document)


def test_executable_demo_artifacts_and_no_clobber(workspace):
    output = workspace / "demo"
    report = run_demo(output, 0)
    assert report["invariantsPassed"]
    assert report["status"] == "attention-required"
    assert not report["reconciliationBeforeReplay"]["equal"]
    assert report["reconciliationAfterReplay"]["equal"]
    assert len(report["unresolvedQuarantine"]) == 2
    steps = {step["step"]: step for step in report["steps"]}
    assert steps["crash after sink apply before checkpoint"]["transportOffset"] == steps[
        "reopen and resume replay"]["transportOffset"]
    assert json.loads((output / "report.json").read_text()) == report
    assert all(isinstance(json.loads(line), dict) for line in (output / "events.jsonl").read_text().splitlines())
    assert (output / "source-records.bin").stat().st_size % 51 == 0
    with pytest.raises(FileExistsError):
        run_demo(output, 0)


def test_apply_timestamps_are_preserved_on_duplicate(store):
    engine = SyncEngine(store)
    envelope = event()
    engine.process(envelope, "0", 1)
    initial = store.get(envelope["accountId"])["_sync"]
    assert initial["sourceCommittedAt"] == envelope["committedAt"]
    assert initial["appliedAt"]
    engine.process(envelope, "0", 2)
    assert store.get(envelope["accountId"])["_sync"] == initial
