"""Test-only transport/storage fakes; production has no local substitute."""
import copy
import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest
from azure.core.exceptions import AzureError, HttpResponseError, ResourceExistsError, ResourceNotFoundError

from vsam_offload.guided_cloud import AzureCloud, ConfigurationError, ManifestLease
from vsam_offload.guided_pipeline import GuidedError, GuidedPipeline, STEPS, digest, encoded, path
from vsam_offload.sync_contract import canonical


class TestStore:
    __test__ = False

    def __init__(self):
        self.documents, self.poison, self.writes = {}, [], 0

    def get(self, key):
        return copy.deepcopy(self.documents.get(key))

    def apply(self, key, transition):
        document, outcome, reason = transition(self.get(key))
        if document is not None:
            self.documents[key] = copy.deepcopy(document)
            self.writes += 1
        return outcome, reason

    def quarantine(self, payload, reason, transport):
        self.poison.append((payload, reason, transport))
        return "test-quarantine"


class TestCloud:
    __test__ = False
    stream, group = "test-namespace/test-hub", "guided-demo"

    def __init__(self):
        self.store = TestStore()
        self.manifests, self.blobs, self.messages, self.locked = {}, {}, [], set()
        self.reads, self.received = [], []
        self.fail_send = False
        self.fail_save_after_sink = False
        self.fail_all_saves = False

    def allocate_accounts(self, run_id, count):
        # Deliberately descending: manifest canonicalization sorts object keys.
        return [f"{900 - len(self.manifests) * 10 - i:012d}" for i in range(count)]

    def create_manifest(self, run):
        self.manifests[run["runId"]] = json.loads(canonical(run))

    def load_manifest(self, run_id):
        if run_id not in self.manifests:
            raise GuidedError(404, "Unknown run")
        return copy.deepcopy(self.manifests[run_id])

    def list_manifests(self):
        return list(self.manifests.values())

    @contextmanager
    def lock(self, run_id):
        if run_id in self.locked:
            raise GuidedError(409, "Busy")
        self.load_manifest(run_id)
        self.locked.add(run_id)

        def save(run):
            if self.fail_save_after_sink and self.store.writes:
                self.fail_all_saves = True
            if self.fail_all_saves:
                raise AzureError("secret must never surface")
            self.create_manifest(run)

        try:
            yield SimpleNamespace(load=lambda: self.load_manifest(run_id), save=save, check=lambda: None)
        finally:
            self.locked.remove(run_id)

    def put(self, name, raw, media=None):
        if name in self.blobs:
            assert self.blobs[name] == raw
        self.blobs[name] = raw
        return self.info(name)

    def get(self, name):
        self.reads.append(name)
        if name not in self.blobs:
            raise GuidedError(404, "Missing artifact")
        return self.blobs[name]

    def info(self, name):
        return dict(name=name, byteCount=len(self.blobs[name]), etag="test-etag")

    def starting_positions(self):
        return {"0": len(self.messages) - 1}

    def send(self, event):
        run = self.manifests[event["runId"]]
        assert "_publish" in run and event["eventId"] in run["_publish"]["eventIds"]
        self.messages.append(copy.deepcopy(event))
        if self.fail_send:
            self.fail_send = False
            raise AzureError("send failed after acceptance")

    def consume(self, positions, receive, done):
        for sequence, event in enumerate(self.messages):
            if done():
                return
            if sequence > positions["0"]:
                self.received.append(event)
                receive("0", str(sequence * 100), sequence, encoded(event))

    def config(self):
        return dict(mode="azure", host={}, services=[])


@pytest.fixture
def demo():
    cloud = TestCloud()
    pipeline = GuidedPipeline(cloud)
    run_id = pipeline.create_run()["runId"]
    return pipeline, cloud, run_id


def advance(pipeline, run_id, end="verify"):
    for step in STEPS[:STEPS.index(end) + 1]:
        result = pipeline.step(run_id, step)
    return result


def test_manual_prerequisites_and_no_sink_before_apply(demo):
    pipeline, cloud, run_id = demo
    assert pipeline.get_run(run_id)["stage"] == 0
    with pytest.raises(GuidedError, match="anteriores") as error:
        pipeline.step(run_id, "verify")
    assert error.value.status == 409
    advance(pipeline, run_id, "publish")
    assert not cloud.store.documents and cloud.store.writes == 0
    assert cloud.manifests[run_id]["stage"] == 4
    result = pipeline.step(run_id, "apply")
    assert result["stage"] == 5 and cloud.store.writes == 4
    assert pipeline.step(run_id, "verify")["steps"]["verify"]["data"]["matches"]
    assert not any(key.startswith("_") for key in pipeline.get_run(run_id))


def test_source_transfer_parse_preserve_real_bytes_not_fixture(demo):
    pipeline, cloud, run_id = demo
    source = pipeline.step(run_id, "source")["steps"]["source"]["data"]
    raw, media, filename = pipeline.artifact(run_id, "source")
    assert len(raw) == 204 and source["recordByteCount"] == 51
    assert source["hex"] == raw.hex() and source["hex64"] == raw[:64].hex()
    assert source["sha256"] == digest(raw) and len(source["fields"]) == 7
    assert source["simulated"] and filename.endswith("-source.bin")
    # Prove later steps don't consult the fixture rows.
    cloud.manifests[run_id]["_sourceRows"][0]["current_balance"] = "999.99"
    advance(pipeline, run_id, "parse")
    landing, _, _ = pipeline.artifact(run_id, "landing")
    assert raw == landing
    data = pipeline.get_run(run_id)["steps"]["parse"]["data"]
    assert data["records"][0]["row"]["current_balance"] == "12450.70"
    assert data["records"][2]["row"]["current_balance"] == "-15.10"
    field = data["records"][0]["fields"][2]
    assert field["rawHex"] == raw[16:24].hex() and field["decodedValue"] == "12450.70"
    assert field["picture"] == "S9(13)V99 COMP-3" and field["offset"] == 16
    assert path(cloud.manifests[run_id], "landing") in cloud.reads


def test_idempotency_lease_unknown_and_path_guards(demo):
    pipeline, cloud, run_id = demo
    first = pipeline.step(run_id, "source")
    assert pipeline.step(run_id, "source") == first
    with cloud.lock(run_id):
        with pytest.raises(GuidedError) as error:
            pipeline.step(run_id, "source")
        assert error.value.status == 409
    assert not cloud.locked
    with pytest.raises(GuidedError) as error:
        pipeline.get_run(str(uuid4()))
    assert error.value.status == 404
    for value in ["../x", "source/x", "..\\x", run_id.upper(), "%2e%2e"]:
        with pytest.raises(GuidedError) as error:
            pipeline.artifact(value, "source")
        assert error.value.status == 400
    for kind in ["../source", "..\\secret", "metadata", "unknown"]:
        with pytest.raises(GuidedError) as error:
            pipeline.artifact(run_id, kind)
        assert error.value.status == 404


def test_partial_publish_keeps_positions_identity_timestamps_on_restart(demo):
    pipeline, cloud, run_id = demo
    advance(pipeline, run_id, "parse")
    cloud.fail_send = True
    with pytest.raises(GuidedError):
        pipeline.step(run_id, "publish")
    failed = pipeline.get_run(run_id)
    assert failed["steps"]["publish"]["status"] == "failed" and failed["stage"] == 3
    first = copy.deepcopy(cloud.messages[0])
    positions = copy.deepcopy(cloud.manifests[run_id]["_publish"])
    restarted = GuidedPipeline(cloud)
    restarted.step(run_id, "publish")
    assert cloud.messages[0] == cloud.messages[1] == first
    assert cloud.manifests[run_id]["_publish"] == positions
    result = advance(restarted, run_id)
    assert result["stage"] == 6 and cloud.store.writes == 4
    assert not cloud.store.poison
    before = len(cloud.messages)
    restarted.step(run_id, "publish")
    assert len(cloud.messages) == before


def test_sink_success_checkpoint_failure_retries_through_version_guard(demo):
    pipeline, cloud, run_id = demo
    advance(pipeline, run_id, "publish")
    cloud.fail_save_after_sink = True
    with pytest.raises(GuidedError):
        pipeline.step(run_id, "apply")
    assert cloud.store.writes == 1
    assert not cloud.manifests[run_id]["_checkpoints"]
    assert cloud.manifests[run_id]["steps"]["apply"]["status"] != "completed"
    cloud.fail_save_after_sink = cloud.fail_all_saves = False
    result = GuidedPipeline(cloud).step(run_id, "apply")
    assert result["steps"]["apply"]["data"]["received"][0]["outcome"] == "duplicate"
    assert cloud.store.writes == 4 and not cloud.locked
    assert json.loads(pipeline.artifact(run_id, "checkpoints")[0])["checkpoints"]["0"]["sequenceNumber"] == 3


def test_missing_transport_never_uses_archive_and_checkpoint_is_run_scoped(demo):
    pipeline, cloud, run_id = demo
    advance(pipeline, run_id, "publish")
    events = copy.deepcopy(cloud.messages)
    cloud.messages.clear()
    with pytest.raises(GuidedError):
        pipeline.step(run_id, "apply")
    assert cloud.store.writes == 0
    unrelated = dict(events[0], runId=str(uuid4()))
    cloud.messages.extend([unrelated, *events])
    result = pipeline.step(run_id, "apply")
    assert result["steps"]["apply"]["data"]["count"] == 4
    assert len(cloud.manifests[run_id]["_received"]) == 4
    assert cloud.manifests[run_id]["_checkpoints"]["0"]["sequenceNumber"] == 4


def test_received_envelope_corruption_and_landing_tamper_fail(demo):
    pipeline, cloud, run_id = demo
    advance(pipeline, run_id, "publish")
    cloud.messages[0]["sourceVersion"] = 999
    with pytest.raises(GuidedError):
        pipeline.step(run_id, "apply")
    assert not cloud.manifests[run_id]["_checkpoints"] and cloud.store.writes == 0
    second = pipeline.create_run()["runId"]
    advance(pipeline, second, "transfer")
    cloud.blobs[path(cloud.manifests[second], "landing")] = b"corrupted"
    with pytest.raises(GuidedError):
        pipeline.step(second, "parse")
    assert pipeline.get_run(second)["steps"]["parse"]["status"] == "failed"


def test_quarantined_version_conflict_never_checkpoints_or_completes(demo):
    pipeline, cloud, run_id = demo
    advance(pipeline, run_id, "publish")
    first = cloud.messages[0]["accountId"]
    cloud.store.documents[first] = {"id": first, "accountId": first, "currentBalance": "0.00"}
    with pytest.raises(GuidedError):
        pipeline.step(run_id, "apply")
    assert cloud.store.poison and not cloud.manifests[run_id]["_checkpoints"]
    assert pipeline.get_run(run_id)["steps"]["apply"]["status"] == "failed"
    assert cloud.store.documents[first]["currentBalance"] == "0.00"


def test_verify_point_reads_independent_expectation_and_change(demo):
    pipeline, cloud, run_id = demo
    advance(pipeline, run_id, "apply")
    first_account = cloud.manifests[run_id]["_primaryAccount"]
    cloud.store.documents[first_account]["currentBalance"] = "999.00"
    with pytest.raises(GuidedError):
        pipeline.step(run_id, "verify")
    evidence = pipeline.get_run(run_id)["steps"]["verify"]["data"]
    assert not evidence["matches"]
    assert next(r for r in evidence["records"] if r["accountId"] == first_account)["expected"]["currentBalance"] == "12450.70"
    cloud.store.documents[first_account]["currentBalance"] = "12450.70"
    pipeline.step(run_id, "verify")
    for version in [2, 3]:
        changed = pipeline.change(run_id)
        assert changed["stage"] == 0 and changed["steps"] == {} and changed["phase"] == version
        assert pipeline.change(run_id) == changed
        assert cloud.manifests[run_id]["_sourceRows"][0]["account_id"] == first_account
        source = pipeline.step(run_id, "source")["steps"]["source"]["data"]
        assert source["byteCount"] == 51
        assert source["records"][0]["sourceVersion"] == version
        assert source["records"][0]["previousVersion"] == version - 1
        result = advance(GuidedPipeline(cloud), run_id)
        assert result["steps"]["verify"]["data"]["actualCount"] == 4
        assert cloud.store.get(first_account)["sourceVersion"] == version
    assert cloud.store.get(first_account)["currentBalance"] == "12470.72"
    assert len(pipeline.get_run(run_id)["history"]) == 2


def test_missing_configuration_never_uses_local_fallback(monkeypatch):
    monkeypatch.delenv("STORAGE_ACCOUNT_URL", raising=False)
    with pytest.raises(ConfigurationError) as error:
        AzureCloud()
    assert error.value.status == 503


def test_manifest_lease_fences_failed_renewal_and_cas():
    blob, lease = Mock(), Mock()
    blob.download_blob.return_value = SimpleNamespace(properties=SimpleNamespace(etag="before"),
                                                      readall=lambda: b'{"runId":"test"}')
    blob.upload_blob.return_value = {"etag": "after"}
    control = ManifestLease(blob, lease)
    run = control.load()
    control.save(run)
    assert blob.upload_blob.call_args.kwargs["etag"] == "before"
    assert blob.upload_blob.call_args.kwargs["lease"] is lease
    assert control.etag == "after"
    control.failure = AzureError("renewal failed")
    with pytest.raises(GuidedError) as error:
        control.save(run)
    assert error.value.status == 409 and blob.upload_blob.call_count == 1


@pytest.mark.parametrize("callback_error", [False, True])
def test_cloud_consumer_reads_transport_and_closes_from_supervisor(callback_error):
    import threading

    cloud = AzureCloud.__new__(AzureCloud)
    cloud._closing_lock, cloud._closing = threading.Lock(), []
    finished, delivered = threading.Event(), []

    class Receiver:
        def receive(self, **kwargs):
            self.receiver_thread = threading.get_ident()
            assert kwargs["partition_id"] == "0"
            assert kwargs["starting_position"] == 10 and not kwargs["starting_position_inclusive"]
            kwargs["on_event"](SimpleNamespace(partition_id="0"),
                               SimpleNamespace(offset="101", sequence_number=11, body=[b'{"actual":"message"}']))
            kwargs["on_event"](SimpleNamespace(partition_id="0"),
                               SimpleNamespace(offset="102", sequence_number=12, body=[b"must not process"]))
            assert finished.wait(2)

        def close(self):
            assert threading.get_ident() != self.receiver_thread
            finished.set()

    cloud._consumer = Receiver

    def receive(*args):
        if callback_error:
            raise AzureError("sink failed")
        delivered.append(args)

    if callback_error:
        with pytest.raises(GuidedError):
            cloud.consume({"0": 10}, receive, lambda: bool(delivered), timeout=1)
    else:
        cloud.consume({"0": 10}, receive, lambda: bool(delivered), timeout=1)
        assert delivered == [("0", "101", 11, b'{"actual":"message"}')]
    assert finished.is_set()


def test_late_sdk_startup_is_eventually_closed_after_http_timeout(monkeypatch):
    import threading
    from vsam_offload import guided_cloud

    monkeypatch.setattr(guided_cloud, "CLEANUP_TIMEOUT", 0.02)
    cloud = AzureCloud.__new__(AzureCloud)
    cloud._closing_lock, cloud._closing = threading.Lock(), []
    may_start, closed = threading.Event(), threading.Event()

    class SlowReceiver:
        def receive(self, **kwargs):
            assert may_start.wait(2)
            self.running = True
            kwargs["on_partition_initialize"](SimpleNamespace(partition_id="0"))
            assert closed.wait(2)

        def close(self):
            assert self.running, "Closing before SDK startup loses its stop signal"
            closed.set()

    cloud._consumer = SlowReceiver
    try:
        with pytest.raises(GuidedError) as error:
            cloud.consume({"0": -1}, Mock(), lambda: False, timeout=0.01)
        assert error.value.status == 503
        with pytest.raises(GuidedError) as error:
            cloud.consume({"0": -1}, Mock(), lambda: False, timeout=0.01)
        assert error.value.status == 503  # Retrying cannot accumulate orphan receivers.
    finally:
        may_start.set()
        for supervisor in cloud._closing:
            supervisor.join(timeout=2)
    assert closed.is_set() and all(not worker.is_alive() for worker in cloud._closing)


@pytest.mark.parametrize("status", [409, 412])
def test_manifest_cas_conflict_closes_write_gate(status):
    blob = Mock()
    error = HttpResponseError("lease or etag conflict")
    error.status_code = status
    blob.upload_blob.side_effect = error
    control = ManifestLease(blob, Mock())
    with pytest.raises(GuidedError) as failure:
        control.save({})
    assert failure.value.status == 409
    assert control.failure is error
    with pytest.raises(GuidedError):
        control.check()


def test_real_cloud_lock_releases_in_finally_and_maps_contention(monkeypatch):
    import azure.storage.blob

    cloud = AzureCloud.__new__(AzureCloud)
    cloud.control = Mock()
    lease = Mock()
    monkeypatch.setattr(azure.storage.blob, "BlobLeaseClient", Mock(return_value=lease))
    with pytest.raises(ValueError, match="deliberate"):
        with cloud.lock(str(uuid4())):
            raise ValueError("deliberate")
    lease.acquire.assert_called_once_with(lease_duration=60, timeout=10)
    lease.release.assert_called_once()
    error = HttpResponseError("busy")
    error.status_code = 409
    lease.acquire.side_effect = error
    with pytest.raises(GuidedError) as failure:
        with cloud.lock(str(uuid4())):
            pytest.fail("must not acquire a busy run")
    assert failure.value.status == 409 and lease.release.call_count == 1
    lease.acquire.side_effect = ResourceNotFoundError("missing")
    with pytest.raises(GuidedError) as failure:
        with cloud.lock(str(uuid4())):
            pytest.fail("must not acquire an unknown run")
    assert failure.value.status == 404


def test_real_cloud_adapter_retains_immutable_bytes_and_partition_key():
    cloud = AzureCloud.__new__(AzureCloud)
    cloud.raw = Mock()
    cloud.raw.container_name = "vsam-raw"
    blob = cloud.raw.get_blob_client.return_value
    blob.url = "https://example.blob.core.windows.net/vsam-raw/source/run.bin"
    blob.get_blob_properties.return_value = SimpleNamespace(etag="etag", size=3)
    info = cloud.put("source/run.bin", b"raw")
    assert info["byteCount"] == 3 and info["etag"] == "etag"
    assert blob.upload_blob.call_args.kwargs["overwrite"] is False
    blob.upload_blob.side_effect = ResourceExistsError("exists")
    blob.download_blob.return_value.readall.return_value = b"raw"
    cloud.put("source/run.bin", b"raw")
    blob.download_blob.return_value.readall.return_value = b"changed"
    with pytest.raises(GuidedError) as failure:
        cloud.put("source/run.bin", b"raw")
    assert failure.value.status == 409
    producer = Mock()
    cloud._producer = Mock()
    cloud._producer.return_value.__enter__ = Mock(return_value=producer)
    cloud._producer.return_value.__exit__ = Mock(return_value=False)
    event = dict(accountId="123456789012", runId=str(uuid4()), phase=1)
    cloud.send(event)
    producer.create_batch.assert_called_once_with(partition_key=event["accountId"])
    message = producer.create_batch.return_value.add.call_args.args[0]
    assert json.loads(message.body_as_str()) == event
    assert message.properties == {"runId": event["runId"], "phase": 1}
    producer.send_batch.assert_called_once_with(producer.create_batch.return_value, timeout=20)


def test_sdk_configuration_uses_identity_seconds_and_actual_resource_links(monkeypatch):
    import azure.cosmos
    import azure.eventhub
    import azure.identity
    import azure.storage.blob

    values = dict(STORAGE_ACCOUNT_URL="https://teststore.blob.core.windows.net",
                  COSMOS_ENDPOINT="https://testcosmos.documents.azure.com",
                  EVENTHUB_FULLY_QUALIFIED_NAMESPACE="testevents.servicebus.windows.net",
                  AZURE_SUBSCRIPTION_ID="test-subscription", AZURE_RESOURCE_GROUP="test-group",
                  AZURE_LOCATION="brazilsouth", AZURE_WEBAPP_NAME="test-app",
                  EVENTHUB_CONSUMER_GROUP="guided-demo")
    for key, value in values.items():
        monkeypatch.setenv(key, value)
    identity, cosmos, blobs, producer, consumer = [Mock() for _ in range(5)]
    monkeypatch.setattr(azure.identity, "DefaultAzureCredential", identity)
    monkeypatch.setattr(azure.cosmos, "CosmosClient", cosmos)
    monkeypatch.setattr(azure.storage.blob, "BlobServiceClient", blobs)
    monkeypatch.setattr(azure.eventhub, "EventHubProducerClient", producer)
    monkeypatch.setattr(azure.eventhub, "EventHubConsumerClient", consumer)
    cloud = AzureCloud()
    cloud._producer()
    cloud._consumer()
    assert cosmos.call_args.kwargs["credential"] is identity.return_value
    assert cosmos.call_args.kwargs["connection_timeout"] == 10
    assert cosmos.call_args.kwargs["read_timeout"] == 15
    assert "request_timeout" not in cosmos.call_args.kwargs  # Legacy parameter is milliseconds.
    assert consumer.call_args.kwargs["consumer_group"] == "guided-demo"
    assert consumer.call_args.kwargs["socket_timeout"] == 0.5
    assert producer.call_args.kwargs["socket_timeout"] == 0.5
    assert producer.call_args.kwargs["credential"] is identity.return_value
    config = cloud.config()
    assert {service["id"] for service in config["services"]} == {
        "origin", "compute", "storage", "eventhubs", "cosmos", "control"}
    assert config["host"]["portalUrl"].endswith("/Microsoft.Web/sites/test-app")
