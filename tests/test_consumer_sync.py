import base64
import threading
from types import SimpleNamespace

import pytest

from vsam_offload.eventhub_consumer import ConsumerHandler, run_consumer
from vsam_offload.sync_engine import ProcessResult


class Engine:
    def __init__(self):
        self.calls = []
        self.failure = False

    def process(self, payload, partition, offset):
        if self.failure:
            raise ConnectionError("sink unavailable")
        self.calls.append((payload, partition, offset))
        return ProcessResult("applied")

    def poison(self, payload, partition, offset, reason):
        self.process(payload, partition, offset)
        return ProcessResult("quarantined", reason)


def delivery(raw=b"{}", offset=1):
    return SimpleNamespace(body=[raw], offset=offset)


def context(partition="0"):
    checkpoints = []
    return SimpleNamespace(partition_id=partition, update_checkpoint=checkpoints.append, checkpoints=checkpoints)


@pytest.mark.parametrize("raw", [
    b"\xff", b"{", b'{"number":NaN}', b'{"number":1e999}',
    b'{"accountId":"a","accountId":"b"}', b'{"accountId":"\\ud800"}'
])
def test_bad_bytes_durable_before_checkpoint(raw):
    engine = Engine()
    handler, partition = ConsumerHandler(engine), context()
    item = delivery(raw)
    handler.on_event(partition, item)
    assert engine.calls[0][0] == {"rawBodyBase64": base64.b64encode(raw).decode()}
    assert partition.checkpoints == [item]
    assert not handler.failed.is_set()


def test_deep_json_is_quarantined_without_blocking_later_event():
    raw = b"[" * 10000 + b"0" + b"]" * 10000
    engine = Engine()
    handler, partition = ConsumerHandler(engine), context()
    invalid, valid = delivery(raw), delivery(offset=2)
    handler.on_event(partition, invalid)
    handler.on_event(partition, valid)
    assert engine.calls[0][0] == {"rawBodyBase64": base64.b64encode(raw).decode()}
    assert partition.checkpoints == [invalid, valid]
    assert not handler.failed.is_set()


@pytest.mark.parametrize("raw", [b"{}", b"\xff", b"[]", b"null"])
def test_failure_latches_globally_no_later_checkpoint(raw):
    engine = Engine()
    handler, partition = ConsumerHandler(engine), context()
    engine.failure = True
    handler.on_event(partition, delivery(raw))
    engine.failure = False
    other = context("1")
    handler.on_event(partition, delivery(offset=2))
    handler.on_event(other, delivery(offset=3))
    assert handler.failed.is_set()
    assert partition.checkpoints == other.checkpoints == []
    assert engine.calls == []


def test_checkpoint_failure_halts():
    handler = ConsumerHandler(Engine())
    partition = context()
    def fail(event):
        raise ConnectionError("blob unavailable")
    partition.update_checkpoint = fail
    handler.on_event(partition, delivery())
    assert handler.failed.is_set()
    assert isinstance(handler.error, ConnectionError)


def test_receive_supervisor_surfaces_swallowed_callback_failure():
    engine = Engine()
    engine.failure = True
    class Client:
        closed = False
        stopped = threading.Event()
        def receive(self, on_event, **kwargs):
            on_event(context(), delivery())
            on_event(context(), delivery(offset=2))
            assert self.stopped.wait(3), "supervisor must close even if receive keeps running"
        def close(self):
            self.closed = True
            self.stopped.set()
    client = Client()
    with pytest.raises(RuntimeError, match="halted") as raised:
        run_consumer(client, engine)
    assert isinstance(raised.value.__cause__, ConnectionError)
    assert client.closed


def test_sdk_error_latches_and_none_event_is_ignored():
    handler = ConsumerHandler(Engine())
    partition = context()
    handler.on_event(partition, None)
    handler.on_error(partition, ConnectionError("transport unavailable"))
    handler.on_event(partition, delivery())
    assert handler.failed.is_set()
    assert not partition.checkpoints
