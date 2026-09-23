"""Optional real Event Hubs -> Cosmos consumer; pre-provision resources with /accountId keys."""
from __future__ import annotations

import base64
import json
import math
import os
import threading
from contextlib import ExitStack

from .sync_engine import SyncEngine
from .sync_store import CosmosStore
from .sync_contract import canonical


def reject_constant(value):
    raise ValueError(f"non-JSON numeric constant: {value}")


def finite_float(value):
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("JSON number overflow")
    return number


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON field: {key}")
        result[key] = value
    return result


class ConsumerHandler:
    """Serialize callbacks, including checkpoints; a sink failure closes the global gate."""

    def __init__(self, engine):
        self.engine = engine
        self.failed = threading.Event()
        self.error: Exception | None = None
        self.lock = threading.RLock()

    def fail(self, error: Exception) -> None:
        with self.lock:
            if self.error is None:
                self.error = error
            self.failed.set()

    def on_error(self, partition_context, error) -> None:
        self.fail(error)

    def on_event(self, context, event) -> None:
        with self.lock:
            if self.failed.is_set() or event is None:
                return
            try:
                if event.offset is None:
                    raise ValueError("Event Hubs delivery has no offset")
                raw = b"".join(event.body)
                try:
                    payload = json.loads(raw.decode("utf-8"), parse_constant=reject_constant,
                                         parse_float=finite_float, object_pairs_hook=unique_object)
                    canonical(payload).encode("utf-8")
                except (UnicodeError, ValueError, RecursionError) as exc:
                    result = self.engine.poison({"rawBodyBase64": base64.b64encode(raw).decode("ascii")},
                                               context.partition_id, event.offset, f"invalid UTF-8/JSON: {exc}")
                else:
                    result = self.engine.process(payload, context.partition_id, event.offset)
                if not result.checkpoint_safe:
                    raise RuntimeError("sink did not acknowledge durable processing")
                context.update_checkpoint(event)
                print(canonical({"partition": context.partition_id, "offset": str(event.offset),
                                 "outcome": result.outcome, "reason": result.reason,
                                 "quarantineId": result.quarantine_id,
                                 "checkpoint": "persisted"}), flush=True)
            except Exception as exc:
                # EH may swallow callback exceptions. The receive supervisor observes this latch.
                self.fail(exc)


def run_consumer(client, engine) -> None:
    handler = ConsumerHandler(engine)
    done = threading.Event()

    def receive():
        try:
            client.receive(on_event=handler.on_event, on_error=handler.on_error, starting_position="-1")
        except Exception as exc:
            handler.fail(exc)
        finally:
            done.set()

    worker = threading.Thread(target=receive, name="sync-eventhub-receive", daemon=True)
    worker.start()
    try:
        while not done.wait(0.1):
            if handler.failed.is_set():
                break
    finally:
        # Close from the supervising thread, not inside an SDK callback.
        handler.failed.set()
        try:
            client.close()
        finally:
            worker.join(timeout=10)
    if handler.error is not None:
        raise RuntimeError("Consumer halted; no subsequent delivery was checkpointed after failure") from handler.error
    if worker.is_alive():
        raise RuntimeError("Event Hubs receive thread did not stop")


def main() -> None:
    from azure.cosmos import CosmosClient
    from azure.eventhub import EventHubConsumerClient
    from azure.identity import DefaultAzureCredential
    try:
        from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
    except ImportError as exc:
        raise RuntimeError("Install azure-eventhub-checkpointstoreblob to run the optional consumer") from exc

    required = ("EVENTHUB_FULLY_QUALIFIED_NAMESPACE", "EVENTHUB_NAME", "CHECKPOINT_BLOB_URL", "COSMOS_ENDPOINT")
    missing = [name for name in required if not os.environ.get(name)]
    if missing:
        raise ValueError("Missing environment variables: " + ", ".join(missing))
    with ExitStack() as stack:
        credential = DefaultAzureCredential()
        stack.callback(credential.close)
        cosmos = CosmosClient(os.environ["COSMOS_ENDPOINT"], credential=credential)
        stack.callback(cosmos.close)
        database = cosmos.get_database_client(os.getenv("COSMOS_DATABASE", "mainframeOffload"))
        store = CosmosStore(database.get_container_client(os.getenv("COSMOS_CONTAINER", "balances")),
                            database.get_container_client(os.getenv("COSMOS_QUARANTINE_CONTAINER", "sync-quarantine")))
        checkpoint_store = BlobCheckpointStore(blob_account_url=os.environ["CHECKPOINT_BLOB_URL"],
                                              container_name=os.getenv("CHECKPOINT_CONTAINER", "sync-checkpoints"),
                                              credential=credential)
        if hasattr(checkpoint_store, "close"):
            stack.callback(checkpoint_store.close)
        group = os.getenv("EVENTHUB_CONSUMER_GROUP", "sync-demo")
        namespace, hub = os.environ["EVENTHUB_FULLY_QUALIFIED_NAMESPACE"], os.environ["EVENTHUB_NAME"]
        client = EventHubConsumerClient(fully_qualified_namespace=namespace, eventhub_name=hub,
                                        consumer_group=group, credential=credential, checkpoint_store=checkpoint_store)
        engine = SyncEngine(store, stream=f"{namespace}/{hub}", group=group)
        try:
            run_consumer(client, engine)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
