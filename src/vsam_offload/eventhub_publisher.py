from __future__ import annotations

import argparse
import json
import os
import time
from pathlib import Path

from azure.eventhub import EventData, EventHubProducerClient
from azure.identity import DefaultAzureCredential


def publish(producer, event_lines, interval_seconds: float = 0.0) -> int:
    """Preserve file order and route every change for a key to one partition."""
    if interval_seconds < 0:
        raise ValueError("interval-seconds must be nonnegative")
    sent = 0
    for line in event_lines:
        if not line.strip():
            continue
        envelope = json.loads(line)
        if not isinstance(envelope, dict) or not isinstance(envelope.get("accountId"), str):
            raise ValueError("Expected raw CDC envelope with accountId; generate with sync_demo.")
        if not envelope["accountId"]:
            raise ValueError("accountId must not be empty")
        batch = producer.create_batch(partition_key=envelope["accountId"])
        batch.add(EventData(line))
        if sent and interval_seconds:
            time.sleep(interval_seconds)
        producer.send_batch(batch)
        sent += 1
    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish simulated VSAM change events to Azure Event Hubs.")
    parser.add_argument("--events", required=True)
    parser.add_argument("--connection-string", default=os.getenv("EVENTHUB_CONNECTION_STRING"))
    parser.add_argument("--fully-qualified-namespace", default=os.getenv("EVENTHUB_FULLY_QUALIFIED_NAMESPACE"))
    parser.add_argument("--eventhub-name", default=os.getenv("EVENTHUB_NAME", "vsam-changes"))
    parser.add_argument("--interval-seconds", type=float, default=0.0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    event_lines = [line for line in Path(args.events).read_text(encoding="utf-8").splitlines() if line.strip()]
    if args.dry_run:
        first = json.loads(event_lines[0]) if event_lines else None
        print(json.dumps({"events": len(event_lines), "firstEvent": first}, indent=2))
        return

    if args.connection_string:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=args.connection_string,
            eventhub_name=args.eventhub_name,
        )
    elif args.fully_qualified_namespace:
        producer = EventHubProducerClient(
            fully_qualified_namespace=args.fully_qualified_namespace,
            eventhub_name=args.eventhub_name,
            credential=DefaultAzureCredential(),
        )
    else:
        raise ValueError(
            "Set EVENTHUB_CONNECTION_STRING or EVENTHUB_FULLY_QUALIFIED_NAMESPACE. "
            "Use the namespace form when local authentication is disabled."
        )

    with producer:
        sent = publish(producer, event_lines, args.interval_seconds)

    print(f"Published {sent} synthetic raw envelopes to {args.eventhub_name}; not sink acknowledgements.")


if __name__ == "__main__":
    main()
