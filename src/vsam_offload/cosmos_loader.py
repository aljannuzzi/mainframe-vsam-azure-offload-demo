from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from azure.cosmos import CosmosClient
from azure.identity import DefaultAzureCredential


def main() -> None:
    parser = argparse.ArgumentParser(description="Legacy create-only bootstrap, NOT a CDC consumer.")
    parser.add_argument("--input", required=True)
    parser.add_argument("--endpoint", default=os.getenv("COSMOS_ENDPOINT"))
    parser.add_argument("--database", default=os.getenv("COSMOS_DATABASE", "mainframeOffload"))
    parser.add_argument("--container", default=os.getenv("COSMOS_CONTAINER", "balances"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bootstrap", action="store_true", help="Explicitly create legacy records; never upsert.")
    args = parser.parse_args()

    documents = [json.loads(line) for line in Path(args.input).read_text(encoding="utf-8").splitlines() if line.strip()]
    if args.dry_run:
        print(json.dumps({"documents": len(documents), "firstDocument": documents[0] if documents else None}, indent=2))
        return

    if not args.bootstrap:
        parser.error("Use eventhub_consumer for synchronization; --bootstrap is legacy create-only loading.")
    if not args.endpoint:
        raise ValueError("Set COSMOS_ENDPOINT or pass --endpoint.")

    with DefaultAzureCredential() as credential, CosmosClient(args.endpoint, credential=credential) as client:
        container = client.get_database_client(args.database).get_container_client(args.container)
        for document in documents:
            container.create_item(document)

    print(f"Created {len(documents)} legacy documents in {args.database}/{args.container}; not synchronized.")


if __name__ == "__main__":
    main()
