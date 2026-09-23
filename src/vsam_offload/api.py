from __future__ import annotations

import os
from functools import lru_cache

from azure.cosmos import CosmosClient, exceptions
from azure.identity import DefaultAzureCredential
from fastapi import FastAPI, HTTPException


app = FastAPI(title="Mainframe offload balance API")


@lru_cache(maxsize=1)
def get_container():
    endpoint = os.getenv("COSMOS_ENDPOINT")
    database_name = os.getenv("COSMOS_DATABASE", "mainframeOffload")
    container_name = os.getenv("COSMOS_CONTAINER", "balances")
    if not endpoint:
        raise RuntimeError("COSMOS_ENDPOINT must be set")

    client = CosmosClient(endpoint, credential=DefaultAzureCredential())
    return client.get_database_client(database_name).get_container_client(container_name)


@app.get("/accounts/{account_id}/balance")
def get_balance(account_id: str):
    try:
        document = get_container().read_item(item=account_id, partition_key=account_id)
    except exceptions.CosmosResourceNotFoundError as exc:
        raise HTTPException(status_code=404, detail=f"Account {account_id} not found") from exc
    if document.get("deleted", False):
        raise HTTPException(status_code=404, detail=f"Account {account_id} not found")
    result = {key: value for key, value in document.items() if not key.startswith("_")}
    if "_sync" in document:
        result["synchronization"] = {
            key: document["_sync"][key]
            for key in ("sourcePosition", "eventId", "sourceCommittedAt", "appliedAt")
            if key in document["_sync"]
        }
    return result
