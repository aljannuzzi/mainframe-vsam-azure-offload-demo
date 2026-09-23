"""Destination CAS and durable poison storage; no sink/transport distributed transaction."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Callable, Protocol

from azure.cosmos.exceptions import CosmosHttpResponseError

from .sync_contract import canonical, fingerprint

Transition = Callable[[dict | None], tuple[dict | None, str, str | None]]


class Store(Protocol):
    def apply(self, key: str, transition: Transition) -> tuple[str, str | None]: ...
    def quarantine(self, payload: object, reason: str, transport: dict) -> str: ...


def quarantine_document(payload: object, reason: str, transport: dict) -> dict:
    identity = fingerprint({"payload": payload, "transport": transport})
    return {"id": identity, "accountId": identity, "payload": payload, "reason": reason,
            "transport": transport, "eventHash": fingerprint(payload), "resolved": False}


class SQLiteStore:
    """Local emulator only. Each apply, quarantine and checkpoint is a separate commit."""

    def __init__(self, path: str | Path, *, stream: str = "local-demo", group: str = "sync-demo"):
        self.stream, self.group = stream, group
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute("PRAGMA synchronous=FULL")
        self.connection.executescript("""
            CREATE TABLE IF NOT EXISTS documents (id TEXT PRIMARY KEY, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS quarantine (id TEXT PRIMARY KEY, body TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS checkpoints (
                stream TEXT, consumer_group TEXT, partition TEXT, offset TEXT NOT NULL,
                PRIMARY KEY(stream, consumer_group, partition));
            CREATE TABLE IF NOT EXISTS audit (sequence INTEGER PRIMARY KEY, body TEXT NOT NULL);
        """)

    def get(self, key: str) -> dict | None:
        row = self.connection.execute("SELECT body FROM documents WHERE id=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def apply(self, key: str, transition: Transition) -> tuple[str, str | None]:
        with self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            document, outcome, reason = transition(self.get(key))
            if document is not None:
                self.connection.execute("INSERT INTO documents VALUES (?, ?) "
                                        "ON CONFLICT(id) DO UPDATE SET body=excluded.body",
                                        (key, canonical(document)))
        return outcome, reason

    def quarantine(self, payload: object, reason: str, transport: dict) -> str:
        doc = quarantine_document(payload, reason, transport)
        with self.connection:
            self.connection.execute("INSERT OR IGNORE INTO quarantine VALUES (?, ?)", (doc["id"], canonical(doc)))
        return doc["id"]

    def resolve_quarantine(self, event: dict) -> None:
        """Explicit operator acknowledgement after a successful replay; retain the evidence."""
        with self.connection:
            for doc in self.quarantines():
                if doc["eventHash"] == fingerprint(event):
                    doc["resolved"] = True
                    self.connection.execute("UPDATE quarantine SET body=? WHERE id=?", (canonical(doc), doc["id"]))

    def documents(self) -> list[dict]:
        return [json.loads(row[0]) for row in self.connection.execute("SELECT body FROM documents ORDER BY id")]

    def quarantines(self) -> list[dict]:
        return [json.loads(row[0]) for row in self.connection.execute("SELECT body FROM quarantine ORDER BY id")]

    def checkpoint(self, partition: str) -> str | None:
        row = self.connection.execute("SELECT offset FROM checkpoints WHERE stream=? AND consumer_group=? "
                                      "AND partition=?", (self.stream, self.group, partition)).fetchone()
        return row[0] if row else None

    def save_checkpoint(self, partition: str, offset: int | str) -> None:
        # Offsets are opaque; callers deliver each partition serially, in transport order.
        with self.connection:
            self.connection.execute("INSERT INTO checkpoints VALUES (?, ?, ?, ?) ON CONFLICT "
                                    "(stream, consumer_group, partition) DO UPDATE SET offset=excluded.offset",
                                    (self.stream, self.group, partition, str(offset)))

    def audit(self, entry: dict) -> None:
        with self.connection:
            self.connection.execute("INSERT INTO audit(body) VALUES (?)", (canonical(entry),))

    def timeline(self) -> list[dict]:
        return [json.loads(row[0]) for row in self.connection.execute("SELECT body FROM audit ORDER BY sequence")]

    def close(self) -> None:
        self.connection.close()


class CosmosStore:
    """Pre-provisioned containers partitioned by /accountId; requires trustworthy _sync state.

    Use a dedicated empty balances container, not legacy bootstrap documents.
    Cosmos and Blob checkpoints cannot commit atomically. SDK retry policy still applies.
    """

    def __init__(self, container, quarantine_container, *, max_attempts: int = 5):
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        self.container, self.quarantine_container = container, quarantine_container
        self.max_attempts = max_attempts

    def get(self, key: str) -> dict | None:
        try:
            return self.container.read_item(item=key, partition_key=key)
        except CosmosHttpResponseError as exc:
            if exc.status_code == 404:
                return None
            raise

    def apply(self, key: str, transition: Transition) -> tuple[str, str | None]:
        from azure.core import MatchConditions

        for attempt in range(self.max_attempts):
            current = self.get(key)
            document, outcome, reason = transition(current)
            if document is None:
                return outcome, reason
            try:
                if current is None:
                    self.container.create_item(body=document)
                else:
                    self.container.replace_item(item=key, body=document, etag=current["_etag"],
                                                match_condition=MatchConditions.IfNotModified)
                return outcome, reason
            except CosmosHttpResponseError as exc:
                if exc.status_code not in (409, 412) or attempt + 1 == self.max_attempts:
                    raise
        raise AssertionError("unreachable")

    def quarantine(self, payload: object, reason: str, transport: dict) -> str:
        doc = quarantine_document(payload, reason, transport)
        try:
            self.quarantine_container.create_item(body=doc)
        except CosmosHttpResponseError as exc:
            if exc.status_code != 409:
                raise
            existing = self.quarantine_container.read_item(item=doc["id"], partition_key=doc["accountId"])
            if any(existing.get(key) != doc[key] for key in ("payload", "transport", "eventHash")):
                raise ValueError("quarantine identity collision") from exc
        return doc["id"]
