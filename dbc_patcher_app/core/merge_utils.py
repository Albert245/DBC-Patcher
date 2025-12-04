"""Utilities for safely cloning and merging cantools database objects."""
from __future__ import annotations

from copy import deepcopy
from typing import Iterable

from cantools.database import Database
from cantools.database.can import Message, Signal


def clone_signal(sig: Signal) -> Signal:
    """Return a deep copy of a cantools ``Signal``.

    Cantools instances reference their parent message/database. A deep copy
    detaches the clone so it can be inserted into another database safely.
    """

    return deepcopy(sig)


def clone_message(msg: Message) -> Message:
    """Return a deep copy of a cantools ``Message`` including its signals."""

    return deepcopy(msg)


def _clone_iterable(items: Iterable) -> list:
    return [deepcopy(item) for item in items]


def insert_message_into_database(db: Database, message: Message) -> Database:
    """Create a new Database with an additional message.

    Cantools does not support mutating ``db.messages`` directly, so we rebuild a
    new database instance copying all existing metadata and appending the
    cloned message.
    """

    new_db = Database(
        messages=_clone_iterable(db.messages) + [clone_message(message)],
        nodes=_clone_iterable(getattr(db, "nodes", []) or []),
        buses=_clone_iterable(getattr(db, "buses", []) or []),
        version=getattr(db, "version", None),
        attributes=getattr(db, "attributes", None),
        choices=getattr(db, "choices", None),
    )

    # Copy protocol if available (cantools stores it privately in some versions)
    if hasattr(db, "protocol"):
        new_db.protocol = deepcopy(getattr(db, "protocol"))

    # Preserve default bus if present.
    if hasattr(db, "dbc") and hasattr(db.dbc, "buses"):
        existing_buses = getattr(db.dbc, "buses") or []
        new_db.dbc.buses = _clone_iterable(existing_buses)

    return new_db
