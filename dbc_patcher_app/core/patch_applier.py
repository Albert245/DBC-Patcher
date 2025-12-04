"""Patch application logic for DBC models."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, List, Tuple

from cantools.database.can import Message

from .dbc_parser import (
    DBCModel,
    DBCMessage,
    DBCSignal,
    DBCParser,
    build_database_from_dict,
)
from .merge_utils import insert_message_into_database
from .ref_db import ReferenceDB


@dataclass
class PatchResult:
    new_model: DBCModel
    applied: List[Dict[str, object]]
    skipped: List[Dict[str, object]]
    conflicts: List[Dict[str, object]]


class PatchApplier:
    """Apply patch JSON on top of a DBC model."""

    def __init__(self, parser: DBCParser, ref_db: ReferenceDB) -> None:
        self.parser = parser
        self.ref_db = ref_db

    def apply_patch(self, model: DBCModel, patch: Dict[str, object]) -> PatchResult:
        applied: List[Dict[str, object]] = []
        skipped: List[Dict[str, object]] = []
        conflicts: List[Dict[str, object]] = []

        rules: List[Dict[str, object]] = patch.get("rules", [])  # type: ignore

        for rule in rules:
            op = rule.get("op")
            handler = getattr(self, f"_handle_{op}", None)
            if not handler:
                skipped.append({"rule": rule, "reason": "unsupported"})
                continue
            status, info = handler(model, rule)
            if status == "applied":
                applied.append(info)
            elif status == "conflict":
                conflicts.append(info)
            else:
                skipped.append(info)

        self.parser.update_database_from_model(model)
        return PatchResult(new_model=model, applied=applied, skipped=skipped, conflicts=conflicts)

    def _message_by_id(self, model: DBCModel, msg_hex: str) -> Tuple[str, DBCMessage | None]:
        msg_id = int(msg_hex, 16)
        return msg_hex, model.messages.get(msg_id)

    def _find_signal_by_match(self, message: DBCMessage, match: Dict[str, int]) -> DBCSignal | None:
        for sig in message.signals:
            if sig.start_bit == match.get("start_bit") and sig.length == match.get("length"):
                return sig
        return None

    def _refresh_model(self, model: DBCModel, database) -> None:
        refreshed = self.parser._build_model_from_db(database, model.source_path)
        model.db = refreshed.db
        model.messages = refreshed.messages
        model.version = refreshed.version
        model.nodes = refreshed.nodes
        model.loaded_at = refreshed.loaded_at

    def _signal_from_dict(self, data: Dict[str, object]) -> DBCSignal:
        return DBCSignal(
            name=str(data.get("name")),
            start_bit=int(data.get("start_bit", 0)),
            length=int(data.get("length", 1)),
            byte_order=str(data.get("byte_order", "intel")),
            is_signed=bool(data.get("is_signed", False)),
            scale=float(data.get("scale", 1.0)),
            offset=float(data.get("offset", 0.0)),
            minimum=data.get("minimum"),
            maximum=data.get("maximum"),
            unit=data.get("unit"),
            comment=data.get("comment"),
            value_table={str(k): str(v) for k, v in (data.get("value_table") or {}).items()},
            multiplex=data.get("multiplex"),
            multiplexer_ids=data.get("multiplexer_ids"),
            receivers=list(data.get("receivers", []) or []),
        )

    def _cantools_message_from_dict(self, data: Dict[str, object]) -> Message:
        temp_db = build_database_from_dict({"version": "", "nodes": [], "messages": [data]})
        return temp_db.messages[0]

    def _handle_update_signal(self, model: DBCModel, rule: Dict[str, object]):
        msg_hex, message = self._message_by_id(model, rule["message_id"])
        if not message:
            return "skipped", {"rule": rule, "reason": "message missing"}
        match = rule.get("signal_match", {})
        signal = self._find_signal_by_match(message, match) if match else None
        if not signal:
            return "skipped", {"rule": rule, "reason": "signal missing"}

        changes = rule.get("changes", {})
        for field, change in changes.items():
            expected = change.get("from")
            new_val = change.get("to")
            current = getattr(signal, field, None)
            if expected is not None and current != expected:
                return "conflict", {
                    "rule": rule,
                    "reason": f"expected {expected} but found {current}",
                }
            setattr(signal, field, new_val)
        return "applied", {"rule": rule}

    def _handle_add_signal_if_missing(self, model: DBCModel, rule: Dict[str, object]):
        return self._handle_add_signal(model, rule)

    def _handle_add_signal(self, model: DBCModel, rule: Dict[str, object]):
        msg_hex, message = self._message_by_id(model, rule["message_id"])
        if not message:
            return "skipped", {"rule": rule, "reason": "message missing"}
        sig_name = rule.get("signal_name")
        if not sig_name:
            return "skipped", {"rule": rule, "reason": "signal unspecified"}
        if any(sig.name == sig_name for sig in message.signals):
            return "skipped", {"rule": rule, "reason": "already exists"}

        template = self.ref_db.suggest_for_signal(str(sig_name))
        if template:
            new_signal = deepcopy(template)
        elif rule.get("signal"):
            new_signal = self._signal_from_dict(rule["signal"])
        else:
            new_signal = DBCSignal(
                name=str(sig_name),
                start_bit=0,
                length=8,
                byte_order="intel",
                is_signed=False,
                scale=1.0,
                offset=0.0,
                minimum=None,
                maximum=None,
                unit=None,
                comment=None,
                value_table={},
            )
        message.signals.append(new_signal)
        return "applied", {"rule": rule}

    def _handle_remove_signal(self, model: DBCModel, rule: Dict[str, object]):
        msg_hex, message = self._message_by_id(model, rule["message_id"])
        if not message:
            return "skipped", {"rule": rule, "reason": "message missing"}
        sig_name = rule.get("signal_name")
        before = len(message.signals)
        message.signals = [s for s in message.signals if s.name != sig_name]
        if len(message.signals) == before:
            return "skipped", {"rule": rule, "reason": "not found"}
        return "applied", {"rule": rule}

    def _handle_rename_signal(self, model: DBCModel, rule: Dict[str, object]):
        msg_hex, message = self._message_by_id(model, rule["message_id"])
        if not message:
            return "skipped", {"rule": rule, "reason": "message missing"}
        match = rule.get("signal_match", {})
        signal = self._find_signal_by_match(message, match)
        if not signal:
            return "skipped", {"rule": rule, "reason": "signal missing"}
        new_name = rule.get("signal_name")
        if not new_name:
            return "skipped", {"rule": rule, "reason": "new name missing"}
        signal.name = str(new_name)
        return "applied", {"rule": rule}

    def _handle_update_message_senders(self, model: DBCModel, rule: Dict[str, object]):
        msg_hex, message = self._message_by_id(model, rule["message_id"])
        if not message:
            return "skipped", {"rule": rule, "reason": "message missing"}

        changes = rule.get("changes", {})
        change = changes.get("senders", {}) if isinstance(changes, dict) else {}
        expected = change.get("from")
        new_val = change.get("to")
        current = message.senders

        if expected is not None and current != expected:
            return "conflict", {"rule": rule, "reason": f"expected {expected} but found {current}"}

        message.senders = list(new_val or [])
        return "applied", {"rule": rule}

    def _handle_add_message(self, model: DBCModel, rule: Dict[str, object]):
        msg_hex = rule.get("message_id")
        if not msg_hex:
            return "skipped", {"rule": rule, "reason": "message id missing"}
        msg_id = int(msg_hex, 16)
        if msg_id in model.messages:
            return "skipped", {"rule": rule, "reason": "message exists"}

        new_db = None
        if rule.get("message"):
            message_obj = self._cantools_message_from_dict(rule["message"])
            new_db = insert_message_into_database(model.db, message_obj)
        else:
            suggestion = self.ref_db.suggest_message(msg_id, rule.get("name") or "")
            if suggestion:
                message_obj = self._cantools_message_from_dict(self.parser._message_to_dict(suggestion))
                new_db = insert_message_into_database(model.db, message_obj)

        if new_db:
            self._refresh_model(model, new_db)
            return "applied", {"rule": rule}

        new_message = DBCMessage(
            message_id=msg_id,
            name=f"MSG_{msg_hex}",
            length=8,
            is_extended_frame=False,
            cycle_time=None,
            comment=None,
            attributes={},
            signals=[],
        )
        model.messages[msg_id] = new_message
        return "applied", {"rule": rule}

    def _handle_remove_message(self, model: DBCModel, rule: Dict[str, object]):
        msg_hex = rule.get("message_id")
        if not msg_hex:
            return "skipped", {"rule": rule, "reason": "message id missing"}
        msg_id = int(msg_hex, 16)
        if msg_id not in model.messages:
            return "skipped", {"rule": rule, "reason": "not found"}
        del model.messages[msg_id]
        return "applied", {"rule": rule}

    def _handle_update_message(self, model: DBCModel, rule: Dict[str, object]):
        msg_hex = rule.get("message_id")
        if not msg_hex:
            return "skipped", {"rule": rule, "reason": "message id missing"}
        msg_id = int(msg_hex, 16)
        if msg_id not in model.messages:
            return "skipped", {"rule": rule, "reason": "message missing"}
        message_data = rule.get("message")
        if not message_data:
            return "skipped", {"rule": rule, "reason": "no message payload"}

        db_dict = self.parser.model_to_dict(model)
        messages = db_dict.get("messages", [])
        db_dict["messages"] = [
            message_data if m.get("frame_id") == msg_id else m for m in messages
        ]
        new_db = build_database_from_dict(db_dict)
        self._refresh_model(model, new_db)
        return "applied", {"rule": rule}

