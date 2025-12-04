"""Patch application logic for DBC models."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Tuple

from .dbc_parser import DBCModel, DBCMessage, DBCSignal, DBCParser
from .ref_db import ReferenceDB


@dataclass
class PatchResult:
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
        return PatchResult(applied=applied, skipped=skipped, conflicts=conflicts)

    def _message_by_id(self, model: DBCModel, msg_hex: str) -> Tuple[str, DBCMessage | None]:
        msg_id = int(msg_hex, 16)
        return msg_hex, model.messages.get(msg_id)

    def _find_signal_by_match(self, message: DBCMessage, match: Dict[str, int]) -> DBCSignal | None:
        for sig in message.signals:
            if sig.start_bit == match.get("start_bit") and sig.length == match.get("length"):
                return sig
        return None

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
        msg_hex, message = self._message_by_id(model, rule["message_id"])
        if not message:
            return "skipped", {"rule": rule, "reason": "message missing"}
        sig_name = rule.get("signal_name")
        if not sig_name:
            return "skipped", {"rule": rule, "reason": "signal unspecified"}
        if any(sig.name == sig_name for sig in message.signals):
            return "skipped", {"rule": rule, "reason": "already exists"}

        template = self.ref_db.suggest_for_signal(sig_name)
        new_signal = template or DBCSignal(
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

