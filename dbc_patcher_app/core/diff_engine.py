"""Diff generation between raw and cleaned DBC models."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .dbc_parser import DBCModel, DBCMessage, DBCSignal


@dataclass
class DiffRule:
    op: str
    message_id: str
    signal_match: Optional[Dict[str, int]] = None
    signal_name: Optional[str] = None
    changes: Optional[Dict[str, Dict[str, object]]] = None
    from_ref: bool = False

    def to_dict(self) -> Dict[str, object]:
        data: Dict[str, object] = {"op": self.op, "message_id": self.message_id}
        if self.signal_match:
            data["signal_match"] = self.signal_match
        if self.signal_name:
            data["signal_name"] = self.signal_name
        if self.changes:
            data["changes"] = self.changes
        if self.from_ref:
            data["from_ref"] = True
        return data


class DiffEngine:
    """Compute domain-specific patch JSON from two DBC models."""

    def generate_patch(self, raw: DBCModel, cleaned: DBCModel) -> Dict[str, object]:
        rules: List[DiffRule] = []

        raw_messages = raw.messages
        clean_messages = cleaned.messages

        raw_ids = set(raw_messages.keys())
        clean_ids = set(clean_messages.keys())

        for msg_id in sorted(clean_ids - raw_ids):
            rules.append(DiffRule(op="add_message", message_id=hex(msg_id)))

        for msg_id in sorted(raw_ids - clean_ids):
            rules.append(DiffRule(op="remove_message", message_id=hex(msg_id)))

        for msg_id in sorted(clean_ids & raw_ids):
            rules.extend(self._compare_message(raw_messages[msg_id], clean_messages[msg_id]))

        return {
            "version": 1,
            "created": datetime.utcnow().isoformat(),
            "rules": [r.to_dict() for r in rules],
        }

    def _compare_message(self, raw_msg: DBCMessage, clean_msg: DBCMessage) -> List[DiffRule]:
        rules: List[DiffRule] = []

        raw_signals = {s.name: s for s in raw_msg.signals}
        clean_signals = {s.name: s for s in clean_msg.signals}

        raw_names = set(raw_signals.keys())
        clean_names = set(clean_signals.keys())

        for sig in clean_names - raw_names:
            rules.append(
                DiffRule(
                    op="add_signal_if_missing",
                    message_id=clean_msg.hex_id,
                    signal_name=sig,
                )
            )

        for sig in raw_names - clean_names:
            rules.append(
                DiffRule(
                    op="remove_signal",
                    message_id=clean_msg.hex_id,
                    signal_name=sig,
                )
            )

        if raw_msg.senders != clean_msg.senders:
            rules.append(
                DiffRule(
                    op="update_message_senders",
                    message_id=clean_msg.hex_id,
                    changes={"senders": {"from": raw_msg.senders, "to": clean_msg.senders}},
                )
            )

        for name in raw_names & clean_names:
            rules.extend(
                self._compare_signal(raw_msg, raw_signals[name], clean_signals[name])
            )

        renamed = self._detect_renames(raw_msg.signals, clean_msg.signals)
        for old, new in renamed:
            rules.append(
                DiffRule(
                    op="rename_signal",
                    message_id=clean_msg.hex_id,
                    signal_match={"start_bit": old.start_bit, "length": old.length},
                    signal_name=new.name,
                )
            )

        return rules

    def _compare_signal(
        self, message: DBCMessage, raw_sig: DBCSignal, clean_sig: DBCSignal
    ) -> List[DiffRule]:
        changes: Dict[str, Dict[str, object]] = {}
        fields = [
            "name",
            "start_bit",
            "length",
            "byte_order",
            "is_signed",
            "scale",
            "offset",
            "minimum",
            "maximum",
            "unit",
            "comment",
            "value_table",
            "receivers",
        ]

        for field in fields:
            raw_val = getattr(raw_sig, field)
            clean_val = getattr(clean_sig, field)
            if raw_val != clean_val:
                changes[field] = {"from": raw_val, "to": clean_val}

        if not changes:
            return []

        return [
            DiffRule(
                op="update_signal",
                message_id=message.hex_id,
                signal_match={"start_bit": raw_sig.start_bit, "length": raw_sig.length},
                changes=changes,
            )
        ]

    def _detect_renames(
        self, raw_signals: List[DBCSignal], clean_signals: List[DBCSignal]
    ) -> List[Tuple[DBCSignal, DBCSignal]]:
        matches: List[Tuple[DBCSignal, DBCSignal]] = []
        for raw_sig in raw_signals:
            for clean_sig in clean_signals:
                if raw_sig.name == clean_sig.name:
                    continue
                if (
                    raw_sig.start_bit == clean_sig.start_bit
                    and raw_sig.length == clean_sig.length
                    and raw_sig.byte_order == clean_sig.byte_order
                ):
                    matches.append((raw_sig, clean_sig))
        return matches

