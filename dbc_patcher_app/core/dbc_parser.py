"""Parser utilities for DBC files using cantools.

This module wraps cantools to provide normalized dataclass-based
representations of DBC content that are convenient for diffing and
patching operations.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional
import datetime

import cantools
from cantools.database import Database, Message, Signal


@dataclass
class DBCSignal:
    """Dataclass describing a DBC signal."""

    name: str
    start_bit: int
    length: int
    byte_order: str
    is_signed: bool
    scale: float
    offset: float
    minimum: Optional[float]
    maximum: Optional[float]
    unit: Optional[str]
    comment: Optional[str]
    value_table: Dict[str, str] = field(default_factory=dict)
    multiplex: Optional[str] = None
    multiplexer_ids: Optional[List[int]] = None


@dataclass
class DBCMessage:
    """Dataclass describing a DBC message."""

    message_id: int
    name: str
    length: int
    cycle_time: Optional[int]
    comment: Optional[str]
    attributes: Dict[str, str] = field(default_factory=dict)
    signals: List[DBCSignal] = field(default_factory=list)

    @property
    def hex_id(self) -> str:
        return hex(self.message_id)


@dataclass
class DBCModel:
    """Wrapper around a cantools Database with normalized structures."""

    db: Database
    messages: Dict[int, DBCMessage]
    source_path: Optional[Path] = None
    loaded_at: datetime.datetime = field(default_factory=datetime.datetime.utcnow)


class DBCParser:
    """Provides loading and saving helpers for DBC files."""

    def load_dbc(self, path: Path) -> DBCModel:
        """Load and normalize a DBC file.

        Args:
            path: Path to the DBC file.

        Returns:
            DBCModel containing normalized data.
        """

        db = cantools.database.load_file(str(path))
        messages: Dict[int, DBCMessage] = {}

        for msg in sorted(db.messages, key=lambda m: m.frame_id):
            signals = self._normalize_signals(msg)
            attributes = self._extract_message_attributes(msg)
            messages[msg.frame_id] = DBCMessage(
                message_id=msg.frame_id,
                name=msg.name,
                length=msg.length,
                cycle_time=msg.cycle_time,
                comment=msg.comment,
                attributes=attributes,
                signals=signals,
            )

        return DBCModel(db=db, messages=messages, source_path=Path(path))

    def save_dbc(self, model: DBCModel, path: Path) -> None:
        """Persist a DBCModel to disk using cantools serialization."""

        with Path(path).open("w", encoding="utf-8") as f:
            f.write(model.db.as_dbc_string())

    def _normalize_signals(self, message: Message) -> List[DBCSignal]:
        signals: List[DBCSignal] = []
        for sig in sorted(message.signals, key=lambda s: (s.start, s.length, s.name)):
            value_table = sig.choices or {}
            multiplex = None
            is_multiplexer = getattr(sig, "is_multiplexer", False)
            multiplexer_ids = getattr(sig, "multiplexer_ids", None)
            if is_multiplexer:
                multiplex = "MUX"
            elif multiplexer_ids:
                multiplex = "SUB"
            signals.append(
                DBCSignal(
                    name=sig.name,
                    start_bit=sig.start,
                    length=sig.length,
                    byte_order="motorola" if sig.byte_order == "big_endian" else "intel",
                    is_signed=sig.is_signed,
                    scale=sig.scale or 1.0,
                    offset=sig.offset or 0.0,
                    minimum=sig.minimum,
                    maximum=sig.maximum,
                    unit=sig.unit,
                    comment=sig.comment,
                    value_table={str(k): str(v) for k, v in value_table.items()},
                    multiplex=multiplex,
                    multiplexer_ids=multiplexer_ids if multiplexer_ids else None,
                )
            )
        return signals

    def update_database_from_model(self, model: DBCModel) -> None:
        """Update the underlying cantools Database based on the dataclass model."""

        messages_by_id = {m.frame_id: m for m in model.db.messages}
        for msg_id, msg_data in model.messages.items():
            ct_message = messages_by_id.get(msg_id)
            if not ct_message:
                continue
            ct_message.name = msg_data.name
            ct_message.length = msg_data.length
            ct_message.comment = msg_data.comment
            ct_message.cycle_time = msg_data.cycle_time
            self._update_message_attributes(ct_message, msg_data.attributes)
            self._update_signals(ct_message, msg_data.signals)

    def _update_signals(self, ct_message: Message, signals: List[DBCSignal]) -> None:
        """Replace cantools signals with values from dataclasses."""

        def create_signal(sig: DBCSignal) -> Signal:
            multiplexer_ids = sig.multiplexer_ids if sig.multiplexer_ids else None
            return Signal(
                name=sig.name,
                start=sig.start_bit,
                length=sig.length,
                byte_order="big_endian" if sig.byte_order == "motorola" else "little_endian",
                is_signed=sig.is_signed,
                scale=sig.scale,
                offset=sig.offset,
                minimum=sig.minimum,
                maximum=sig.maximum,
                unit=sig.unit,
                comment=sig.comment,
                choices=sig.value_table or None,
                is_multiplexer=sig.multiplex == "MUX",
                multiplexer_ids=multiplexer_ids,
            )

        ct_message.signals.clear()
        for sig in signals:
            ct_message.signals.append(create_signal(sig))

    def _extract_message_attributes(self, msg: Message) -> Dict[str, str]:
        """Fetch message attributes defensively across cantools versions."""

        def get_attributes_from_specifics(specifics: object) -> Dict[str, object]:
            if specifics is None:
                return {}
            if hasattr(specifics, "attributes"):
                return getattr(specifics, "attributes") or {}
            if hasattr(specifics, "attribute_values"):
                return getattr(specifics, "attribute_values") or {}
            return {}

        raw_attrs: Dict[str, object]
        if hasattr(msg, "attributes"):
            raw_attrs = getattr(msg, "attributes") or {}
        elif hasattr(msg, "dbc_specifics"):
            raw_attrs = get_attributes_from_specifics(getattr(msg, "dbc_specifics"))
        elif hasattr(msg, "_dbc_specifics"):
            raw_attrs = get_attributes_from_specifics(getattr(msg, "_dbc_specifics"))
        else:
            raw_attrs = {}

        return {str(k): str(v) for k, v in raw_attrs.items()}

    def _update_message_attributes(self, ct_message: Message, attributes: Dict[str, str]) -> None:
        """Update attributes on a cantools Message, handling API differences."""

        if hasattr(ct_message, "attributes"):
            ct_message.attributes = attributes
            return

        specifics = None
        if hasattr(ct_message, "dbc_specifics"):
            specifics = getattr(ct_message, "dbc_specifics")
        elif hasattr(ct_message, "_dbc_specifics"):
            specifics = getattr(ct_message, "_dbc_specifics")

        if specifics is None:
            return

        if hasattr(specifics, "attributes"):
            setattr(specifics, "attributes", attributes)
        elif hasattr(specifics, "attribute_values"):
            setattr(specifics, "attribute_values", attributes)

