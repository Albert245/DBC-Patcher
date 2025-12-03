"""Parser utilities for DBC files using cantools.

This module wraps cantools to provide normalized dataclass-based
representations of DBC content that are convenient for diffing and
patching operations.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any
import inspect
import datetime

import cantools
from cantools.database import Database, Message, Signal

try:
    from cantools.database.conversion import LinearConversion
except Exception:  # pragma: no cover - fallback for older cantools
    LinearConversion = None


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
    receivers: List[str] = field(default_factory=list)


@dataclass
class DBCMessage:
    """Dataclass describing a DBC message."""

    message_id: int
    name: str
    length: int
    cycle_time: Optional[int]
    comment: Optional[str]
    attributes: Dict[str, str] = field(default_factory=dict)
    senders: List[str] = field(default_factory=list)
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
                senders=list(getattr(msg, "senders", []) or []),
                signals=signals,
            )

        return DBCModel(db=db, messages=messages, source_path=Path(path))

    def save_dbc(self, model: DBCModel, path: Path) -> None:
        """Persist a DBCModel to disk using cantools serialization."""

        with Path(path).open("w", encoding="utf-8") as f:
            f.write(model.db.as_dbc_string())

    def _normalize_signals(self, message: Message) -> List[DBCSignal]:
        signals: List[DBCSignal] = []

        def extract_conversion_attribute(obj: object, attr: str, default: Any) -> Any:
            return getattr(obj, attr, default) if obj is not None else default

        for sig in sorted(message.signals, key=lambda s: (s.start, s.length, s.name)):
            conversion = getattr(sig, "conversion", None)
            scale = getattr(sig, "scale", None)
            offset = getattr(sig, "offset", None)
            value_table = getattr(sig, "choices", None) or {}
            receivers = sorted(getattr(sig, "receivers", []) or [])

            if conversion is not None:
                scale = extract_conversion_attribute(conversion, "scale", scale)
                offset = extract_conversion_attribute(conversion, "offset", offset)
                conv_choices = extract_conversion_attribute(conversion, "choices", None) or {}
                if conv_choices:
                    value_table = conv_choices

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
                    scale=scale if scale is not None else 1.0,
                    offset=offset if offset is not None else 0.0,
                    minimum=getattr(sig, "minimum", None),
                    maximum=getattr(sig, "maximum", None),
                    unit=getattr(sig, "unit", None),
                    comment=getattr(sig, "comment", None),
                    value_table={str(k): str(v) for k, v in value_table.items()},
                    multiplex=multiplex,
                    multiplexer_ids=multiplexer_ids if multiplexer_ids else None,
                    receivers=receivers,
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
            if hasattr(ct_message, "senders"):
                ct_message.senders = list(msg_data.senders)
            self._update_message_attributes(ct_message, msg_data.attributes)
            self._update_signals(ct_message, msg_data.signals)

    def _update_signals(self, ct_message: Message, signals: List[DBCSignal]) -> None:
        """Replace cantools signals with values from dataclasses."""

        def build_conversion(sig: DBCSignal):
            if LinearConversion:
                try:
                    return LinearConversion(
                        scale=sig.scale,
                        offset=sig.offset,
                        is_float=False,
                        choices=sig.value_table or None,
                    )
                except Exception:
                    pass

            class SimpleConversion:
                def __init__(self, scale: float, offset: float, is_float: bool = False, choices=None):
                    self.scale = scale
                    self.offset = offset
                    self.is_float = is_float
                    self.choices = choices

            return SimpleConversion(sig.scale, sig.offset, False, sig.value_table or None)

        def create_signal(sig: DBCSignal) -> Signal:
            multiplexer_ids = sig.multiplexer_ids if sig.multiplexer_ids else None
            parameters = inspect.signature(Signal).parameters
            supports_scale = "scale" in parameters
            supports_conversion = "conversion" in parameters
            supports_receivers = "receivers" in parameters

            kwargs = dict(
                name=sig.name,
                start=sig.start_bit,
                length=sig.length,
                byte_order="big_endian" if sig.byte_order == "motorola" else "little_endian",
                is_signed=sig.is_signed,
            )

            if supports_scale:
                kwargs.update(
                    scale=sig.scale,
                    offset=sig.offset,
                    minimum=sig.minimum,
                    maximum=sig.maximum,
                    unit=sig.unit,
                    comment=sig.comment,
                    choices=sig.value_table or None,
                )
            elif supports_conversion:
                kwargs.update(
                    conversion=build_conversion(sig),
                    minimum=sig.minimum,
                    maximum=sig.maximum,
                    unit=sig.unit,
                    comment=sig.comment,
                )
                if "choices" in parameters:
                    kwargs["choices"] = sig.value_table or None

            if supports_receivers:
                kwargs["receivers"] = list(sig.receivers)

            if "is_multiplexer" in parameters:
                kwargs["is_multiplexer"] = sig.multiplex == "MUX"
            if "multiplexer_ids" in parameters:
                kwargs["multiplexer_ids"] = multiplexer_ids

            return Signal(**kwargs)

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

