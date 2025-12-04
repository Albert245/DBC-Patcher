"""Parser utilities for DBC files using cantools.

This module wraps cantools to provide normalized dataclass-based
representations of DBC content that are convenient for diffing and
patching operations.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional
import datetime

import cantools
from cantools.database import Database, Message
from cantools.database.can import Node, Signal


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
    minimum: Optional[float] = None
    maximum: Optional[float] = None
    unit: Optional[str] = None
    comment: Optional[str] = None
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
    cycle_time: Optional[int] = None
    comment: Optional[str] = None
    is_extended_frame: bool = False
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
    version: str = ""
    nodes: List[str] = field(default_factory=list)
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
        return self._build_model_from_db(db, Path(path))

    def _build_model_from_db(
        self, db: Database, source_path: Optional[Path] = None
    ) -> DBCModel:
        """Normalize a cantools Database into a DBCModel."""

        messages: Dict[int, DBCMessage] = {}

        for msg in sorted(db.messages, key=lambda m: m.frame_id):
            signals = self._normalize_signals(msg)
            attributes = self._extract_message_attributes(msg)
            is_extended = bool(
                getattr(msg, "is_extended_frame", False)
                or getattr(msg, "is_extended", False)
            )
            messages[msg.frame_id] = DBCMessage(
                message_id=msg.frame_id,
                name=msg.name,
                length=msg.length,
                is_extended_frame=is_extended,
                cycle_time=getattr(msg, "cycle_time", None),
                comment=getattr(msg, "comment", None),
                attributes=attributes,
                senders=list(getattr(msg, "senders", []) or []),
                signals=signals,
            )

        version = getattr(db, "version", "") or ""
        nodes = [n.name for n in getattr(db, "nodes", []) or [] if getattr(n, "name", None)]

        return DBCModel(
            db=db,
            messages=messages,
            version=version,
            nodes=nodes,
            source_path=source_path,
        )

    def save_dbc(self, model: DBCModel, path: Path, validate: bool = True) -> None:
        """Persist a DBCModel to disk using cantools serialization."""

        db_dict = self.model_to_dict(model)
        dbc_text = generate_dbc_text_from_dict(db_dict, validate=validate)

        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(dbc_text, encoding="utf-8")

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
        """Rebuild the underlying cantools Database from dataclass content."""

        db_dict = self.model_to_dict(model)
        new_db = build_database_from_dict(db_dict)
        refreshed_db = cantools.database.load_string(new_db.as_dbc_string())
        refreshed = self._build_model_from_db(refreshed_db, model.source_path)

        model.db = refreshed.db
        model.messages = refreshed.messages
        model.version = refreshed.version
        model.nodes = refreshed.nodes
        model.loaded_at = refreshed.loaded_at

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

    def _signal_to_dict(self, signal: DBCSignal) -> Dict[str, object]:
        """Convert a DBCSignal into a serializable dictionary."""

        return {
            "name": signal.name,
            "start_bit": signal.start_bit,
            "length": signal.length,
            "byte_order": signal.byte_order,
            "is_signed": signal.is_signed,
            "scale": signal.scale,
            "offset": signal.offset,
            "minimum": signal.minimum,
            "maximum": signal.maximum,
            "unit": signal.unit,
            "comment": signal.comment,
            "value_table": signal.value_table,
            "multiplex": signal.multiplex,
            "multiplexer_ids": signal.multiplexer_ids,
            "receivers": signal.receivers,
        }

    def _message_to_dict(self, message: DBCMessage) -> Dict[str, object]:
        """Convert a DBCMessage into a serializable dictionary."""

        return {
            "frame_id": message.message_id,
            "name": message.name,
            "length": message.length,
            "is_extended_frame": message.is_extended_frame,
            "cycle_time": message.cycle_time,
            "comment": message.comment,
            "attributes": message.attributes,
            "senders": message.senders,
            "signals": [self._signal_to_dict(sig) for sig in message.signals],
        }

    def model_to_dict(self, model: DBCModel) -> Dict[str, object]:
        """Export a dataclass model into a dictionary representation."""

        nodes = list(model.nodes)
        if not nodes:
            node_set = set()
            for msg in model.messages.values():
                node_set.update(msg.senders)
                for sig in msg.signals:
                    node_set.update(sig.receivers)
            nodes = sorted(node_set)

        return {
            "version": model.version,
            "nodes": nodes,
            "messages": [
                self._message_to_dict(msg)
                for msg in sorted(model.messages.values(), key=lambda m: m.message_id)
            ],
        }


def export_message_to_dict(message: Message) -> Dict[str, object]:
    """Convert a cantools Message into a plain dictionary."""

    parser = DBCParser()
    signals = parser._normalize_signals(message)
    attributes = parser._extract_message_attributes(message)
    is_extended = bool(
        getattr(message, "is_extended_frame", False) or getattr(message, "is_extended", False)
    )

    msg_data = DBCMessage(
        message_id=message.frame_id,
        name=message.name,
        length=message.length,
        is_extended_frame=is_extended,
        cycle_time=getattr(message, "cycle_time", None),
        comment=getattr(message, "comment", None),
        attributes=attributes,
        senders=list(getattr(message, "senders", []) or []),
        signals=signals,
    )

    return parser._message_to_dict(msg_data)


def export_db_to_dict(database: Database) -> Dict[str, object]:
    """Convert a cantools Database into a serializable dictionary."""

    parser = DBCParser()
    model = parser._build_model_from_db(database)
    return parser.model_to_dict(model)


def generate_dbc_text_from_dict(db_dict: Dict[str, object], validate: bool = True) -> str:
    """Construct a DBC string from a dictionary representation."""

    db = build_database_from_dict(db_dict)
    dbc_text = db.as_dbc_string()
    if validate:
        cantools.database.load_string(dbc_text)
    return dbc_text


def _build_signal_from_dict(data: Dict[str, object]) -> Signal:
    receivers = list(data.get("receivers", []) or [])
    byte_order = "big_endian" if data.get("byte_order") == "motorola" else "little_endian"
    choices: Dict[object, str] = {}
    for raw_key, raw_value in (data.get("value_table") or {}).items():
        try:
            key: object = int(raw_key)
        except Exception:
            key = str(raw_key)
        choices[key] = str(raw_value)

    minimum = data.get("minimum")
    maximum = data.get("maximum")
    multiplex_field = data.get("multiplex")
    is_multiplexer = multiplex_field == "MUX"
    multiplexer_ids_raw = data.get("multiplexer_ids") or []
    multiplexer_ids = [int(mid) for mid in multiplexer_ids_raw] if multiplexer_ids_raw else None

    if not is_multiplexer and multiplex_field not in (None, "", "MUX") and not multiplexer_ids:
        try:
            multiplexer_ids = [int(multiplex_field)]
        except Exception:
            multiplexer_ids = None

    scale = float(data.get("scale", 1.0))
    offset = float(data.get("offset", 0.0))
    is_float = bool(data.get("is_float", False))
    conversion = data.get("conversion") or data.get("decimal")
    if conversion is not None:
        scale = float(getattr(conversion, "scale", scale))
        offset = float(getattr(conversion, "offset", offset))
        is_float = bool(getattr(conversion, "is_float", is_float))
        conv_choices = getattr(conversion, "choices", None)
        if conv_choices:
            normalized_choices: Dict[object, str] = {}
            for raw_key, raw_value in conv_choices.items():
                try:
                    norm_key: object = int(raw_key)
                except Exception:
                    norm_key = str(raw_key)
                normalized_choices[norm_key] = str(raw_value)
            choices = normalized_choices

    signal = Signal(
        name=str(data.get("name", "")),
        start=int(data.get("start_bit", 0)),
        length=int(data.get("length", 1)),
        byte_order=byte_order,
        is_signed=bool(data.get("is_signed", False)),
        scale=scale,
        offset=offset,
        minimum=float(minimum) if minimum is not None else None,
        maximum=float(maximum) if maximum is not None else None,
        unit=data.get("unit"),
        choices=choices or None,
        comment=data.get("comment"),
        receivers=receivers,
        is_multiplexer=is_multiplexer,
        multiplexer_ids=multiplexer_ids,
        multiplexer_signal=data.get("multiplexer_signal"),
        is_float=is_float,
    )

    return signal


def _build_message_from_dict(data: Dict[str, object]) -> Message:
    signals = [_build_signal_from_dict(sig) for sig in data.get("signals", []) or []]
    senders = list(data.get("senders", []) or [])

    message = Message(
        frame_id=int(data.get("frame_id", 0)),
        name=str(data.get("name", f"MSG_{data.get('frame_id', 0)}")),
        length=int(data.get("length", 8)),
        signals=signals,
        senders=senders or None,
        cycle_time=data.get("cycle_time"),
        is_extended_frame=bool(data.get("is_extended_frame", False)),
        comment=data.get("comment"),
    )

    attributes = data.get("attributes") or {}
    if attributes:
        setattr(message, "attributes", {str(k): v for k, v in attributes.items()})

    return message


def build_database_from_dict(db_dict: Dict[str, object]) -> Database:
    nodes = [Node(name=str(node)) for node in db_dict.get("nodes", []) or []]
    messages = [_build_message_from_dict(msg) for msg in db_dict.get("messages", []) or []]
    version = str(db_dict.get("version", "")) if db_dict.get("version") is not None else ""

    database = cantools.database.Database(
        messages=messages,
        nodes=nodes,
        version=version,
        attributes=None,
    )
    return database

