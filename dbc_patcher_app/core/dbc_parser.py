"""Parser utilities for DBC files using cantools.

This module wraps cantools to provide normalized dataclass-based
representations of DBC content that are convenient for diffing and
patching operations.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any
import datetime

import cantools
from cantools.database import Database, Message


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
        """Rebuild the underlying cantools Database from dataclass content."""

        db_dict = self.model_to_dict(model)
        dbc_text = generate_dbc_text_from_dict(db_dict)
        new_db = cantools.database.load_string(dbc_text)
        refreshed = self._build_model_from_db(new_db, model.source_path)

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


def generate_dbc_text_from_dict(db_dict: Dict[str, object]) -> str:
    """Construct a DBC string from a dictionary representation."""

    def quote(value: object) -> str:
        escaped = str(value).replace('"', '\\"')
        return f'"{escaped}"'

    def format_attr_value(value: object) -> str:
        if value is None:
            return quote("")
        if isinstance(value, (int, float)):
            return str(value)
        try:
            float_val = float(value)
            if str(value).strip() == str(int(float_val)):
                return str(int(float_val))
            return str(float_val)
        except Exception:
            return quote(value)

    version = db_dict.get("version", "")
    nodes = db_dict.get("nodes", []) or []
    messages = db_dict.get("messages", []) or []

    ns_defaults = [
        "NS_DESC_",
        "CM_",
        "BA_DEF_",
        "BA_",
        "VAL_",
        "CAT_DEF_",
        "CAT_",
        "FILTER",
        "BA_DEF_DEF_",
        "EV_DATA_",
        "ENVVAR_DATA_",
        "SGTYPE_",
        "SGTYPE_VAL_",
        "BA_DEF_SGTYPE_",
        "BA_SGTYPE_",
        "SIG_TYPE_REF_",
        "VAL_TABLE_",
        "SIG_GROUP_",
        "SIG_VALTYPE_",
        "SIGTYPE_VALTYPE_",
        "BO_TX_BU_",
        "BA_DEF_REL_",
        "BA_REL_",
        "BA_DEF_DEF_REL_",
        "BU_SG_REL_",
        "BU_EV_REL_",
        "BU_BO_REL_",
        "SG_MUL_VAL_",
    ]

    lines: List[str] = []
    lines.append(f"VERSION {quote(version)}")
    lines.append("")
    lines.append("NS_ :")
    for token in ns_defaults:
        lines.append(f"    {token}")

    lines.append("")
    lines.append("BS_:")
    lines.append("")
    lines.append("BU_: " + " ".join(nodes))

    comment_lines: List[str] = []
    attribute_lines: List[str] = []
    value_table_lines: List[str] = []

    for message in sorted(messages, key=lambda m: m.get("frame_id", 0)):
        frame_id = int(message.get("frame_id", 0))
        name = message.get("name", f"MSG_{frame_id}")
        length = int(message.get("length", 8))
        senders = message.get("senders", []) or []
        transmitter = senders[0] if senders else "Vector__XXX"

        lines.append("")
        lines.append(f"BO_ {frame_id} {name}: {length} {transmitter}")

        cycle_time = message.get("cycle_time")
        if cycle_time is not None:
            attribute_lines.append(
                f"BA_ \"GenMsgCycleTime\" BO_ {frame_id} {format_attr_value(cycle_time)};"
            )

        msg_comment = message.get("comment")
        if msg_comment:
            comment_lines.append(f"CM_ BO_ {frame_id} {quote(msg_comment)};")

        attributes = message.get("attributes", {}) or {}
        for attr_name, attr_val in attributes.items():
            attribute_lines.append(
                f"BA_ {quote(attr_name)} BO_ {frame_id} {format_attr_value(attr_val)};"
            )

        for signal in message.get("signals", []) or []:
            sig_name = signal.get("name", "")
            start_bit = int(signal.get("start_bit", 0))
            length_bits = int(signal.get("length", 1))
            byte_order = signal.get("byte_order", "motorola")
            byte_flag = 0 if byte_order == "motorola" else 1
            sign_flag = "-" if signal.get("is_signed") else "+"
            scale = signal.get("scale", 1)
            offset = signal.get("offset", 0)
            minimum = signal.get("minimum")
            maximum = signal.get("maximum")
            unit = signal.get("unit") or ""
            receivers = signal.get("receivers", []) or ["Vector__XXX"]
            multiplex = signal.get("multiplex")
            multiplexer_ids = signal.get("multiplexer_ids") or []

            multiplex_tag = ""
            if multiplex == "MUX":
                multiplex_tag = " M"
            elif multiplex == "SUB" and multiplexer_ids:
                multiplex_tag = f" m{int(multiplexer_ids[0])}"

            min_val = 0 if minimum is None else minimum
            max_val = 0 if maximum is None else maximum

            signal_line = (
                f" SG_ {sig_name}{multiplex_tag} : {start_bit}|{length_bits}"
                f"@{byte_flag}{sign_flag} ({scale},{offset}) [{min_val}|{max_val}]"
                f" {quote(unit)} {' '.join(receivers)}"
            )
            lines.append(signal_line)

            sig_comment = signal.get("comment")
            if sig_comment:
                comment_lines.append(
                    f"CM_ SG_ {frame_id} {sig_name} {quote(sig_comment)};"
                )

            value_table = signal.get("value_table", {}) or {}
            if value_table:
                entries = " ".join(
                    f"{int(k) if str(k).isdigit() else k} {quote(v)}"
                    for k, v in sorted(value_table.items(), key=lambda item: str(item[0]))
                )
                value_table_lines.append(f"VAL_ {frame_id} {sig_name} {entries} ;")

    lines.append("")
    lines.extend(comment_lines)
    if comment_lines:
        lines.append("")
    lines.extend(attribute_lines)
    if attribute_lines:
        lines.append("")
    lines.extend(value_table_lines)

    return "\n".join(lines).strip() + "\n"

