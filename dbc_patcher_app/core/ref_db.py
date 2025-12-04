"""Reference database for canonical signals and messages."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional
from copy import deepcopy

from .dbc_parser import DBCModel, DBCMessage, DBCSignal


@dataclass
class ReferenceDB:
    path: Path
    signals: Dict[str, DBCSignal]
    messages: Dict[str, DBCMessage]

    @classmethod
    def load_ref(cls, path: Path) -> "ReferenceDB":
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps({"signals": {}, "messages": {}}), encoding="utf-8")
        content = json.loads(path.read_text(encoding="utf-8"))
        signals = {
            name: DBCSignal(**sig) for name, sig in content.get("signals", {}).items()
        }
        messages = {
            name: DBCMessage(**msg) for name, msg in content.get("messages", {}).items()
        }
        return cls(path=path, signals=signals, messages=messages)

    def save_ref(self) -> None:
        payload = {
            "signals": {k: vars(v) for k, v in self.signals.items()},
            "messages": {k: self._message_to_dict(v) for k, v in self.messages.items()},
        }
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def _message_to_dict(self, msg: DBCMessage) -> Dict[str, object]:
        data = vars(msg).copy()
        data["signals"] = [vars(s) for s in msg.signals]
        return data

    def update_from_dbc(self, model: DBCModel) -> None:
        for msg in model.messages.values():
            key = self._message_key(msg.message_id, msg.name)
            canonical = self._canonicalize_message(msg)
            self.messages[key] = canonical
            self.messages[msg.name] = canonical
            for sig in msg.signals:
                self.signals[sig.name] = deepcopy(sig)
        self.save_ref()

    def suggest_for_message(self, message: DBCMessage) -> Optional[DBCMessage]:
        key = self._message_key(message.message_id, message.name)
        return self.messages.get(key) or self.messages.get(message.name)

    def suggest_for_signal(self, signal_name: str) -> Optional[DBCSignal]:
        return self.signals.get(signal_name)

    def suggest_message(self, frame_id: int, name: str) -> Optional[DBCMessage]:
        return self.messages.get(self._message_key(frame_id, name)) or self.messages.get(name)

    def _canonicalize_message(self, message: DBCMessage) -> DBCMessage:
        cloned_signals = [deepcopy(sig) for sig in message.signals]
        return DBCMessage(
            message_id=message.message_id,
            name=message.name,
            length=message.length,
            cycle_time=message.cycle_time,
            comment=message.comment,
            is_extended_frame=message.is_extended_frame,
            attributes=deepcopy(message.attributes),
            senders=list(message.senders),
            signals=cloned_signals,
        )

    def _message_key(self, frame_id: int, name: str) -> str:
        return f"{frame_id}:{name}"

