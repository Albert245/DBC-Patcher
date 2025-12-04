"""Reference database for canonical signals and messages."""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

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
            self.messages[msg.name] = msg
            for sig in msg.signals:
                self.signals[sig.name] = sig
        self.save_ref()

    def suggest_for_message(self, message: DBCMessage) -> Optional[DBCMessage]:
        return self.messages.get(message.name)

    def suggest_for_signal(self, signal_name: str) -> Optional[DBCSignal]:
        return self.signals.get(signal_name)

