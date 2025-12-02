"""History logging utilities."""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List


@dataclass
class HistoryEntry:
    timestamp: str
    action: str
    details: Dict[str, object]


class HistoryLogger:
    def __init__(self, path: Path) -> None:
        self.path = path
        if not self.path.exists():
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.path.write_text("[]", encoding="utf-8")

    def log(self, action: str, details: Dict[str, object]) -> None:
        entry = HistoryEntry(
            timestamp=datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            action=action,
            details=details,
        )
        data = self._load()
        data.append(entry.__dict__)
        self.path.write_text(json.dumps(data, indent=2), encoding="utf-8")

    def _load(self) -> List[Dict[str, object]]:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def entries(self) -> List[Dict[str, object]]:
        return self._load()

