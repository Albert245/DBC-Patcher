"""Shared helpers for the DBC Patcher app."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Tuple


def ensure_data_file(path: Path, default_content: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        if isinstance(default_content, (dict, list)):
            path.write_text(json.dumps(default_content, indent=2), encoding="utf-8")
        else:
            path.write_text(str(default_content), encoding="utf-8")


def human_message_id(msg_id: int) -> str:
    return f"0x{msg_id:X}"


def diff_summary(rule: Dict[str, object]) -> Tuple[str, str]:
    op = rule.get("op", "?")
    target = rule.get("signal_name") or rule.get("message_id")
    return str(op), str(target)

