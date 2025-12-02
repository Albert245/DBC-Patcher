"""Tab for generating DBC patches."""
from __future__ import annotations

import json
from pathlib import Path
from typing import List

from PyQt5 import QtWidgets

from ...core.diff_engine import DiffEngine
from ...core.dbc_parser import DBCParser
from ...core.ref_db import ReferenceDB
from ...core.history import HistoryLogger
from ...core.utils import diff_summary
from ..widgets.file_selector import FileSelector
from ..widgets.diff_preview_table import DiffPreviewTable


class GeneratePatchTab(QtWidgets.QWidget):
    def __init__(
        self,
        parser: DBCParser,
        ref_db: ReferenceDB,
        history: HistoryLogger,
        parent: QtWidgets.QWidget | None = None,
    ) -> None:
        super().__init__(parent)
        self.parser = parser
        self.ref_db = ref_db
        self.history = history
        self.diff_engine = DiffEngine()
        self.patch_data: dict | None = None
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QtWidgets.QVBoxLayout(self)
        self.raw_selector = FileSelector("Raw DBC:", "DBC Files (*.dbc)")
        self.clean_selector = FileSelector("Clean DBC:", "DBC Files (*.dbc)")
        layout.addWidget(self.raw_selector)
        layout.addWidget(self.clean_selector)

        self.generate_btn = QtWidgets.QPushButton("Generate Patch")
        self.generate_btn.clicked.connect(self._on_generate)
        layout.addWidget(self.generate_btn)

        self.diff_table = DiffPreviewTable()
        layout.addWidget(self.diff_table)

        self.save_btn = QtWidgets.QPushButton("Save Patch")
        self.save_btn.clicked.connect(self._save_patch)
        layout.addWidget(self.save_btn)
        layout.addStretch()

    def _on_generate(self) -> None:
        raw_path = self.raw_selector.path()
        clean_path = self.clean_selector.path()
        if not raw_path.exists() or not clean_path.exists():
            QtWidgets.QMessageBox.warning(self, "Missing files", "Please select both DBC files.")
            return
        raw_model = self.parser.load_dbc(raw_path)
        clean_model = self.parser.load_dbc(clean_path)
        self.patch_data = self.diff_engine.generate_patch(raw_model, clean_model)
        rows = [
            {
                "field": summary[0],
                "old": "",
                "new": summary[1],
                "type": rule.get("op"),
                "status": "modified",
            }
            for rule in self.patch_data["rules"]
            for summary in [diff_summary(rule)]
        ]
        self.diff_table.load_diffs(rows)
        self.history.log(
            "generate_patch",
            {"raw": str(raw_path), "clean": str(clean_path), "rules": len(self.patch_data["rules"])}
        )

    def _save_patch(self) -> None:
        if not self.patch_data:
            QtWidgets.QMessageBox.information(self, "No patch", "Generate a patch first.")
            return
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self, "Save Patch", "patch.json", "JSON (*.json)"
        )
        if not path:
            return
        Path(path).write_text(json.dumps(self.patch_data, indent=2), encoding="utf-8")
        QtWidgets.QMessageBox.information(self, "Saved", f"Patch saved to {path}")

