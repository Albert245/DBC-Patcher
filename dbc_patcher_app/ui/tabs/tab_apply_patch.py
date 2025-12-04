"""Tab for applying patches to DBC files."""
from __future__ import annotations

import json
from pathlib import Path

from PyQt5 import QtWidgets

from ...core.dbc_parser import DBCParser
from ...core.patch_applier import PatchApplier
from ...core.ref_db import ReferenceDB
from ...core.history import HistoryLogger
from ..widgets.file_selector import FileSelector


class ApplyPatchTab(QtWidgets.QWidget):
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
        self.applier = PatchApplier(parser=self.parser, ref_db=self.ref_db)
        self.result = None
        self.model = None
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QtWidgets.QVBoxLayout(self)
        self.raw_selector = FileSelector("Raw New DBC:", "DBC Files (*.dbc)")
        self.patch_selector = FileSelector("Patch JSON:", "JSON Files (*.json)")
        layout.addWidget(self.raw_selector)
        layout.addWidget(self.patch_selector)

        self.apply_btn = QtWidgets.QPushButton("Apply Patch")
        self.apply_btn.clicked.connect(self._on_apply)
        layout.addWidget(self.apply_btn)

        self.applied_table = QtWidgets.QTableWidget(0, 2)
        self.applied_table.setHorizontalHeaderLabels(["Operation", "Target"])
        self.skipped_table = QtWidgets.QTableWidget(0, 2)
        self.skipped_table.setHorizontalHeaderLabels(["Operation", "Reason"])
        self.conflicts_table = QtWidgets.QTableWidget(0, 2)
        self.conflicts_table.setHorizontalHeaderLabels(["Conflict", "Detail"])

        layout.addWidget(QtWidgets.QLabel("Applied"))
        layout.addWidget(self.applied_table)
        layout.addWidget(QtWidgets.QLabel("Skipped"))
        layout.addWidget(self.skipped_table)
        layout.addWidget(QtWidgets.QLabel("Conflicts"))
        layout.addWidget(self.conflicts_table)

        self.save_btn = QtWidgets.QPushButton("Save Cleaned DBC")
        self.save_btn.clicked.connect(self._save_cleaned)
        layout.addWidget(self.save_btn)
        layout.addStretch()

    def _on_apply(self) -> None:
        raw_path = self.raw_selector.path()
        patch_path = self.patch_selector.path()
        if not raw_path.exists() or not patch_path.exists():
            QtWidgets.QMessageBox.warning(self, "Missing files", "Select both files")
            return
        self.model = self.parser.load_dbc(raw_path)
        patch_data = json.loads(patch_path.read_text(encoding="utf-8"))
        self.result = self.applier.apply_patch(self.model, patch_data)
        self._populate_results()
        self.history.log(
            "apply_patch",
            {
                "raw_file": str(raw_path),
                "patch_file": str(patch_path),
                "conflicts": len(self.result.conflicts),
            },
        )

    def _populate_results(self) -> None:
        if not self.result:
            return
        self._fill_table(self.applied_table, self.result.applied, ["rule"])
        self._fill_table(self.skipped_table, self.result.skipped, ["reason"])
        self._fill_table(self.conflicts_table, self.result.conflicts, ["reason"])

    def _fill_table(
        self, table: QtWidgets.QTableWidget, data: list[dict], extra_fields: list[str]
    ) -> None:
        table.setRowCount(len(data))
        for row, item in enumerate(data):
            rule = item.get("rule", {})
            table.setItem(row, 0, QtWidgets.QTableWidgetItem(str(rule.get("op"))))
            details = ", ".join(f"{k}:{v}" for k, v in item.items() if k in extra_fields)
            table.setItem(row, 1, QtWidgets.QTableWidgetItem(details))
        table.resizeColumnsToContents()

    def _save_cleaned(self) -> None:
        if not self.model:
            QtWidgets.QMessageBox.information(self, "Nothing to save", "Apply a patch first.")
            return
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self, "Save cleaned DBC", "cleaned.dbc", "DBC (*.dbc)"
        )
        if not path:
            return
        target_model = self.result.new_model if self.result else self.model
        self.parser.save_dbc(target_model, Path(path))
        QtWidgets.QMessageBox.information(self, "Saved", f"Cleaned DBC saved to {path}")

