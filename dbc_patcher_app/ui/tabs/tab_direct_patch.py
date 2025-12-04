"""Tab implementing the 3-file direct patch workflow."""
from __future__ import annotations

import json
from typing import Dict, List, Optional

from PyQt5 import QtCore, QtWidgets

from ...core.dbc_parser import DBCParser
from ...core.diff_engine import DiffEngine
from ...core.patch_applier import PatchApplier, PatchResult
from ...core.ref_db import ReferenceDB
from ...core.history import HistoryLogger
from ..widgets.file_selector import FileSelector


class DirectPatchTab(QtWidgets.QWidget):
    def __init__(
        self,
        parser: DBCParser,
        ref_db: ReferenceDB,
        history: HistoryLogger,
        parent: Optional[QtWidgets.QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self.parser = parser
        self.ref_db = ref_db
        self.history = history
        self.diff_engine = DiffEngine()
        self.applier = PatchApplier(parser=self.parser, ref_db=self.ref_db)
        self.result: Optional[PatchResult] = None
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QtWidgets.QVBoxLayout(self)

        self.raw_old_selector = FileSelector("Raw old DBC:", "DBC Files (*.dbc)")
        self.clean_old_selector = FileSelector("Clean old DBC (v1):", "DBC Files (*.dbc)")
        self.raw_new_selector = FileSelector("Raw new DBC:", "DBC Files (*.dbc)")
        self.output_selector = FileSelector("Output cleaned new DBC:", "DBC Files (*.dbc)", mode="save")

        layout.addWidget(self.raw_old_selector)
        layout.addWidget(self.clean_old_selector)
        layout.addWidget(self.raw_new_selector)
        layout.addWidget(self.output_selector)

        options_layout = QtWidgets.QHBoxLayout()
        self.export_patch_chk = QtWidgets.QCheckBox("Export patch file")
        self.validate_chk = QtWidgets.QCheckBox("Validate output with cantools")
        self.patch_path_selector = FileSelector("Patch output (optional):", "JSON Files (*.json)", mode="save")
        self.patch_path_selector.setDisabled(True)

        self.export_patch_chk.stateChanged.connect(self._toggle_patch_export)

        options_layout.addWidget(self.export_patch_chk)
        options_layout.addWidget(self.validate_chk)
        layout.addLayout(options_layout)
        layout.addWidget(self.patch_path_selector)

        self.run_btn = QtWidgets.QPushButton("Run Direct Patch")
        self.run_btn.clicked.connect(self._on_run)
        layout.addWidget(self.run_btn)

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

        layout.addStretch()

    def _toggle_patch_export(self, state: int) -> None:
        enabled = state == QtCore.Qt.Checked
        self.patch_path_selector.setEnabled(enabled)

    def _on_run(self) -> None:
        raw_old = self.raw_old_selector.path()
        clean_old = self.clean_old_selector.path()
        raw_new = self.raw_new_selector.path()
        output_path = self.output_selector.path()

        selectors = [self.raw_old_selector, self.clean_old_selector, self.raw_new_selector]
        if any(not selector.is_set() for selector in selectors):
            QtWidgets.QMessageBox.warning(self, "Missing files", "Please select all required files.")
            return

        if not self.output_selector.is_set():
            QtWidgets.QMessageBox.warning(self, "Output path", "Select an output path for the cleaned DBC.")
            return

        missing = [p for p in [raw_old, clean_old, raw_new] if not p.exists()]
        if missing:
            QtWidgets.QMessageBox.warning(self, "Missing files", "One or more input files do not exist.")
            return

        raw_old_model = self.parser.load_dbc(raw_old)
        clean_old_model = self.parser.load_dbc(clean_old)
        raw_new_model = self.parser.load_dbc(raw_new)

        patch_obj = self.diff_engine.build_patch(raw_old_model, clean_old_model)
        self.result = self.applier.apply_patch(raw_new_model, patch_obj)

        validate = self.validate_chk.isChecked()
        self.parser.save_dbc(self.result.new_model, output_path, validate=validate)

        if self.export_patch_chk.isChecked():
            if not self.patch_path_selector.is_set():
                QtWidgets.QMessageBox.warning(self, "Patch path", "Select a path to export the patch file.")
            else:
                patch_path = self.patch_path_selector.path()
                patch_path.parent.mkdir(parents=True, exist_ok=True)
                patch_path.write_text(json.dumps(patch_obj, indent=2), encoding="utf-8")

        self._populate_results()
        self.history.log(
            "direct_patch",
            {
                "raw_old": str(raw_old),
                "clean_old": str(clean_old),
                "raw_new": str(raw_new),
                "output": str(output_path),
                "conflicts": len(self.result.conflicts) if self.result else 0,
            },
        )

    def _populate_results(self) -> None:
        if not self.result:
            return
        self._fill_table(self.applied_table, self.result.applied, ["rule"])
        self._fill_table(self.skipped_table, self.result.skipped, ["reason"])
        self._fill_table(self.conflicts_table, self.result.conflicts, ["reason"])

    def _fill_table(
        self, table: QtWidgets.QTableWidget, data: List[Dict[str, object]], extra_fields: List[str]
    ) -> None:
        table.setRowCount(len(data))
        for row, item in enumerate(data):
            rule = item.get("rule", {})
            table.setItem(row, 0, QtWidgets.QTableWidgetItem(str(rule.get("op"))))
            details = ", ".join(f"{k}:{v}" for k, v in item.items() if k in extra_fields)
            table.setItem(row, 1, QtWidgets.QTableWidgetItem(details))
        table.resizeColumnsToContents()
