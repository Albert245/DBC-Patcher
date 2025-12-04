"""History viewer tab."""
from __future__ import annotations

import csv
from pathlib import Path

from PyQt5 import QtWidgets

from ...core.history import HistoryLogger


class HistoryTab(QtWidgets.QWidget):
    def __init__(self, history: HistoryLogger, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.history = history
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QtWidgets.QVBoxLayout(self)
        self.table = QtWidgets.QTableWidget(0, 5)
        self.table.setHorizontalHeaderLabels(
            ["Timestamp", "Action", "Input", "Conflicts", "Output"]
        )
        self.export_btn = QtWidgets.QPushButton("Export CSV")
        self.export_btn.clicked.connect(self._export_csv)
        layout.addWidget(self.table)
        layout.addWidget(self.export_btn)
        layout.addStretch()
        self._refresh()

    def _refresh(self) -> None:
        entries = self.history.entries()
        self.table.setRowCount(len(entries))
        for row, entry in enumerate(entries):
            details = entry.get("details", {})
            self.table.setItem(row, 0, QtWidgets.QTableWidgetItem(entry.get("timestamp", "")))
            self.table.setItem(row, 1, QtWidgets.QTableWidgetItem(entry.get("action", "")))
            input_files = ", ".join(str(v) for k, v in details.items() if "file" in k or k in {"raw", "clean"})
            self.table.setItem(row, 2, QtWidgets.QTableWidgetItem(input_files))
            conflicts = str(details.get("conflicts", details.get("conflicts_count", "")))
            self.table.setItem(row, 3, QtWidgets.QTableWidgetItem(conflicts))
            output = str(details.get("output", ""))
            self.table.setItem(row, 4, QtWidgets.QTableWidgetItem(output))
        self.table.resizeColumnsToContents()

    def _export_csv(self) -> None:
        path, _ = QtWidgets.QFileDialog.getSaveFileName(self, "Export history", "history.csv", "CSV (*.csv)")
        if not path:
            return
        entries = self.history.entries()
        with Path(path).open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "action", "details"])
            for entry in entries:
                writer.writerow([entry.get("timestamp"), entry.get("action"), entry.get("details")])
        QtWidgets.QMessageBox.information(self, "Exported", f"History saved to {path}")

