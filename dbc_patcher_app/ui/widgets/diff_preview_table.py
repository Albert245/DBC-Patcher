"""Table widget to display diff previews."""
from __future__ import annotations

from typing import List, Dict

from PyQt5 import QtWidgets, QtGui


class DiffPreviewTable(QtWidgets.QTableWidget):
    HEADERS = ["Field", "Old", "New", "Type"]

    def __init__(self, parent=None):
        super().__init__(0, len(self.HEADERS), parent)
        self.setHorizontalHeaderLabels(self.HEADERS)
        self.horizontalHeader().setStretchLastSection(True)

    def load_diffs(self, rows: List[Dict[str, str]]) -> None:
        self.setRowCount(len(rows))
        for row_idx, row in enumerate(rows):
            for col_idx, key in enumerate(["field", "old", "new", "type"]):
                item = QtWidgets.QTableWidgetItem(str(row.get(key, "")))
                if row.get("status") == "added":
                    item.setBackground(QtGui.QColor("#d4edda"))
                elif row.get("status") == "removed":
                    item.setBackground(QtGui.QColor("#f8d7da"))
                elif row.get("status") == "modified":
                    item.setBackground(QtGui.QColor("#fff3cd"))
                self.setItem(row_idx, col_idx, item)
        self.resizeColumnsToContents()

