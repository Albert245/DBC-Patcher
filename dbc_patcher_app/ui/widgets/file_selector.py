"""Reusable file selector widget with validation."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from PyQt5 import QtWidgets


class FileSelector(QtWidgets.QWidget):
    def __init__(
        self,
        label: str,
        filter: str = "All Files (*.*)",
        mode: str = "open",
        parent: Optional[QtWidgets.QWidget] = None,
    ) -> None:
        super().__init__(parent)
        self.filter = filter
        self.mode = mode
        self._init_ui(label)

    def _init_ui(self, label: str) -> None:
        layout = QtWidgets.QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.label = QtWidgets.QLabel(label)
        self.path_edit = QtWidgets.QLineEdit()
        self.browse_btn = QtWidgets.QPushButton("Browse")
        self.browse_btn.clicked.connect(self._browse)

        layout.addWidget(self.label)
        layout.addWidget(self.path_edit)
        layout.addWidget(self.browse_btn)

    def _browse(self) -> None:
        dialog = QtWidgets.QFileDialog(self)
        dialog.setNameFilter(self.filter)
        if self.mode == "save":
            path, _ = dialog.getSaveFileName(self, "Save file", "", self.filter)
        else:
            path, _ = dialog.getOpenFileName(self, "Select file", "", self.filter)
        if path:
            self.path_edit.setText(path)

    def path(self) -> Path:
        return Path(self.path_edit.text())

    def set_path(self, path: Path) -> None:
        self.path_edit.setText(str(path))

    def exists(self) -> bool:
        return self.path().exists()

    def is_set(self) -> bool:
        return bool(self.path_edit.text().strip())

