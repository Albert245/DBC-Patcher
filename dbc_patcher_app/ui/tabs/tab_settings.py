"""Settings tab."""
from __future__ import annotations

from pathlib import Path

from PyQt5 import QtWidgets

from ...core.ref_db import ReferenceDB
from ...core.history import HistoryLogger


class SettingsTab(QtWidgets.QWidget):
    def __init__(self, ref_db: ReferenceDB, history: HistoryLogger, parent=None) -> None:
        super().__init__(parent)
        self.ref_db = ref_db
        self.history = history
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QtWidgets.QFormLayout(self)

        self.ref_path_edit = QtWidgets.QLineEdit(str(self.ref_db.path))
        self.history_path_edit = QtWidgets.QLineEdit(str(self.history.path))

        self.auto_update_cb = QtWidgets.QCheckBox("Auto-update ref DB on patch generation")
        self.skip_warnings_cb = QtWidgets.QCheckBox("Skip warnings on apply")
        self.overwrite_cb = QtWidgets.QCheckBox("Overwrite OEM changes (danger mode)")

        self.save_btn = QtWidgets.QPushButton("Save")
        self.save_btn.clicked.connect(self._save)

        layout.addRow("Reference DB Path", self.ref_path_edit)
        layout.addRow("History DB Path", self.history_path_edit)
        layout.addRow(self.auto_update_cb)
        layout.addRow(self.skip_warnings_cb)
        layout.addRow(self.overwrite_cb)
        layout.addRow(self.save_btn)

    def _save(self) -> None:
        QtWidgets.QMessageBox.information(self, "Settings", "Settings saved locally.")

