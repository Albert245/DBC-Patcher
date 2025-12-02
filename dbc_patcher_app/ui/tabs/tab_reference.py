"""Reference database viewer tab."""
from __future__ import annotations

from pathlib import Path

from PyQt5 import QtWidgets

from ...core.ref_db import ReferenceDB
from ...core.dbc_parser import DBCParser
from ..widgets.file_selector import FileSelector


class ReferenceTab(QtWidgets.QWidget):
    def __init__(self, ref_db: ReferenceDB, parent: QtWidgets.QWidget | None = None) -> None:
        super().__init__(parent)
        self.ref_db = ref_db
        self.parser = DBCParser()
        self._init_ui()

    def _init_ui(self) -> None:
        layout = QtWidgets.QVBoxLayout(self)
        self.signal_table = QtWidgets.QTableWidget(0, 3)
        self.signal_table.setHorizontalHeaderLabels(["Signal", "Start", "Length"])
        self.message_table = QtWidgets.QTableWidget(0, 2)
        self.message_table.setHorizontalHeaderLabels(["Message", "ID"])

        self.import_selector = FileSelector("Import DBC:", "DBC Files (*.dbc)")
        self.import_btn = QtWidgets.QPushButton("Import to reference")
        self.import_btn.clicked.connect(self._import_dbc)

        self.export_btn = QtWidgets.QPushButton("Export reference JSON")
        self.export_btn.clicked.connect(self._export_ref)

        self.search_box = QtWidgets.QLineEdit()
        self.search_box.setPlaceholderText("Search signal or message")
        self.search_box.textChanged.connect(self._refresh_tables)

        layout.addWidget(self.import_selector)
        layout.addWidget(self.import_btn)
        layout.addWidget(self.export_btn)
        layout.addWidget(self.search_box)
        layout.addWidget(QtWidgets.QLabel("Signals"))
        layout.addWidget(self.signal_table)
        layout.addWidget(QtWidgets.QLabel("Messages"))
        layout.addWidget(self.message_table)
        layout.addStretch()

        self._refresh_tables()

    def _import_dbc(self) -> None:
        path = self.import_selector.path()
        if not path.exists():
            QtWidgets.QMessageBox.warning(self, "File missing", "Select a DBC file")
            return
        model = self.parser.load_dbc(path)
        self.ref_db.update_from_dbc(model)
        self._refresh_tables()
        QtWidgets.QMessageBox.information(self, "Imported", "Reference updated")

    def _export_ref(self) -> None:
        path, _ = QtWidgets.QFileDialog.getSaveFileName(
            self, "Export", "ref_db.json", "JSON (*.json)"
        )
        if not path:
            return
        self.ref_db.save_ref()
        dest = Path(path)
        dest.write_text(Path(self.ref_db.path).read_text(encoding="utf-8"), encoding="utf-8")
        QtWidgets.QMessageBox.information(self, "Exported", f"Reference saved to {path}")

    def _refresh_tables(self) -> None:
        term = self.search_box.text().lower()
        signals = [s for s in self.ref_db.signals.values() if term in s.name.lower()]
        self.signal_table.setRowCount(len(signals))
        for row, sig in enumerate(signals):
            self.signal_table.setItem(row, 0, QtWidgets.QTableWidgetItem(sig.name))
            self.signal_table.setItem(row, 1, QtWidgets.QTableWidgetItem(str(sig.start_bit)))
            self.signal_table.setItem(row, 2, QtWidgets.QTableWidgetItem(str(sig.length)))
        messages = [m for m in self.ref_db.messages.values() if term in m.name.lower()]
        self.message_table.setRowCount(len(messages))
        for row, msg in enumerate(messages):
            self.message_table.setItem(row, 0, QtWidgets.QTableWidgetItem(msg.name))
            self.message_table.setItem(row, 1, QtWidgets.QTableWidgetItem(msg.hex_id))
        self.signal_table.resizeColumnsToContents()
        self.message_table.resizeColumnsToContents()

