"""Main window for DBC Patcher."""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from PyQt5 import QtWidgets

from ..core.dbc_parser import DBCParser
from ..core.ref_db import ReferenceDB
from ..core.history import HistoryLogger
from .tabs.tab_generate_patch import GeneratePatchTab
from .tabs.tab_apply_patch import ApplyPatchTab
from .tabs.tab_direct_patch import DirectPatchTab
from .tabs.tab_reference import ReferenceTab
from .tabs.tab_history import HistoryTab
from .tabs.tab_settings import SettingsTab


class MainWindow(QtWidgets.QMainWindow):
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
        self.setWindowTitle("DBC Patcher")
        self.resize(1100, 700)
        self._init_ui()

    def _init_ui(self) -> None:
        tabs = QtWidgets.QTabWidget()
        tabs.addTab(
            GeneratePatchTab(parser=self.parser, ref_db=self.ref_db, history=self.history),
            "Generate Patch",
        )
        tabs.addTab(
            ApplyPatchTab(parser=self.parser, ref_db=self.ref_db, history=self.history),
            "Apply Patch",
        )
        tabs.addTab(
            DirectPatchTab(
                parser=self.parser, ref_db=self.ref_db, history=self.history
            ),
            "3-File Patch (Direct Apply)",
        )
        tabs.addTab(ReferenceTab(ref_db=self.ref_db), "Reference")
        tabs.addTab(HistoryTab(history=self.history), "History")
        tabs.addTab(
            SettingsTab(ref_db=self.ref_db, history=self.history),
            "Settings",
        )
        self.setCentralWidget(tabs)

