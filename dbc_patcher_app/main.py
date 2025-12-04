"""Application entry point for DBC Patcher."""
from __future__ import annotations

import sys
from pathlib import Path

from PyQt5 import QtWidgets

from .core.utils import ensure_data_file
from .core.ref_db import ReferenceDB
from .core.history import HistoryLogger
from .core.dbc_parser import DBCParser
from .ui.main_window import MainWindow


DATA_DIR = Path(__file__).resolve().parent / "data"
REF_DB_PATH = DATA_DIR / "ref_db.json"
HISTORY_PATH = DATA_DIR / "history.json"


def bootstrap_files() -> None:
    ensure_data_file(REF_DB_PATH, {"signals": {}, "messages": {}})
    ensure_data_file(HISTORY_PATH, [])


def main() -> None:
    bootstrap_files()
    app = QtWidgets.QApplication(sys.argv)
    app.setApplicationName("DBC Patcher")

    parser = DBCParser()
    ref_db = ReferenceDB.load_ref(REF_DB_PATH)
    history = HistoryLogger(HISTORY_PATH)

    window = MainWindow(parser=parser, ref_db=ref_db, history=history)
    window.show()

    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

