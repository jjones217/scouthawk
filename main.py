import sys
from pathlib import Path

import pandas as pd
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QTabWidget, QTableView, QLabel, QPushButton, QFileDialog,
    QLineEdit, QSpinBox, QDoubleSpinBox, QStatusBar, QHeaderView,
    QComboBox, QMessageBox, QSizePolicy, QFrame, QTextEdit,
)
from PySide6.QtCore import (
    Qt, QAbstractTableModel, QModelIndex, QSortFilterProxyModel,
    QThread, Signal,
)
from PySide6.QtGui import QFont, QColor

import config
import db as ootp_db


# ---------------------------------------------------------------------------
# Table model wrapping a pandas DataFrame
# ---------------------------------------------------------------------------

class PandasModel(QAbstractTableModel):
    _FLOAT_COLS = {'AVG', 'OBP', 'SLG', 'OPS', 'wOBA', 'ERA', 'WHIP', 'FIP', 'IP'}
    _ONE_DEC    = {'K/9', 'BB/9', 'HR/9'}

    def __init__(self, df: pd.DataFrame | None = None):
        super().__init__()
        self._df = df if df is not None else pd.DataFrame()

    def rowCount(self, parent=QModelIndex()):
        return len(self._df)

    def columnCount(self, parent=QModelIndex()):
        return len(self._df.columns)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        val = self._df.iloc[index.row(), index.column()]

        if role == Qt.DisplayRole:
            if pd.isna(val):
                return ''
            col = self._df.columns[index.column()]
            if isinstance(val, float):
                if col in self._FLOAT_COLS:
                    return f'{val:.3f}'
                if col in self._ONE_DEC:
                    return f'{val:.1f}'
                return f'{val:.2f}'
            return str(val)

        if role == Qt.UserRole:
            # Raw value for sorting — push NaN to bottom
            if pd.isna(val):
                return -float('inf')
            return val

        if role == Qt.TextAlignmentRole:
            if isinstance(val, (int, float)) and not pd.isna(val):
                return int(Qt.AlignRight | Qt.AlignVCenter)
            return int(Qt.AlignLeft | Qt.AlignVCenter)

        if role == Qt.BackgroundRole:
            # Subtle zebra striping
            if index.row() % 2 == 1:
                return QColor(245, 245, 250)

        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            return str(self._df.columns[section])
        return str(section + 1)

    def update(self, df: pd.DataFrame):
        self.beginResetModel()
        self._df = df.copy().reset_index(drop=True)
        self.endResetModel()


# ---------------------------------------------------------------------------
# Proxy model that sorts by UserRole (raw values)
# ---------------------------------------------------------------------------

class NumericSortProxy(QSortFilterProxyModel):
    def __init__(self):
        super().__init__()
        self.setSortRole(Qt.UserRole)
        self.setFilterCaseSensitivity(Qt.CaseInsensitive)
        self.setFilterKeyColumn(-1)  # search all columns


# ---------------------------------------------------------------------------
# Reusable tab: filter bar + sortable table
# ---------------------------------------------------------------------------

class StatsTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._model = PandasModel()
        self._proxy = NumericSortProxy()
        self._proxy.setSourceModel(self._model)

        layout = QVBoxLayout(self)
        layout.setSpacing(4)

        # Filter bar
        filter_bar = QHBoxLayout()
        self._search = QLineEdit()
        self._search.setPlaceholderText('Search...')
        self._search.textChanged.connect(self._proxy.setFilterFixedString)
        self._row_count = QLabel('0 rows')
        self._row_count.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        filter_bar.addWidget(QLabel('Filter:'))
        filter_bar.addWidget(self._search)
        filter_bar.addWidget(self._row_count)
        layout.addLayout(filter_bar)

        # Table
        self._table = QTableView()
        self._table.setModel(self._proxy)
        self._table.setSortingEnabled(True)
        self._table.setSelectionBehavior(QTableView.SelectRows)
        self._table.setEditTriggers(QTableView.NoEditTriggers)
        self._table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeToContents)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.verticalHeader().setVisible(False)
        self._table.setAlternatingRowColors(False)  # handled by model
        font = QFont('Consolas', 9)
        self._table.setFont(font)
        layout.addWidget(self._table)

        self._proxy.rowsInserted.connect(self._update_count)
        self._proxy.rowsRemoved.connect(self._update_count)
        self._proxy.modelReset.connect(self._update_count)

    def load(self, df: pd.DataFrame):
        self._model.update(df)
        self._update_count()

    def _update_count(self):
        self._row_count.setText(f'{self._proxy.rowCount()} rows')


# ---------------------------------------------------------------------------
# Schema explorer tab
# ---------------------------------------------------------------------------

class SchemaTab(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        self._text = QTextEdit()
        self._text.setReadOnly(True)
        self._text.setFont(QFont('Consolas', 9))
        layout.addWidget(QLabel('Database tables and columns — useful for troubleshooting'))
        layout.addWidget(self._text)

    def load(self, schema: dict[str, list[str]]):
        lines = []
        for table, cols in sorted(schema.items()):
            lines.append(f'[{table}]')
            lines.append('  ' + ',  '.join(cols))
            lines.append('')
        self._text.setPlainText('\n'.join(lines))


# ---------------------------------------------------------------------------
# Background loader thread
# ---------------------------------------------------------------------------

class LoadWorker(QThread):
    done = Signal(dict)
    error = Signal(str)

    def __init__(self, path: str, year: int, min_pa: int, min_ip: float):
        super().__init__()
        self.path = path
        self.year = year
        self.min_pa = min_pa
        self.min_ip = min_ip

    def run(self):
        try:
            db = ootp_db.OOTPDatabase(self.path)
            batters  = db.get_batters(self.year, self.min_pa)
            pitchers = db.get_pitchers(self.year, self.min_ip)
            schema   = db.schema_summary()
            years    = db.get_years()
            cur_year = db.get_current_year()
            db.close()
            self.done.emit({
                'batters': batters,
                'pitchers': pitchers,
                'schema': schema,
                'years': years,
                'current_year': cur_year,
            })
        except Exception as e:
            self.error.emit(str(e))


# ---------------------------------------------------------------------------
# Main window
# ---------------------------------------------------------------------------

class MainWindow(QMainWindow):
    DEFAULT_SAVE_DIR = r'C:\Users\Justi\Documents\Out of the Park Developments\OOTP Baseball 26\saved_games'

    def __init__(self):
        super().__init__()
        self.setWindowTitle('OOTP 26 Analyzer')
        self.resize(1200, 700)

        self._cfg = config.load()
        self._db_path = self._cfg.get('league_file', '')
        self._worker: LoadWorker | None = None

        self._build_ui()
        self._apply_style()

        if self._db_path and Path(self._db_path).exists():
            self._path_edit.setText(self._db_path)
            self._reload()

    # ------------------------------------------------------------------
    # UI construction
    # ------------------------------------------------------------------

    def _build_ui(self):
        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)
        root.setSpacing(6)
        root.setContentsMargins(8, 8, 8, 8)

        # --- File bar ---
        file_bar = QHBoxLayout()
        file_bar.addWidget(QLabel('League file:'))

        self._path_edit = QLineEdit(self._db_path)
        self._path_edit.setPlaceholderText('Browse to your .lg save file...')
        self._path_edit.setReadOnly(True)
        file_bar.addWidget(self._path_edit, stretch=1)

        browse_btn = QPushButton('Browse...')
        browse_btn.clicked.connect(self._browse)
        file_bar.addWidget(browse_btn)

        self._reload_btn = QPushButton('Reload')
        self._reload_btn.clicked.connect(self._reload)
        file_bar.addWidget(self._reload_btn)

        root.addLayout(file_bar)

        # Help text
        help_label = QLabel(
            'Select the .lg folder:  Documents \u2192 Out of the Park Developments \u2192 '
            'OOTP Baseball 26 \u2192 saved_games \u2192 [Your League Name] \u2192 [League Name].lg  '
            '(select the folder, don\u2019t open it)'
        )
        help_label.setStyleSheet('color: #555; font-size: 11px; padding-left: 4px;')
        root.addWidget(help_label)

        # --- Options bar ---
        opts_bar = QHBoxLayout()

        opts_bar.addWidget(QLabel('Year:'))
        self._year_combo = QComboBox()
        self._year_combo.setMinimumWidth(80)
        self._year_combo.currentIndexChanged.connect(lambda _: self._reload())
        opts_bar.addWidget(self._year_combo)

        opts_bar.addSpacing(12)
        opts_bar.addWidget(QLabel('Min PA:'))
        self._min_pa = QSpinBox()
        self._min_pa.setRange(0, 700)
        self._min_pa.setValue(self._cfg.get('min_pa', 50))
        opts_bar.addWidget(self._min_pa)

        opts_bar.addSpacing(12)
        opts_bar.addWidget(QLabel('Min IP:'))
        self._min_ip = QDoubleSpinBox()
        self._min_ip.setRange(0, 300)
        self._min_ip.setValue(self._cfg.get('min_ip', 20))
        opts_bar.addWidget(self._min_ip)

        opts_bar.addStretch()
        root.addLayout(opts_bar)

        # Separator
        sep = QFrame()
        sep.setFrameShape(QFrame.HLine)
        sep.setFrameShadow(QFrame.Sunken)
        root.addWidget(sep)

        # --- Tabs ---
        self._tabs = QTabWidget()

        self._batters_tab  = StatsTab()
        self._pitchers_tab = StatsTab()
        self._schema_tab   = SchemaTab()

        self._tabs.addTab(self._batters_tab,  'Batters')
        self._tabs.addTab(self._pitchers_tab, 'Pitchers')
        self._tabs.addTab(self._schema_tab,   'Schema Explorer')
        root.addWidget(self._tabs, stretch=1)

        # Status bar
        self._status = QStatusBar()
        self.setStatusBar(self._status)
        self._status.showMessage('No league loaded. Use Browse to select a .lg file.')

    def _apply_style(self):
        self.setStyleSheet("""
            * { color: #1a1a1a; }
            QMainWindow, QWidget { background: #f4f4f4; }
            QTabWidget::pane { border: 1px solid #bbb; background: white; }
            QTabBar::tab {
                background: #dde3ec;
                color: #1a1a1a;
                padding: 6px 16px;
                border: 1px solid #bbb;
                border-bottom: none;
                margin-right: 2px;
            }
            QTabBar::tab:selected {
                background: white;
                color: #1a1a1a;
                border-bottom: 2px solid #2060c0;
            }
            QTabBar::tab:hover { background: #ccd4e8; }
            QPushButton {
                background: #2060c0;
                color: white;
                padding: 5px 14px;
                border: none;
                border-radius: 3px;
            }
            QPushButton:hover { background: #1a50a8; }
            QPushButton:disabled { background: #aaa; color: #eee; }
            QLineEdit, QSpinBox, QDoubleSpinBox, QComboBox {
                background: white;
                color: #1a1a1a;
                border: 1px solid #bbb;
                padding: 3px 6px;
                border-radius: 2px;
            }
            QLabel { color: #1a1a1a; background: transparent; }
            QHeaderView::section {
                background: #dde3ec;
                color: #1a1a1a;
                padding: 5px 6px;
                border: none;
                border-right: 1px solid #bbb;
                border-bottom: 1px solid #bbb;
                font-weight: bold;
            }
            QTableView {
                background: white;
                color: #1a1a1a;
                gridline-color: #e0e0e0;
                selection-background-color: #b8d0f0;
                selection-color: #1a1a1a;
            }
            QStatusBar { background: #e0e4ec; color: #1a1a1a; }
            QTextEdit { background: white; color: #1a1a1a; }
        """)

    # ------------------------------------------------------------------
    # Actions
    # ------------------------------------------------------------------

    def _browse(self):
        start = self._db_path if self._db_path else self.DEFAULT_SAVE_DIR
        # .lg is a folder in OOTP 26 — use directory picker
        path = QFileDialog.getExistingDirectory(
            self,
            'Select your .lg league folder',
            start,
        )
        if path:
            self._db_path = path
            self._path_edit.setText(path)
            self._cfg['league_file'] = path
            config.save(self._cfg)
            # Reset year combo then reload
            self._year_combo.blockSignals(True)
            self._year_combo.clear()
            self._year_combo.blockSignals(False)
            self._reload()

    def _reload(self):
        path = self._db_path
        if not path or not Path(path).exists():
            self._status.showMessage('File not found. Use Browse to select a valid .lg file.')
            return

        year_text = self._year_combo.currentText()
        year = int(year_text) if year_text.isdigit() else None
        min_pa = self._min_pa.value()
        min_ip = self._min_ip.value()

        self._reload_btn.setEnabled(False)
        self._status.showMessage('Loading...')

        self._worker = LoadWorker(path, year, min_pa, min_ip)
        self._worker.done.connect(self._on_load_done)
        self._worker.error.connect(self._on_load_error)
        self._worker.start()

    def _on_load_done(self, data: dict):
        self._reload_btn.setEnabled(True)

        # Populate year combo if empty
        if self._year_combo.count() == 0 and data['years']:
            self._year_combo.blockSignals(True)
            for y in data['years']:
                self._year_combo.addItem(str(y))
            cur = data.get('current_year')
            if cur:
                idx = self._year_combo.findText(str(cur))
                if idx >= 0:
                    self._year_combo.setCurrentIndex(idx)
            self._year_combo.blockSignals(False)

        batters  = data['batters']
        pitchers = data['pitchers']

        self._batters_tab.load(batters)
        self._pitchers_tab.load(pitchers)
        self._schema_tab.load(data['schema'])

        b_count = len(batters)
        p_count = len(pitchers)
        year    = self._year_combo.currentText() or '?'
        league  = Path(self._db_path).stem
        self._status.showMessage(
            f'{league} | Year: {year} | {b_count} batters  {p_count} pitchers'
        )

        if b_count == 0 and p_count == 0:
            QMessageBox.warning(
                self,
                'No data found',
                'No stats were found for the selected year/filters.\n\n'
                'Check the Schema Explorer tab to see what tables are available, '
                'then let me know so we can adjust the queries.',
            )

    def _on_load_error(self, msg: str):
        self._reload_btn.setEnabled(True)
        self._status.showMessage(f'Error: {msg}')
        QMessageBox.critical(self, 'Load error', msg)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    app = QApplication(sys.argv)
    app.setApplicationName('OOTP 26 Analyzer')
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
