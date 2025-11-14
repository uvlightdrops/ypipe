from textual.app import App, ComposeResult
from textual.widgets import DataTable, Button
from textual.containers import Container
from textual import events
from textual.binding import Binding
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt
from textual.widgets import RichLog

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class TableApp(App):
    CSS_PATH = None
    BINDINGS = [
        ("space", "toggle_checkbox", "Toggle Checkbox"),
        ("e", "toggle_expand", "Expand/Collapse Columns"),
        ("enter", "confirm", "Bestätigen")
    ]
    def __init__(self, rows, columns, pk_idx, truncate_cell, collapsible_cols=None, truncate_len=10, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rows = rows
        self.columns = columns
        self.pk_idx = pk_idx
        self.truncate_cell = truncate_cell
        self.collapsible_cols = collapsible_cols
        self.truncate_len = truncate_len
        self.selected = set()
        self.expanded_cols = set()
    def compose(self) -> ComposeResult:
        dt = DataTable()
        dt.add_columns("[ ]", *self.columns)
        for row in self.rows:
            dt.add_row("[ ]", *[self.truncate_cell(cell, col) for cell, col in zip(row, self.columns)])
        yield Container(dt)
        yield RichLog()
        yield Button("Bestätigen", id="confirm")
    def on_mount(self):
        self.query_one(DataTable).focus()
        logger.debug("TableApp mounted.")
    def key_space(self):
        dt = self.query_one(DataTable)
        idx = dt.cursor_row
        if idx in self.selected:
            self.selected.remove(idx)
            dt.update_cell(idx, 0, "[ ]")
        else:
            self.selected.add(idx)
            dt.update_cell(idx, 0, "[x]")
    def action_toggle_expand(self):
        dt = self.query_one(DataTable)
        for col_idx, col in enumerate(self.columns):
            if self.collapsible_cols and col in self.collapsible_cols:
                for row_idx, row in enumerate(self.rows):
                    if col in self.expanded_cols:
                        dt.update_cell(row_idx, col_idx+1, self.truncate_cell(row[col_idx], col))
                    else:
                        dt.update_cell(row_idx, col_idx+1, str(row[col_idx]))
                if col in self.expanded_cols:
                    self.expanded_cols.remove(col)
                else:
                    self.expanded_cols.add(col)
    def action_confirm(self):
        self.exit()
    def on_button_pressed(self, event):
        pass
    def on_key(self, event: events.Key) -> None:
        self.query_one(RichLog).write(event)

