from textual.app import App, ComposeResult
from textual.widgets import DataTable, Button, Static, Input, Label, Footer, Header
from textual.containers import Container, Horizontal, Vertical, Grid
from textual.screen import Screen, ModalScreen
from textual import events, getters
from textual.binding import Binding
from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt
import pandas as pd
from textual.widgets._data_table import RowKey

from flowpy.utils import setup_logger
from .iaBase import iaBase
from .dialogs import QuitScreen, EditCellDialog
from .baseScreen import BaseScreen, BaseTableScreen
logger = setup_logger(__name__, __name__+'.log')


class TableApp(BaseTableScreen, iaBase):
    CSS_PATH = "tableApp.css"
    BINDINGS = [
        ("space", "toggle_checkbox", "Toggle Checkbox"),
        ("e", "toggle_expand", "Expand/Collapse Columns"),
        ("enter", "confirm", "Bestätigen")
    ]
    def __init__(self, df, columns=None, pk_col=None, collapsible_cols=None, truncate_len=10, *args, **kwargs):
        logger.debug("init TableApp with kwargs %s", kwargs)
        for kw in ['col_widths', 'add_data']:
            if kw in kwargs:
                logger.debug("Setting attribute %s from kwargs", kw)
                setattr(self, kw, kwargs.pop(kw))

        self.kwargs = kwargs
        self.df = df
        self.columns = columns if columns is not None else list(df.columns)
        logger.debug("TableApp initialized with columns: %s", self.columns)
        self.pk_col = pk_col # if pk_col is not None else self.columns[0]
        self.collapsible_cols = collapsible_cols
        self.truncate_len = truncate_len
        self.expanded_cols = set()
        self.prompt_text = "Zuordn.OK? (Space/Enter/other)?"
        self.role_index_list = []
        self.selected = set()
        super().__init__(*args, **kwargs)

    def build_main(self):
        return self.compose_main()

    def compose_main(self):
        self.main_table = DataTable(id="main_table")
        self.main_table.add_column("[ ]", key="checkbox")
        self.main_table.add_columns(*self.columns)

        logger.debug("Adding %s columns: %s", len(self.columns), self.columns )
        columns_list = list(self.main_table.columns.values())
        # pop first column (checkbox)
        columns_list.pop(0)
        self.set_col_attrs(self.main_table, col_widths=self.col_widths)
        for idx, row in self.df.iterrows():
            row, values = self.truncate_all(row)
            row_key = self.main_table.add_row("[ ]", *values, key=idx)  # Setze numerischen Key

        self.prompt = Static(self.prompt_text, id="prompt")

        self.role_index_list.append('alf_ArbG_schul_Admin')
        self.role_table = DataTable(id="role_table")
        self.role_table.add_column("role_index", key="role_index")

        confirm = Button("Bestätigen", id="confirm")
        upper = Container(self.main_table, id="upper")
        separator = Static("", id="separator")
        kvlist = [f"{k}: {v}" for k, v in self.add_data.items()]
        o2 = "\n".join(kvlist)
        output = "Gruppenweite Attribute\n" + o2
        logger.debug("add_data output: %s", output)
        meta = Static(output, id="meta")
        self.current_line_dt = DataTable(id="current_line")
        self.current_line_dt.add_columns(*self.columns)
        self.set_col_attrs(self.current_line_dt, col_widths=self.col_widths)

        status = Horizontal(meta, id="status")
        lower = Vertical(self.current_line_dt, status)

        lowerright = Vertical(self.prompt, confirm, meta, id="lower_right")
        lowerleft = Horizontal(self.role_table, lowerright, id="lower")

        layout = Vertical(upper, separator, lower)

        return layout


    def on_mount(self):
        self.refresh_all()
        self.main_table.focus()

    def refresh_all(self):
        self.role_table.clear()

        for idx in sorted(self.role_index_list, key=lambda x: str(x).lower()):
        #for idx in self.role_index_list:
            self.role_table.add_row(str(idx))
            logger.debug("Adding role_index %s to role_table", idx)

        self.role_table.refresh()
        self.main_table.refresh()

    """
    def toggle_checkbox(self):
        dt = self.main_table

        idx = dt.cursor_row
        row_key = RowKey(idx)

        checkbox_col_key = "checkbox"
        # best effort: erzeuge role_idx, kann bei Fehler None bleiben
        role_idx = None
        try:
            if self.pk_col is not None:
                role_idx = self.df.iloc[idx][self.pk_col]
        except Exception:
            role_idx = None
        # logger.debug('role_idx: %s', role_idx)

        try:
            dt.get_cell(row_key, checkbox_col_key)
        except Exception as e:
            logger.error("Error getting cell at row_key %s, checkbox_col_key %s:", row_key, checkbox_col_key)

        try:
            if idx in self.selected:
                logger.debug("idx:%s in self.sel %s -> Deselect", idx, self.selected)
                dt.update_cell(row_key, checkbox_col_key, "[ ]")
                self.selected.remove(idx)
                if role_idx is not None and role_idx in self.role_index_list:
                    self.role_index_list.remove(role_idx)
            else:
                logger.debug("idx:%s Not in self.sel %s -> SELECT", idx, self.selected)
                dt.update_cell(row_key, checkbox_col_key, " x ")
                self.selected.add(idx)
                if role_idx is not None and role_idx not in self.role_index_list:
                    self.role_index_list.append(role_idx)
                    # XXX or have a dict with role_index as keys, we can
                    # the row values to use for insert?
            logger.debug("Current selected role_index_list: %s", self.role_index_list)
            logger.debug("selected  %s", self.selected)
            #logger.debug("Updating role_table with id %s", self.role_table.id)

        except Exception as e:
            logger.error("Error updating checkbox at row_key %s, checkbox_col_key %s:", row_key, checkbox_col_key)
            logger.error(e)

        self.refresh_all()

        self.refresh()
    """

    def action_toggle_expand(self):
        dt = self.main_table
        for col_idx, col in enumerate(self.columns):
            if self.collapsible_cols and col in self.collapsible_cols:
                for row_idx, row in self.df.iterrows():
                    if col in self.expanded_cols:
                        dt.update_cell(row_idx, col_idx+1, self.truncate_cell(row[col], col))
                    else:
                        dt.update_cell(row_idx, col_idx+1, str(row[col]))
                if col in self.expanded_cols:
                    self.expanded_cols.remove(col)
                else:
                    self.expanded_cols.add(col)
        self.main_table.refresh()

    def on_button_pressed(self, event):
        logger.debug("Button pressed: %s", event.button.id)
        if event.button.id == "confirm":
            self.exit()
    """
    """

#from textual import on

class TableAppAllRows(BaseTableScreen, iaBase):
    CSS_PATH = "tableApp.css"
    BINDINGS = [("q", "request_quit", "Quit")]

    def __init__(self, df, columns=None, pk_col=None, collapsible_cols=None, truncate_len=10, *args, **kwargs):
        for kw in ['col_widths', 'add_data']:
            if kw in kwargs:
                logger.debug("Setting attribute %s from kwargs", kw)
                setattr(self, kw, kwargs.pop(kw))

        self.kwargs = kwargs
        self.df = df
        self.columns = columns if columns is not None else list(df.columns)
        self.pk_col = pk_col
        self.collapsible_cols = collapsible_cols
        self.truncate_len = truncate_len
        self.selected = set(range(len(df)))
        super().__init__(*args, **kwargs)


    def build_main(self):
        return self.compose_main()

    def compose_main(self) -> ComposeResult:
        self.main_table = DataTable(id="main_table")
        logger.debug('type(self.columns[0]: %s', self.columns[0])
        self.main_table.add_columns(*self.columns)
        self.set_col_attrs(self.main_table, col_widths=self.col_widths)

        logger.debug("Adding %s columns: %s", len(self.columns), self.columns )

        for idx, row in self.df.iterrows():
            row, values = self.truncate_all(row)
            row_key = self.main_table.add_row(*values, key=idx)  # Setze numerischen Key

        output = [f"{k}: {v}" for k, v in self.add_data.items()]
        #logger.debug("add_data output: %s", output)

        upper = Container(self.main_table, id="upper")

        meta = Static("\n".join(output), id="meta")
        self.current_line_dt = DataTable(id="current_line")
        self.current_line_dt.add_columns(*self.columns)
        self.set_col_attrs(self.current_line_dt, col_widths=self.col_widths)

        status = Horizontal(meta, id="status")
        lower = Vertical(self.current_line_dt, status)

        layout = Vertical(upper, lower)

        return layout

    def refresh_all(self):
        self.main_table.refresh()


    def edit_cell(self):
        dt = self.main_table
        row_idx = dt.cursor_row
        col_idx = dt.cursor_column
        col_key = list(dt.columns.keys())[col_idx]
        col_name = self.columns[col_idx]
        row_key = RowKey(row_idx)  # Korrektur: RowKey verwenden
        logger.debug("Editing cell at row %s, column %s (key: %s)", row_idx, col_name, col_key)
        current_value = dt.get_cell(row_key, col_key)
        # we want to show some fields of current row
        row = self.df.iloc[row_idx]
        #value = await self.push_screen(
        kwargs = {
            'row_idx': row_idx,
            'col_key': col_key,
            'current_value': current_value,
            'row': row,
            'col_name': col_name,
        }
        def on_close(value):
            logger.debug('value: %s', value)
            dt.update_cell(row_key, col_key, value)
            self.refresh()
        self.push_screen(EditCellDialog(**kwargs), on_close)

    def on_key(self, event: events.Key) -> None:
        # Erweiterung: Enter öffnet EditCellDialog
        if event.key == "c":
            self.edit_cell()
            event.stop()
        else:
            super().on_key(event)
