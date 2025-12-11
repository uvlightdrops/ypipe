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
logger = setup_logger(__name__, __name__+'.log')


class iaBase:
    def truncate_cell(self, cell, col, width):
        if isinstance(cell, float) and cell.is_integer():
            cell = str(int(cell))
        else:
            cell = str(cell)
        if isinstance(cell, str) and len(cell) > width+2:
            return cell[:width-2] + '..'
        return cell

    def truncate_cell_all(self, row):
        values = []
        for col_name, width in self.col_widths.items():
            clean = self.truncate_cell(row[col_name], col_name, width)
            row[col_name] = clean
            #if col_name in self.main_table.columns:
            if col_name in self.columns:
                values.append(clean)
        return row, values

    def truncate_all(self, row):
        values = []
        #for col_name in self.columns:
        if True:
            # we loop over all columns in the main_table to get correct order
            for col_name in self.columns: #main_table.columns:
                # name of a colum from the columnKey object
                #logger.debug("Truncating column %s", col_name)
                #if col_name in self.main_table.columns:
                width = self.col_widths.get(col_name, 10)
                clean = self.truncate_cell(row[col_name], col_name, width)
                row[col_name] = clean
                # if col_name in self.main_table.columns:
                if col_name in self.columns:
                    values.append(clean)

        return row, values


class QuitScreen(ModalScreen[bool]):
    def __init__(self, **kwargs):
        super().__init__()
        for kw in kwargs:
            #logger.debug("Setting attribute %s from kwargs", kw)
            setattr(self, kw, kwargs[kw])

    def compose(self) -> ComposeResult:
        yield Grid(
            Label("Are you sure you want to quit?", id="question"),
            Button("Quit", variant="error", id="quit"),
            Button("Cancel", variant="primary", id="cancel"),
            id="dialog",
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "quit":
            self.dismiss(True)
        else:
            self.dismiss(False)


class TableApp(App, iaBase):
    CSS_PATH = "tableApp.css"
    BINDINGS = [
        ("space", "toggle_checkbox", "Toggle Checkbox"),
        ("e", "toggle_expand", "Expand/Collapse Columns"),
        ("enter", "confirm", "Bestätigen")
    ]
    def __init__(self, df, columns=None, pk_col=None, collapsible_cols=None, truncate_len=10, *args, **kwargs):
        for kw in ['col_widths', 'add_data']:
            if kw in kwargs:
                logger.debug("Setting attribute %s from kwargs", kw)
                setattr(self, kw, kwargs.pop(kw))

        self.kwargs = kwargs
        super().__init__(*args, **kwargs)
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


    def compose(self) -> ComposeResult:
        dt = DataTable(id="main_table")
        dt.add_column("[ ]", key="checkbox")
        dt.add_columns(*self.columns)
        logger.debug("Adding %s columns: %s", len(self.columns), self.columns )
        columns_list = list(dt.columns.values())

        for idx, width in enumerate(col_widths):
            columns_list[idx].width = width

        for idx, row in self.df.iterrows():
            # XXX get width per column from self.col_widths
            #width = self.col_widths.get()
            #values = [self.truncate_cell(row[col], col, width) for col in self.columns]
            row_key = dt.add_row("[ ]", *values, key=idx)  # Setze numerischen Key
            #logger.debug("Added row %d with key %s", idx, row_key.value)


        self.prompt = Static(self.prompt_text, id="prompt")

        self.role_index_list = []
        self.role_index_list.append('alf_ArbG_schul_Admin')
        role_table = DataTable(id="role_table")
        role_table.add_column("role_index", key="role_index")
        self.role_table = role_table
        self.main_table = dt

        confirm = Button("Bestätigen", id="confirm")
        upper = Container(dt, id="upper")
        separator = Static("", id="separator")  # Separator wird jetzt per CSS gestylt
        output = [f"{k}: {v}" for k, v in self.add_data.items()]
        logger.debug("add_data output: %s", output)
        meta = Static("\n".join(output), id="meta")
        lowerright = Vertical(self.prompt, confirm, meta, id="lower_right")
        lower = Horizontal(role_table, lowerright, id="lower")
        #lower = Vertical(meta, id="lower")

        #upper.styles.height = "80%"
        #lower.styles.height = "20%"
        #layout = Vertical(upper)
        layout = Vertical(upper, separator, lower)

        yield layout


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

    """
    def action_confirm(self):
        logger.debug("Confirming selection.")
        self.exit()
    def on_button_pressed(self, event):
        logger.debug("Button pressed: %s", event.button.id)
        if event.button.id == "confirm":
            self.exit()
    """

    """
    def on_key(self, event: events.Key) -> None:
        #logger.debug("Key pressed: %s", event.key)
        if event.key == "space":
            self.toggle_checkbox()
            event.stop()
        elif event.key == "e":
            self.action_toggle_expand()
            event.stop()
        else:
            pass
        elif event.key == "enter":
            self.action_confirm()
            event.stop()
    """


class EditCellDialog(ModalScreen):
    def __init__(self, *args, **kwargs):
        super().__init__()
        for kw in ['row_idx', 'col_key', 'current_value', 'row', 'col_name']:
            if kw in kwargs:
                #logger.debug("Setting attribute %s from kwargs", kw)
                setattr(self, kw, kwargs.pop(kw))
        logger.debug(type(self.col_name))

    def compose(self):
        yield Grid(
            Label(f"Wert für Feld {self.col_name} (Title='{self.row['title']}'):", id="question"),
            Input(value=str(self.current_value), id="cell_input"),
            #Label("Are you sure you want to quit?", id="question"),
            Button("Save", variant="success", id="ok"),
            Button("Cancel", variant="primary", id="cancel"),
            id="dialog",
        )

    def on_button_pressed(self, event):
        if event.button.id == "cancel":
            self.dismiss(None)

        if event.button.id == "ok":
            value = self.query_one("#cell_input", Input).value
            logger.debug("EditCellDialog: OK pressed, dismissing with value: %s", value)
            self.dismiss(value)
        #else:
        #    self.dismiss(None)

    def on_key(self, event: events.Key) -> None:
        if event.key == "enter":
            value = self.query_one("#cell_input", Input).value
            logger.debug("EditCellDialog: Enter pressed, dismissing with value: %s", value)
            self.dismiss(value)
            #event.stop()
        elif event.key == "escape":
            self.dismiss(None)

        else:
            pass
            #super().on_key(event)

"""
"""


from textual import on

class TableAppAllRows(TableApp):
    CSS_PATH = "tableApp.css"
    BINDINGS = [("q", "request_quit", "Quit")]

    def __init__(self, df, *args, **kwargs):
        super().__init__(df, *args, **kwargs)
        self.selected = set(range(len(df)))

    def set_col_attrs(self, col_widths=None):
        logger.debug("Setting column attributes with col_widths: %s", col_widths)
        default_width = col_widths.pop('default', 10)
        for col_name, width in col_widths.items():
            if col_name in self.columns:
                value = width
            else:
                value = default_width
            logger.debug("Setting width of column %s to %s", col_name, value)

            if col_name in self.main_table.columns:
                self.main_table.columns[col_name].width = value


    def compose(self) -> ComposeResult:
        dt = DataTable(id="main_table")
        logger.debug('type(self.columns[0]: %s', self.columns[0])
        dt.add_columns(*self.columns)
        self.main_table = dt
        self.set_col_attrs(self.col_widths)

        logger.debug("Adding %s columns: %s", len(self.columns), self.columns )

        for idx, row in self.df.iterrows():
            #values = [self.truncate_cell(row[col], col, width) for col in self.columns]
            row, values = self.truncate_all(row)
            row_key = dt.add_row(*values, key=idx)  # Setze numerischen Key

        output = [f"{k}: {v}" for k, v in self.add_data.items()]
        #logger.debug("add_data output: %s", output)

        upper = Container(dt, id="upper")

        meta = Static("\n".join(output), id="meta")
        current_line_dt = DataTable(id="current_line")
        current_line_dt.add_columns(*self.columns)
        self.current_line_dt = current_line_dt

        status = Horizontal(meta, id="status")
        lower = Vertical(current_line_dt, status)

        layout = Vertical(upper, lower)

        yield layout

    def refresh_all(self):
        self.main_table.refresh()

    #@on(DataTable.RowHighlighted)
    #def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:

    def update_current_line(self):
        row = self.df.iloc[self.main_table.cursor_row]
        logger.debug("Hightlighting row %s", self.main_table.cursor_row)
        row, values = self.truncate_all(row)
        self.current_line_dt.clear()
        self.current_line_dt.add_row(*values)
        self.current_line_dt.refresh()

    def action_request_quit(self) -> None:
        def check_quit(quit: bool | None) -> None:
            """Called when QuitScreen is dismissed."""
            if quit:
                self.exit()
        self.push_screen(QuitScreen(), check_quit)

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

    #async def on_key(self, event: events.Key) -> None:
    def on_key(self, event: events.Key) -> None:
        # Erweiterung: Enter öffnet EditCellDialog
        # on cursor up and down
        if event.key in ["up", "down", "left", "right"]:
            event.stop()
            #super().on_key(event)
            self.update_current_line()
        elif event.key == "space":
            self.edit_cell()
            event.stop()
        elif event.key == "e":
            self.action_toggle_expand()
            event.stop()
        else:
            pass
            #super().on_key(event)
