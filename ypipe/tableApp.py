from textual.app import App, ComposeResult
from textual.widgets import DataTable, Button, Static, Input, Label, Footer, Header
from textual.widgets import SelectionList, RadioButton, RadioSet, OptionList
from textual.widgets.selection_list import Selection
from textual.widgets.option_list import Option
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
from .dialogs import QuitScreen, EditCellDialog, SelectCellDialog
from .baseScreen import BaseScreen, BaseTableScreen
logger = setup_logger(__name__, __name__+'.log')


class TableAppBase(BaseTableScreen):
    CSS_PATH = "tableApp.css"

    def __init__(self, df, columns=None, pk_col=None, collapsible_cols=None, truncate_len=10, *args, **kwargs):
        logger.debug("init TableApp with kwargs %s", kwargs)
        for kw in ['col_widths', 'col_widths_max', 'add_data', 'canonical_data']:
            if kw in kwargs:
                logger.debug("Setting attribute %s from kwargs", kw)
                setattr(self, kw, kwargs.pop(kw))
            else:
                logger.debug("Attribute %s not in kwargs, setting default", kw)
        self.kwargs = kwargs
        self.df = df
        self.columns = columns if columns is not None else list(df.columns)
        self.pk_col = pk_col # if pk_col is not None else self.columns[0]
        self.collapsible_cols = collapsible_cols
        self.truncate_len = truncate_len
        logger.debug("TableApp initialized with columns: %s", self.columns)
        # we dont have another custom base class, this inits the textual App class
        super().__init__(*args, **kwargs)
        #super().__init__()


class TableApp(TableAppBase, iaBase):
    BINDINGS = [
        ("space", "toggle_checkbox", "Toggle Checkbox"),
        ("e", "toggle_expand", "Expand/Collapse Columns"),
        ("enter", "confirm", "Bestätigen")
    ]
    def __init__(self, df, columns=None, pk_col=None, collapsible_cols=None, truncate_len=10, *args, **kwargs):
        self.expanded_cols = set()
        self.prompt_text = "Zuordn.OK? (Space/Enter/other)?"
        self.role_index_list = []
        self.selected = set()
        # XXX remove truncate_len, we never need?
        logger.debug('kwargs in TableApp: %s', kwargs)
        super().__init__(df, columns=columns, pk_col=pk_col, collapsible_cols=collapsible_cols,
                         truncate_len=truncate_len, *args, **kwargs)

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

    def on_button_pressed(self, event):
        logger.debug("Button pressed: %s", event.button.id)
        if event.button.id == "confirm":
            self.exit()
    """
    """

#from textual import on

class TableAppAllRows(TableAppBase, iaBase):
    BINDINGS = [("q", "request_quit", "Quit")]

    def __init__(self, df, columns=None, pk_col=None, collapsible_cols=None, truncate_len=10, *args, **kwargs):
        self.selected = set(range(len(df)))
        super().__init__(df, columns=columns, pk_col=pk_col, collapsible_cols=collapsible_cols,
                         truncate_len=truncate_len, *args, **kwargs)

        output = "\n".join( [f"{k}: {v}" for k, v in self.add_data.items()] )
        self.footer_text = "c: Edit Cell | l: Edit Inline | Enter: Edit Current Line | q: Quit || "+output


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


        upper = Container(self.main_table, id="upper")

        output = [f"{k}: {v}" for k, v in self.add_data.items()]
        logger.debug("add_data output: %s", output)
        meta = Static("\n".join(output), id="meta")
        self.current_line_dt = DataTable(id="current_line")
        self.current_line_dt.add_columns(*self.columns)
        self.set_col_attrs(self.current_line_dt, col_widths=self.col_widths)


        verticals = self.build_preset_optionlists()

        self.inp_label = Label('label')
        self.inp_label.styles.margin = (0,0,1,3)
        self.inp_widget = Input(placeholder="Freitext Eingabe", id="freeinput")
        inputcont = Vertical(self.inp_label, self.inp_widget, id="inputcont")

        #verticals = []
        logger.debug("Prepared %s preset verticals", len(verticals))
        # Jetzt alle Verticals nebeneinander in einem Horizontal-Container
        spacer = Static("", id="spacer_preset")
        spacer_r = Static("", id="spacer_r")
        spacer.styles.width = 30
        spacer_r.styles.width = 10
        commitbut = Button("Tabelle übernehmen", id="commitbut")
        presetbuttons = Horizontal(spacer, *verticals, spacer_r, inputcont, commitbut, id="presetbuttons")

        #status = Horizontal(meta, id="status")

        lower = Vertical(self.current_line_dt, presetbuttons) #, status)

        layout = Vertical(upper, lower)

        return layout

    def build_preset_optionlists(self):
        self.prepare_presets()
        verticals = []
        sig_keys = list(self.presets.keys())
        sig_keys = ['sig_item', 'sig_app', 'sig_behd', 'sig_crit']
        # build verticals for each preset, but only for those in sig_keys
        for col_name in sig_keys:
            #logger.debug("Preparing preset for col: %s", col_name)
            width = self.col_widths.get(col_name, 12)
            label = Label(col_name, id=f"label_{col_name}")
            presets = self.presets.get(col_name, [])
            #logger.debug('presets for %s: %s', col_name, presets)
            selections = set()
            for value in presets:
                selections.add(Option(value, id=f"option_{col_name}_{value}"))

            # maybe *selections
            body = OptionList(*selections, id=f"preset_{col_name}")
            width += 5
            item = Vertical(label, body, id=f"preset_container_{col_name}")
            item.styles.width = (width)
            verticals.append(item)
        return verticals

    def build_editable_line(self):
        preset_buttons = {}
        self.prepare_presets()
        # logger.debug("Presets prepared: %s", self.presets)
        verticals = []

        sig_keys = list(self.presets.keys())
        logger.debug("Preparing preset buttons for sig_keys: %s", sig_keys)

        label = Label(' stat ', id=f"label_stat")
        body = Static(" XXX", id=f"spacer_stat")
        item = Vertical(label, body, id=f"preset_container_stat")
        width = self.col_widths.get('stat', 12)
        item.styles.width = (width)
        verticals.append(item)

        for col_name in self.columns[1:14]:
            logger.debug("Preparing preset for col: %s", col_name)
            width = self.col_widths.get(col_name, 12)
            cname = col_name
            if col_name.startswith('sig_'):
                cname = 's_'+col_name[4:]
            label = Label(cname, id=f"label_{col_name}")

            if col_name not in sig_keys:
                body = Static("X"*(width-2), id=f"spacer_{col_name}")
                #item = Label(col_name)
                # width -= 1

            elif col_name in sig_keys:
                presets = self.presets.get(col_name, [])
                logger.debug('presets for %s: %s', col_name, presets)
                selections = set()
                for value in presets:
                    selections.add(Option(value))

                # maybe *selections
                body = OptionList(*selections, id=f"preset_{col_name}")
                width +=3

            item = Vertical(label, body, id=f"preset_container_{col_name}")
            # logger.debug("Setting width %s for preset item %s", width, item)
            item.styles.width = (width)
            verticals.append(item)

        return verticals


    def prepare_presets(self):
        # use values from config_d and set hotkeys for them
        #logger.debug('canonical_data: %s', self.canonical_data)
        sig_app     = self.canonical_data.get('app')
        sig_behd    = self.canonical_data.get('behd')
        sig_crit    = self.canonical_data.get('crit')
        sig_item    = self.canonical_data.get('item')
        self.presets = {
            'sig_app': sig_app,
            'sig_behd': sig_behd,
            'sig_crit': sig_crit,
            'sig_item': sig_item,
        #    'sig_hn': sig_hn,
        }

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
            if value is None:
                return
            logger.debug('value: %s', value)
            dt.update_cell(row_key, col_key, value)
            self.refresh()

        self.push_screen(EditCellDialog(**kwargs), on_close)


    # another method to edit all fields of current line
    # but directly inline in the datatable current_line_dt
    def edit_current_inline(self):
        dt = self.current_line_dt
        row_idx = 0  # only one row
        col_idx = dt.cursor_column
        logger.debug("col_idx: %s", col_idx)
        col_key = list(dt.columns.keys())[col_idx]
        col_name = self.columns[col_idx]

        # Es gibt genau eine Zeile, hole den einzigen RowKey
        #only_row_key = next(iter(dt.rows))
        only_row_key = list[dt.rows.values()][0]
        logger.debug("only_row_key: %s", only_row_key)
        current_value = dt.get_cell(only_row_key, col_key)
        logger.debug("cur val: %s", current_value)


    def edit_current_line(self):
        dt = self.main_table
        row_idx = dt.cursor_row
        row = self.df.iloc[row_idx]
        col_keys = list(dt.columns.keys())
        col_names = self.columns
        presets = getattr(self, 'presets', {})

        def edit_next_field(idx=0):
            if idx >= len(col_names):
                self.refresh()
                return
            col_key = col_keys[idx]
            col_name = col_names[idx]
            row_key = RowKey(row_idx)
            current_value = dt.get_cell(row_key, col_key)
            kwargs = {
                'row_idx': row_idx,
                'col_key': col_key,
                'current_value': current_value,
                'row': row,
                'col_name': col_name,
            }
            def on_close(value):
                if value is not None:
                    dt.update_cell(row_key, col_key, value)
                # Weiter zum nächsten Feld
                edit_next_field(idx+1)
            # Prüfe, ob Presets für dieses Feld existieren
            if col_name in presets:
                self.push_screen(SelectCellDialog(presets[col_name], **kwargs), on_close)
            else:
                self.push_screen(EditCellDialog(**kwargs), on_close)
        edit_next_field()

    def on_button_pressed(self, event):
        logger.debug("Button pressed: %s", event.button.id)
        if event.button.id == "commitbut":
            # save the current table back to self.df
            dt = self.main_table

            col_list = list(dt.columns.keys())
            dd = {}
            row_idx = 0
            # we need the list of column keys
            # oder ein iterable für die columns

            for col_idx, col in dt.columns.items():
                logger.debug("Committing col %s (idx %s) back to dd", col, col_idx)
                values = list(dt.get_column(col_idx))
                #logger.debug("values: %s", values)
                dd[str(col.label)] = values

            """
            for row in dt.rows.values():
                row_idx = row.key
                val_line = []
                dd[row_idx.value] = []
                #logger.debug("Committing row %s back to self.df", row_idx.value)
                for col_idx, col in enumerate(self.columns):
                    col_key = col_list[col_idx]
                    #logger.debug("Committing col %s col_key %s, (idx %s)", col, col_key, col_idx)
                    value = dt.get_cell(row_idx, col_key)
                    val_line.append(value)
                    if col == 'sig_item' and row_idx.value in [0, 1, 2]:
                        logger.debug("Commit value %s at row %s, col %s", value, row_idx.value, col)
                logger.debug("val_line : %s", len(val_line))
                dd[row_idx.value].append(val_line)
            """
            logger.debug("len(dd): %s", len(dd))

            import pandas as pd
            self.result = pd.DataFrame(dd, columns=self.columns)
            debugfields = ['stat', 'info', 'pk', 'sig_item', 'sig_app', 'sig_behd', 'sig_crit']
            logger.debug("Table committed back to self.df")
            logger.debug(self.result[debugfields].head())
            self.exit()

    def on_key(self, event: events.Key) -> None:
        # Erweiterung: Enter öffnet edit_current_line
        if self.focused is self.main_table:
            if event.key == 'enter':
                # check current col, if it is one of sig_key
                # then we focus the correspnding option list
                col_idx = self.main_table.cursor_column
                col_name = self.columns[col_idx]
                if col_name in self.presets:
                    preset_id = f"preset_{col_name}"
                    preset_widget = self.query_one(f"#{preset_id}", OptionList)
                    logger.debug("Focusing preset widget %s for col %s", preset_id, col_name)
                    self.set_focus(preset_widget)
                else:
                    logger.debug("No presets for col %s, focusing inp_widget", col_name)
                    self.set_focus(self.inp_widget)
                event.stop()

            elif event.key == "c":
                self.edit_cell()
                event.stop()
            elif event.key == "l":
                self.edit_current_inline()
                event.stop()
            else:
                pass

        elif self.focused is self.inp_widget:
            if event.key == 'enter':
                # Wert aus Input-Feld in die aktuelle Zelle schreiben
                dt = self.main_table
                row_idx = dt.cursor_row
                col_idx = dt.cursor_column
                col_key = list(dt.columns.keys())[col_idx]
                row_key = RowKey(row_idx)
                new_value = self.inp_widget.value
                logger.debug("Input widget enter: updating row %s, col %s with value %s", row_idx, col_key, new_value)
                dt.update_cell(row_key, col_key, new_value)
                self.refresh()
                self.main_table.focus()
                event.stop()
            elif event.key == 'escape':
                self.main_table.focus()
                event.stop()
            else:
                pass
                super().on_key(event)

        elif self.focused.__class__ is OptionList:
            option_list: OptionList = self.focused
            if event.key == 'enter':
                selected_option = option_list.highlighted_option
                #logger.debug("OptionList enter: selected option: %s", selected_option)
                if selected_option is not None:
                    selected_value = selected_option.prompt
                    # Wert in die entsprechende Zelle schreiben
                    dt = self.main_table
                    row_idx = dt.cursor_row
                    col_idx = dt.cursor_column
                    col_key = list(dt.columns.keys())[col_idx]
                    row_key = RowKey(row_idx)
                    logger.debug("OptionList enter: updating row %s, col %s with value %s", row_idx, col_key, selected_value)
                    dt.update_cell(row_key, col_key, selected_value)
                    self.refresh()
                self.main_table.focus()
                event.stop()
            elif event.key == 'escape':
                self.main_table.focus()
                event.stop()
        """
        """



        # Erweiterung: Cursorsteuerung und Edit für current_line_dt
        """
        if self.focused is self.current_line_dt:
            dt = self.current_line_dt
            logger.debug("current_line_dt focused %s", dt)
            row_idx = 0  # Nur eine Zeile
            col_idx = dt.cursor_column
            col_key = list(dt.columns.keys())[col_idx]
            col_name = self.columns[col_idx]
            row_key = dt.rows[0].key
            current_value = dt.get_cell(row_key, col_key)
            row = self.df.iloc[self.main_table.cursor_row]
            presets = getattr(self, 'presets', {})
            kwargs = {
                'row_idx': row_idx,
                'col_key': col_key,
                'current_value': current_value,
                'row': row,
                'col_name': col_name,
            }
            if event.key == "enter":
                def on_close(value):
                    if value is not None:
                        dt.update_cell(row_key, col_key, value)
                        # Optional: Nach rechts springen
                        if col_idx + 1 < len(self.columns):
                            dt.move_cursor(column=col_idx + 1)
                    self.refresh()
                if col_name in presets:
                    self.push_screen(SelectCellDialog(presets[col_name], **kwargs), on_close)
                else:
                    self.push_screen(EditCellDialog(**kwargs), on_close)
                event.stop()
            elif event.key == "right":
                if col_idx + 1 < len(self.columns):
                    dt.move_cursor(column=col_idx + 1)
                event.stop()
            elif event.key == "left":
                if col_idx > 0:
                    dt.move_cursor(column=col_idx - 1)
                event.stop()
            elif event.key == "escape":
                self.main_table.focus()
                event.stop()
            else:
                super().on_key(event)
        """

    def focus_current_line(self):
        self.current_line_dt.focus()
        self.current_line_dt.move_cursor(row=0, column=0)

    def to_dataframe(self, index_col=None):
        """
        Extrahiere die aktuelle Tabelle aus self.main_table als pandas DataFrame.
        index_col: Name der Spalte, die als Index verwendet werden soll (optional)
        """
        import pandas as pd
        dt = self.main_table
        # Spaltennamen extrahieren
        columns = [col.label if hasattr(col, 'label') and col.label else str(col.key) for col in dt.columns.values()]
        data = []
        row_keys = list(dt.rows.keys())
        for row_key in row_keys:
            values = [dt.get_cell(row_key, col.key) for col in dt.columns.values()]
            data.append(values)
        df = pd.DataFrame(data, columns=columns)
        if index_col and index_col in df.columns:
            df.set_index(index_col, inplace=True)
        return df
