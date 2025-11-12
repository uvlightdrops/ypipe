from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt
import pandas as pd
from textual.widgets import RichLog


from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class ConsoleMixin:
    """Mixin für Konsolenausgaben und Interaktion in Tasks"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.console = Console()

    def print(self, msg, style=None):
        self.console.print(msg, style=style)

    def show_table(self, columns, rows, title=None):
        table = Table(title=title)
        for col in columns:
            table.add_column(str(col))
        for row in rows:
            table.add_row(*[str(cell) for cell in row])
        self.console.print(table)

    def prompt(self, text, choices=None, default=None):
        return Prompt.ask(text, choices=choices, default=default)

    def edit_table(self, df):
        # Interaktive Bearbeitung
        self.print(f'[bold]Interaktive Bearbeitung für Gruppe: {getattr(self, "group", "")}[/bold]')
        columns = list(df.columns)

        for idx, row in df.iterrows():
            table = Table(show_header=True, header_style="bold magenta")
            for col in columns:
                table.add_column(col)
            table.add_row(*[str(cell) for cell in row])
            self.console.print(table)


        # Nach Abschluss gesamte Tabelle anzeigen
        self.show_table(columns, df.values.tolist(), title="Bearbeitetes Ergebnis")

    def ia_merge(self, df1, df2):
        # Merge-Vorschlag
        merged = []
        print()
        self.print(f'[bold]Interaktiver Merge für Gruppe: {group}[/bold]')
        # Zeilenweise durchgehen
        for idx, (row1, row2) in enumerate(zip(df1.itertuples(index=False), df2.itertuples(index=False))):
            table = Table(show_header=True, header_style="bold magenta")
            table.add_column("Frame1")
            table.add_column("Frame2")
            table.add_row(str(row1), str(row2))
            self.console.print(table)
            choice = Prompt.ask(f"Zeile {idx + 1} übernehmen?", choices=["y", "n"], default="y")
            if choice == "y":
                merged.append(row1)
        # Ergebnis-DataFrame
        df_merged = pd.DataFrame(merged, columns=df1.columns)

    def select_pks(self, df, prompt_text="Gib pk-Wert(e) zum Auswählen ein (Komma getrennt, Enter für Abbruch):"):
        selected_pks = []
        while True:
            pk_input = self.prompt(prompt_text, default="")
            if not pk_input:
                break
            try:
                selected_pks += [int(pk.strip()) for pk in pk_input.split(",") if pk.strip()]
            except ValueError:
                self.print("Ungültige Eingabe, bitte nur Zahlen eingeben.", style="red")
                continue
        return selected_pks

    def select_rows_with_checkboxes(self, df, pk_col='pk', cols_view=None, title=None, truncate_len=10, collapsible_cols=None):
        """
        Interaktive Auswahl von Zeilen mit Checkboxen (textual).
        Optional: Spalten können gekürzt oder ausgeklappt werden.
        Gibt Liste der ausgewählten pk-Werte zurück.
        """
        from textual.app import App, ComposeResult
        from textual.widgets import DataTable, Button
        from textual.containers import Container
        from textual import events
        from textual.binding import Binding

        selected_pks = []
        df_view = df[cols_view] if cols_view else df
        rows = df_view.values.tolist()
        columns = list(df_view.columns)
        pk_idx = columns.index(pk_col)

        # Truncate cell text for specified columns
        def truncate_cell(cell, col):
            if collapsible_cols and col in collapsible_cols:
                return str(cell)[:truncate_len] + ('...' if len(str(cell)) > truncate_len else '')
            return str(cell)

        class TableApp(App):
            CSS_PATH = None
            BINDINGS = [
                ("space", "toggle_checkbox", "Toggle Checkbox"),
                ("e", "toggle_expand", "Expand/Collapse Columns"),
                ("enter", "confirm", "Bestätigen")
            ]
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.selected = set()
                self.expanded_cols = set()
            def compose(self) -> ComposeResult:
                dt = DataTable()
                dt.add_columns("[ ]", *columns)
                for row in rows:
                    dt.add_row("[ ]", *[truncate_cell(cell, col) for cell, col in zip(row, columns)])
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
                for col_idx, col in enumerate(columns):
                    if collapsible_cols and col in collapsible_cols:
                        for row_idx, row in enumerate(rows):
                            if col in self.expanded_cols:
                                dt.update_cell(row_idx, col_idx+1, truncate_cell(row[col_idx], col))
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
                # Nur beenden, wenn Button "Bestätigen" gedrückt wurde
                #if hasattr(event, 'button') and getattr(event.button, 'id', None) == "confirm":
                #    self.exit()
            def on_key(self, event: events.Key) -> None:
                self.query_one(RichLog).write(event)

            """
            def on_key(self, event):
                if event.key == "space":
                    self.action_toggle_checkbox()
                    event.stop()
                elif event.key == "e":
                    self.action_toggle_expand()
                    event.stop()
                elif event.key == "enter":
                    self.action_confirm()
                    event.stop()
            """

        app = TableApp()
        app.run()
        selected_pks = [rows[i][pk_idx] for i in app.selected]
        return selected_pks

    # Weitere Methoden für Interaktion können ergänzt werden
