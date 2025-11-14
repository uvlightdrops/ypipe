from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt
import pandas as pd
from textual.widgets import RichLog

from .tableApp import TableApp

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class ConsoleMixin:
    """Mixin für Konsolenausgaben und Interaktion in Tasks"""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.console = Console()
        if 'quiet' in kwargs:
            self.quiet = kwargs['quiet']
        self.quiet = True # DEV

    def print(self, msg, style=None):
        self.console.print(msg, style=style)

    def show_table(self, columns, rows, title=None):
        table = Table(title=title)
        for col in columns:
            table.add_column(str(col))
        for row in rows:
            table.add_row(*[str(cell) for cell in row])
        if not self.quiet:
            self.console.print(table)

    def prompt(self, text, choices=None, default=None):
        return Prompt.ask(text, choices=choices, default=default)

    def edit_table(self, df):
        # Interaktive Bearbeitung
        self.print(f'[bold]Interaktive Bearbeitung für Gruppe: {getattr(self, "group", "")}[/bold]')
        columns = list(df.columns)
        """
        for idx, row in df.iterrows():
            table = Table(show_header=True, header_style="bold magenta")
            for col in columns:
                table.add_column(col)
            table.add_row(*[str(cell) for cell in row])
            self.console.print(table)
        """
        # XXX

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
        selected_pks = []
        df_view = df[cols_view] if cols_view else df
        rows = df_view.values.tolist()
        columns = list(df_view.columns)
        pk_idx = columns.index(pk_col)

        def truncate_cell(cell, col):
            if collapsible_cols and col in collapsible_cols:
                return str(cell)[:truncate_len] + ('...' if len(str(cell)) > truncate_len else '')
            return str(cell)

        app = TableApp(rows, columns, pk_idx, truncate_cell, collapsible_cols, truncate_len)
        app.run()
        selected_pks = [rows[i][pk_idx] for i in app.selected]
        return selected_pks

    # Weitere Methoden für Interaktion können ergänzt werden
