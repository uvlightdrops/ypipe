from rich.console import Console
from rich.table import Table
from rich.prompt import Prompt
from rich.live import Live
import pandas as pd
from textual.widgets import DataTable

from .tableApp import TableApp, TableAppAllRows

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
        #table = DataTable(title=title)
        for col in columns:
            table.add_column(str(col))
        for row in rows:
            table.add_row(*[str(cell) for cell in row])
        if not self.quiet:
            self.console.print(table)

    def prompt(self, text, choices=None, default=None):
        return Prompt.ask(text, choices=choices, default=default)


    def gen_table(self, row, cols_dismiss=None):
        table = Table(show_header=True, header_style="bold magenta")
        # Add columns to the table, excluding those in cols_dismiss
        columns = [col for col in row.keys() if not (cols_dismiss and col in cols_dismiss)]
        logger.debug('cols L=%s not to dismiss: %s', len(columns), columns)
        for col in columns:
            table.add_column(col)
        table.add_row(*[str(row[col]) for col in columns])
        return table

    def update_table(self, columns, row, cols_prompt=None):
        # Prompt für jede Spalte nacheinander
        input_d = {}
        for col in columns:
            if col not in cols_prompt:
                input_d[col] = row[col]
                continue
            new_value = self.prompt(f"{col} [{row[col]}]:", default=str(row[col]))
            input_row.append(new_value)
        logger.debug('input_row L=%s: %s', len(input_row), input_row)

        # new update_table: param for all user inputs given before,
        # loop this dict of user input and compare with current row values
        # if different, generate new table with updated values
        updated_row = {}
        for idx, col in enumerate(columns):
            if col in cols_prompt:
                updated_row[col] = input_row[idx]
            else:
                updated_row[col] = row[col]


    def edit_and_confirm_app(self, df, cols_prompt=None, cols_dismiss=None):
        columns = [col for col in df.columns if not (cols_dismiss and col in cols_dismiss)]
        logger.debug('ia keyword update cols L=%s not to dismiss: %s', len(columns), columns)
        add_data = {'task': self.name}

        col_widths = self.kp_pf.get('col_widths', {})

        app = TableAppAllRows(df, columns=columns, pk_col=None,
                              add_data=add_data, col_widths=col_widths)
        app.run()

        confirmed_rows = [df.iloc[idx] for idx in app.selected]
        df_confirmed = pd.DataFrame(confirmed_rows)
        return df_confirmed


    def edit_and_confirm_row(self, row, cols_prompt=None, cols_dismiss=None):
        columns = [col for col in row.keys() if not (cols_dismiss and col in cols_dismiss)]
        if cols_prompt is None:
            cols_prompt = columns
        # Initiale Tabelle
        table = Table(show_header=True, header_style="bold magenta",
                      col_width=self.kp_pf.get('col_widths', {}))
        for col in columns:
            table.add_column(col)
        table.add_row(*[str(row[col]) for col in columns])

        # Live-Kontext starten
        with Live(table, console=self.console, refresh_per_second=4) as live:
            input_row = [str(row[col]) for col in columns]
            for idx, col in enumerate(columns):
                if col not in cols_prompt:
                    continue
                new_value = self.prompt(f"{col} [{row[col]}]:", default=str(row[col]))
                input_row[idx] = new_value
                # Tabelle neu generieren mit aktualisierten Werten
                new_table = Table(show_header=True, header_style="bold magenta")
                for c in columns:
                    new_table.add_column(c)
                new_table.add_row(*[str(row[col]) for col in columns])
                new_table.add_row(*input_row)
                live.update(new_table)

            # Update row dict
            for idx, col in enumerate(columns):
                if col in cols_prompt:
                    row[col] = input_row[idx]
            confirm = self.prompt("Zeile bestätigen? (y/n):", choices=["y", "n"], default="y")
        return confirm == "y", row


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
    """
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
    """

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

        app = TableApp(rows, columns, pk_idx, truncate_cell, collapsible_cols,
                       truncate_len, col_widths=self.kp_pf.get('col_widths', {}))
        app.run()
        selected_pks = [rows[i][pk_idx] for i in app.selected]
        return selected_pks

    # Weitere Methoden für Interaktion können ergänzt werden
