from textual.app import App, ComposeResult
from textual.widgets import Header, Footer
from .dialogs import QuitScreen

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class BaseScreen(App):
    """Basis-Screen mit Standard-Header, Footer und generischem on_key-Handler."""
    def compose(self) -> ComposeResult:
        yield Header()
        yield self.build_main()
        yield Footer()

    def build_main(self):
        # Platzhalter für den Hauptinhalt, von Subklassen zu überschreiben
        return None

    async def on_key(self, event):
        # Standard-Key-Handling für Navigation und App-Exit
        if event.key == "escape":
            if hasattr(self, "action_request_quit"):
                self.action_request_quit()
            event.stop()
        else:
            pass
        return


class BaseTableScreen(BaseScreen):
    """Erweiterung von BaseScreen für Tabellenanwendungen."""

    def action_confirm(self):
        logger.debug("Confirming selection.")


    def action_request_quit(self) -> None:
        def check_quit(quit: bool | None) -> None:
            """Called when QuitScreen is dismissed."""
            if quit:
                self.exit()
        self.push_screen(QuitScreen(), check_quit)

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

    async def on_key(self, event):
        if event.key in ["up", "down", "left", "right"]:
            await super().on_key(event)  # Erst das Framework bewegen lassen!
            if event.key in ["up", "down"]:
                if hasattr(self, "update_current_line"):
                    self.call_later(self.update_current_line)
            if hasattr(self, "inp_widget"):
                self.call_later(self.update_input_field)
            event.stop()
            return
        elif event.key == "space":
            self.toggle_checkbox()
            event.stop()
        elif event.key == "e":
            self.action_toggle_expand()
            event.stop()
        elif event.key == "enter":
            self.action_confirm()
            event.stop()
        else:
            await super().on_key(event)
