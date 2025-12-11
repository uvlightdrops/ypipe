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

    def on_key(self, event):
        # Standard-Key-Handling für Navigation und App-Exit
        if event.key == "escape":
            if hasattr(self, "action_request_quit"):
                self.action_request_quit()
            event.stop()
        else:
            pass


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

    def on_key(self, event):
        if event.key in ["up", "down", "left", "right"]:
            event.stop()
            if hasattr(self, "update_current_line"):
                self.update_current_line()
        if event.key == "space":
            self.toggle_checkbox()
            event.stop()
        elif event.key == "e":
            self.action_toggle_expand()
            event.stop()
        elif event.key == "enter":
            self.action_confirm()
            event.stop()
        else:
            super().on_key(event)
