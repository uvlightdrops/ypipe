from textual.widgets import Button, Label
from textual.containers import Grid
from textual.screen import ModalScreen
from textual.app import ComposeResult

class QuitScreen(ModalScreen[bool]):
    def __init__(self, **kwargs):
        super().__init__()
        for kw in kwargs:
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


class EditCellDialog(ModalScreen):
    def __init__(self, *args, **kwargs):
        super().__init__()
        for kw in ['row_idx', 'col_key', 'current_value', 'row', 'col_name']:
            if kw in kwargs:
                setattr(self, kw, kwargs.pop(kw))

    def compose(self):
        yield Grid(
            Label(f"Wert für Feld {self.col_name} (Title='{self.row['title']}'):", id="question"),
            Input(value=str(self.current_value), id="cell_input"),
            Button("Save", variant="success", id="ok"),
            Button("Cancel", variant="primary", id="cancel"),
            id="dialog",
        )

    def on_button_pressed(self, event):
        if event.button.id == "cancel":
            self.dismiss(None)
        if event.button.id == "ok":
            value = self.query_one("#cell_input", Input).value
            self.dismiss(value)

    def on_key(self, event):
        if event.key == "enter":
            value = self.query_one("#cell_input", Input).value
            self.dismiss(value)
        elif event.key == "escape":
            self.dismiss(None)
# ...existing code...
