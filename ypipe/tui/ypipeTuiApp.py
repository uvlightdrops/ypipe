from textual.app import App, on
from textual import work
from textual.containers import Horizontal, Vertical
from textual.widgets import ListView, ListItem, Label, Button, Static, Collapsible, RichLog
from ypipe.baseScreen import BaseScreen
from ypipe.pipeline import Pipeline
from .pipeline_tui_view import PipelineTUIView
from textual.worker import Worker
from rich.markup import escape
from rich.text import Text

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')
print(__name__)

class YpipeTuiApp(BaseScreen):
# Weitere Widgets und Views können hier hinzugefügt werden
    CSS_PATH = "ypipe_tui_app.css"
    def __init__(self, pipeline: Pipeline, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline = pipeline
        self.pipeline.load_task_definitions()
        self.view = PipelineTUIView(pipeline)
        self.task_views = {}
        self.pipeline.register_status_callback(self.on_task_status)
        # Pipeline-Status-Anzeige
        # Entfernt: Step-Modus-Variablen, diese sind jetzt in Pipeline

    def on_task_status(self, task_name, status):
        # Wird von der Pipeline aufgerufen, wenn ein Task startet/fertig ist
        # UI-Update im richtigen Thread
        self.call_from_thread(self.update_task_status, task_name, status)

    def update_task_status(self, task_name, status):
        # Status-Log aktualisieren
        self.status_log.write(Text.from_markup(f"[b]{task_name}[/b]: {status}"))
        # Pipeline-Status aktualisieren
        self.pipeline_status_label.update(f"[b]{task_name}[/b]")
        # Status im DataTable aktualisieren
        if hasattr(self.view, "update_task_status_in_table"):
            self.view.update_task_status_in_table(task_name, status)
        if status == "stopped":
            self.status_log.write(Text.from_markup("[red]Pipeline wurde gestoppt (action=stop). Die TUI bleibt aktiv.[/red]"))
            self.pipeline_status_label.update("[red]gestoppt[/red]")
        if task_name in self.task_views:
            self.task_views[task_name].add_class("highlight")

    def build_main(self):
        return self.compose_main()

    def compose_main(self):
        left_pane = self.view
        self.task_views = self.create_task_views()

        # Control Panel mit Buttons
        self.pipeline_status_label = Label("[b]loaded[/b]", id="pipeline_status_label")
        control_panel = Horizontal(
            Vertical(
                Static("[b]%s[/b]" %self.pipeline.plname),
                self.pipeline_status_label,
                classes="ControlPanel",
        ),
            Button("Start", id="btn_start"),
            Button("Stop", id="btn_stop"),
            Button("Nächster Task", id="btn_next"),
            classes="CPanel"
        )
        # Pipeline-Status-Anzeige
        status_label_panel = Horizontal(self.pipeline_status_label, classes="StatusLabelPanel")
        # RichLog für Status/Log-Ausgabe
        self.status_log = RichLog(id="status_log", max_lines=10, highlight=True)
        status_panel = Horizontal(self.status_log, classes="SPanel")

        # Task-Views als Collapsible-ListView
        listarg = []
        for name, task_view in self.task_views.items():
            # Collapsible: Task-Name als Überschrift, TaskView als Inhalt (collapsed by default)
            collapsible = Collapsible(task_view, title=name, collapsed=True)
            listarg.append(ListItem(collapsible))
        content_cont = ListView(*listarg)

        # Layout: ControlPanel oben, darunter StatusLabel, darunter die TaskViews
        main_content = Vertical(control_panel, status_panel, content_cont)
        layout = Horizontal(left_pane, main_content)
        return layout

    def create_task_views(self):
        from ypipe.tui.task_tui_view import TaskView
        task_views = {}
        #logger.debug(self.pipeline.task_defs)
        for name, task_def in self.pipeline.task_defs.items():
            #logger.debug("task_def: %s", task_def)
            task_view = TaskView(task_def)
            task_views[name] = task_view
        return task_views

    @on(Button.Pressed, "#btn_start")
    def on_start_pressed(self, event: Button.Pressed) -> None:
        # Pipeline im Hintergrund starten with @work
        self.run_pipeline_in_background()

    @work(thread=True)
    def run_pipeline_in_background(self):
        try:
            self.pipeline.run_all()
            self.call_from_thread(self._set_status_done)
        except Exception as e:
            raise e
            logger.debug("Fehler in Pipeline: %s", str(e))
            self.call_from_thread(self._set_status_error, str(e))

    def _set_status_done(self):
        self.status_log.write(Text.from_markup("[green]Pipeline fertig![/green]"))
        self.pipeline_status_label.update("[green]DONE[/green]")

    def _set_status_error(self, msg):
        self.status_log.write(Text.from_markup(f"[red]Error: {escape(msg)}[/red]"))
        self.pipeline_status_label.update(f"[red]Error[/red] {escape(msg)}")

    @on(Button.Pressed, "#btn_stop")
    def on_stop_pressed(self, event: Button.Pressed) -> None:
        # Hier ggf. Pipeline-Stop-Logik ergänzen
        pass

    @on(Button.Pressed, "#btn_next")
    def on_next_pressed(self, event: Button.Pressed) -> None:
        self.run_next_task_in_background()

    @work(thread=True)
    def run_next_task_in_background(self):
        # Step-Modus über Pipeline-API
        if not hasattr(self.pipeline, '_step_order') or self.pipeline._step_order is None:
            self.pipeline.step_init()
        task_name = self.pipeline.step_next()
        logger.info("Nächster Task im Step-Modus: %s", task_name)
         # UI-Update
        if task_name is None:
            self.call_from_thread(self._set_status_done)
        else:
            self.call_from_thread(self.status_log.write, Text.from_markup(f"[yellow]Task {task_name} fertig![/yellow]"))
