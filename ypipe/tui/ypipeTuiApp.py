from textual.app import App, on
from textual import work
from textual.containers import Horizontal, Vertical
from textual.widgets import ListView, ListItem, Label, Button, Static, Collapsible, RichLog
from ypipe.baseScreen import BaseScreen
from ypipe.pipeline import Pipeline
from .pipeline_tui_view import PipelineTUIView, PipelineContainer
from .task_result_view import TaskResultView
from textual.worker import Worker
from rich.markup import escape
from rich.text import Text

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')
print(__name__)

class YpipeTuiApp(BaseScreen):
# Weitere Widgets und Views können hier hinzugefügt werden
    CSS_PATH = "ypipe_tui_app.css"
    # YpipeTuiApp wird selbst ein OutputHandler für die Pipeline
    def __init__(self, ypipe_app, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.ypipe_app = ypipe_app
        self.pipeline = ypipe_app.active_pipeline
        self.pipeline.load_task_definitions()
        #self.view_pipeline = PipelineTUIView(pipeline)
        self.view_pipeline = PipelineContainer(self.pipeline)
        self.view_taskresult = TaskResultView(self.pipeline)
        self.task_views = {}

        # Registriere die TUI als OutputHandler/Observer
        self.pipeline.register_task_status_callback(self.on_task_status)
        self.pipeline.register_pipeline_status_callback(self.on_pipeline_status)
        # Pipeline-Status-Anzeige
        # Entfernt: Step-Modus-Variablen, diese sind jetzt in Pipeline

    def on_task_status(self, task_name, status):
        # Wird von der Pipeline aufgerufen, wenn ein Task startet/fertig ist
        self.call_from_thread(self.update_task_status, task_name, status)
        if status == "done":
            self.view_taskresult.update(task_name=task_name)

    def on_pipeline_status(self, pl_name, status):
        # Wird von der Pipeline aufgerufen, wenn sich der Pipeline-Status ändert
        self.call_from_thread(self.status_log.write, Text.from_markup(f"[b]Pipeline {pl_name} status:[/b] {status}"))
        if status == "stopped":
            self.status_log.write(Text.from_markup("[red]Pipeline wurde gestoppt (action=stop). Die TUI bleibt aktiv.[/red]"))
            self.pipeline_status_label.update("[red]gestoppt[/red]")
        elif status == "enter sub-pipeline":
            self.status_log.write(Text.from_markup(f"[blue]Entering sub-pipeline {pl_name}[/blue]"))
            self.view_pipeline.add_sub_table()


    def update_task_status(self, task_name, status):
        # Status-Log aktualisieren
        self.status_log.write(Text.from_markup(f"[b]{task_name}[/b]: {status}"))
        # Pipeline-Status aktualisieren
        self.pipeline_status_label.update(f"[b]{task_name}[/b]")
        # Status im DataTable aktualisieren
        if hasattr(self.view_pipeline, "update_task_status_in_table"):
            self.view_pipeline.update_task_status_in_table(task_name, status)
        if status == "stopped":
            self.status_log.write(Text.from_markup("[red]Pipeline wurde gestoppt (action=stop). Die TUI bleibt aktiv.[/red]"))
            self.pipeline_status_label.update("[red]gestoppt[/red]")
        if task_name in self.task_views:
            self.task_views[task_name].add_class("highlight")

    def build_main(self):
        return self.compose_main()

    def compose_main(self):
        left_pane = self.view_pipeline
        # welche plname, sub-pipeline tiefe sind wir?
        # PipelineTUIView mehrfach erzeugen gemäss der level tiefe

        #left.pane.
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
            Button("Next Task", id="btn_next"),
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
        #content_cont = ListView(*listarg)

        content_cont = Static("TV")

        # Layout: ControlPanel oben, darunter StatusLabel, darunter die TaskViews
        main_content = Vertical(control_panel, status_panel, content_cont)
        pipeline_cont = Horizontal(left_pane, main_content)
        layout = Vertical(pipeline_cont, self.view_taskresult)
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
