from textual.app import App
from textual.containers import Horizontal, Vertical
from textual.widgets import ListView, ListItem, Label, Button, Static
from ypipe.baseScreen import BaseScreen
from ypipe.pipeline import Pipeline
from .pipeline_tui_view import PipelineTUIView


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


    def build_main(self):
        return self.compose_main()

    def compose_main(self):
        left_pane = self.view

        self.task_views = self.create_task_views()

        # Control Panel mit Buttons und Statusanzeige
        control_panel = Horizontal(
        #control_panel=Vertical(
            Static("[b]Steuerung[/b]", classes="ControlPanel"),
            Button("Start", id="btn_start"),
            Button("Stop", id="btn_stop"),
            Button("Nächster Task", id="btn_next"),
            Static("Status: [b]Bereit[/b]", id="status_label", classes="ControlPanel"),
            classes="CPanel"
        )

        # Task-Views als ListView
        listarg = []
        for name, task_view in self.task_views.items():
            listarg.append(ListItem(Label(name)))
            listarg.append(ListItem(task_view))
        content_cont = ListView(*listarg)

        # Layout: ControlPanel oben, darunter die TaskViews
        main_content = Vertical(control_panel, content_cont)
        layout = Horizontal(left_pane, main_content)
        return layout

    def create_task_views(self):
        from ypipe.tui.task_tui_view import TaskView
        task_views = {}
        logger.debug(self.pipeline.task_defs)
        for name, task_def in self.pipeline.task_defs.items():
            #logger.debug("task_def: %s", task_def)
            task_view = TaskView(task_def)
            task_views[name] = task_view
        return task_views
