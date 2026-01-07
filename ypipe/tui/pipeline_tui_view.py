from textual.app import ComposeResult
from textual.widgets import Header, Footer, DataTable
from textual.containers import Container
from textual.coordinate import Coordinate
from textual.widgets.data_table import CellType
from rich.text import Text

from ypipe.iaBase import iaBase

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class PipelineTUIView(Container, iaBase):
    """Textual TUI-View für die Pipeline-Ansicht im ypipe-Framework."""
    def __init__(self, pipeline, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline = pipeline
        self.columns = ['Task Name', 'Status', 'Action']
        self.table = DataTable(id="pipeline_table")
        #self.create_pipeline_table(self.pipeline.plname))

        self.table.can_focus = False
        # Neue Tabelle für Subpipelines
        #self.sub_table = DataTable(id="subpipeline_table")
        #self.sub_table.can_focus = False
        logger.info("PipelineTUIView initialisiert")

    def compose(self) -> ComposeResult:
        """ Erstellt eine pipeline table view"""
        yield self.table

    def create_pipeline_table(self, pl_name):
        """Erstellt eine neue Tabelle für eine Subpipeline."""
        self.table.add_columns(*self.columns)

        for name, t_def in self.pipeline.task_defs.items():
            status = 'loaded'
            out_status = Text(status, style="blue")
            details = t_def.get('action', 'unknown')
            self.table.add_row(name, out_status, details)
        self.set_col_attrs(table, col_widths={'Status': 12, 'Action': 30, 'default': 20})
        logger.info(f"Subpipeline-Tabelle für {pl_name} erstellt.")
        return self.table

    def update_task_status_in_table(self, task_name, status, subpipeline=False):
        # Suche die Zeile mit dem Tasknamen und aktualisiere die Status-Spalte
        table = self.sub_table if subpipeline else self.table
        col_keys = list(table.columns.keys())
        style_map = {
            'loaded': 'blue',
            'started': 'yellow',
            'running': 'yellow',
            'failed': 'red',
            'done': 'green',
        }
        found = False
        for row_key in range(len(table.rows)):
            cell_value = table.get_cell_at(Coordinate(row_key, 0))
            if cell_value == task_name:
                col_key = col_keys[1]
                logger.debug(f'Updating {"Sub-" if subpipeline else ""}row {row_key}, col {col_key} to status {status}')
                if status in style_map:
                    out = Text(status, style=style_map[status])
                else:
                    out = Text(status)
                table.update_cell_at(Coordinate(row_key, 1), out)
                found = True
                break
        if not found:
            logger.warning(f'Taskname nicht in {"Sub-" if subpipeline else ""}Tabelle gefunden: {task_name}')



class PipelineContainer(Container):
    """Container für die Pipeline-Ansicht im ypipe-Framework."""
    def __init__(self, main_pipeline, *args, **kwargs):
        super().__init__(**kwargs)
        # the dict of pipeline views must be ordered, and managed like a stack
        # because we find out at runtime how deep we go into sub-pipelines
        self.pipeline = main_pipeline

        self.pipeline_views = {}
        # Nur initialisieren, nicht mounten!
        self.main_view = PipelineTUIView(self.pipeline)

        self.pipeline_views[self.pipeline.plname] = self.main_view
        # Kein mount im Konstruktor!
        logger.info("PipelineContainer initialisiert.")

    def compose(self) -> ComposeResult:
        logger.debug('Composing PipelineContainer views')
        # Jetzt mounten/yielden!
        yield self.main_view
        # Weitere Subpipeline-Views können hier ebenfalls gemountet werden
        for pl_name, sub_view in self.pipeline_views.items():
            if pl_name != self.pipeline.plname:
                yield sub_view

    def add_sub_table(self, pl_name):
        """Fügt eine neue Subpipeline-Tabelle hinzu."""
        if pl_name not in self.pipeline_views:
            sub_view = PipelineTUIView(self.pipeline)
            self.pipeline_views[pl_name] = sub_view
            # Mount erfolgt jetzt in compose!
            logger.info(f"Subpipeline-Tabelle für {pl_name} hinzugefügt.")
        else:
            logger.warning(f"Subpipeline-Tabelle für {pl_name} existiert bereits.")