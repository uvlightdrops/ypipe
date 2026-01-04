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
        self.table = DataTable(id="pipeline_table")
        self.table.can_focus = False

    def compose(self) -> ComposeResult:
        self.columns = ["Task", "Status", "Action" ]
        self.table.add_columns(*self.columns)
        self.set_col_attrs(self.table, col_widths={'Status': 12, 'Action': 30, 'default': 20})

        for name, t_def in self.pipeline.task_defs.items():
            status = 'loaded'
            details = t_def.get('action', 'unknown')
            self.table.add_row(name, status, details)
        yield self.table

    def update_task_status_in_table(self, task_name, status):
        # Suche die Zeile mit dem Tasknamen und aktualisiere die Status-Spalte
        col_keys = list(self.table.columns.keys())
        for row_key in range(len(self.table.rows)):
            #logger.debug('row_key=%s', row_key)
            cell_value = self.table.get_cell_at(Coordinate(row_key, 0))
            if cell_value == task_name:
                col_key = col_keys[1]
                logger.debug('Updating row %s, col %s to status %s', row_key, col_key, status)
                if status == 'done':
                    out = Text(status, style="green")
                elif status == 'running':
                    out = Text(status, style="yellow")
                elif status == 'failed':
                    out = Text(status, style="red")
                else:
                    out = Text(status)
                self.table.update_cell_at(Coordinate(row_key, 1), out)
                break
