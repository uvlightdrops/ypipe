from textual.app import ComposeResult
from textual.widgets import Header, Footer, DataTable, Label
from textual.containers import Container
from textual.coordinate import Coordinate
from textual.widgets.data_table import CellType
from rich.text import Text

from ypipe.iaBase import iaBase

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class TaskResultView(Container, iaBase):
    """Textual TUI-View für die Pipeline-Ansicht im ypipe-Framework."""
    def __init__(self, pipeline, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline = pipeline
        self.table = DataTable(id="pipeline_table")
        self.label = Label()
        self.message = Label("No results to show yet.", id="task_result_message")

    def compose(self) -> ComposeResult:
        yield self.label
        yield self.message
        yield self.table

    def update(self, task_name=None):
        self.update_table(task_name=task_name)
        self.label.update(f"Results for task: {task_name}")

    def update_table(self, task_name=None):
        self.table.clear()
        task_def = self.pipeline.task_defs.get(task_name, {}) if task_name else {}

        if 'provides' not in task_def:
            self.message.update(f"Task does not provide any results, cannot show results.")
            logger.warning("TaskResultView: task does not provide any results, cannot show results.", task_name)
            return

        prov_main = task_def['provides']['main']
        if prov_main['type'] != 'frame_group':
            self.message.update(f"Task does not provide a frame_group, cannot show FrGroup results.")
            logger.warning("TaskResultView: task does not provide a frame_group, cannot show results.", task_name)
            return

        #framegroup_out = task_def['out'][0]
        framegroup_out = prov_main['key']

        result_df = self.pipeline.fc.get_frame_group(framegroup_out)
        self.columns = result_df.columns
        self.table.add_columns(*self.columns)
        self.set_col_attrs(self.table, col_widths={})
        for idx, row in result_df.iterrows():
            row_values = [str(row[col]) for col in self.columns]
            self.table.add_row(*row_values)
        self.message.update(f"Showing results for frame_group: {framegroup_out} with {len(result_df)} rows.")
        logger.info("TaskResultView: showing results for task %s, frame_group %s with %d rows.", task_name, framegroup_out, len(result_df))
        
