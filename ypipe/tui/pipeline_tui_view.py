from textual.app import ComposeResult
from textual.widgets import Header, Footer, DataTable
from textual.containers import Container

class PipelineTUIView(Container):
    """Textual TUI-View für die Pipeline-Ansicht im ypipe-Framework."""
    def __init__(self, pipeline, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pipeline = pipeline


    def compose(self) -> ComposeResult:


        table = DataTable(id="pipeline_table")
        table.add_columns("Task", "Status", "Details")
        for name, t_def in self.pipeline.task_defs.items():
            status = getattr(t_def, 'status', '-')
            details = t_def.get('action', '-')
            table.add_row(name, status, details)
        yield table
