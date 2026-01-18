"""
PipelineOutputHandler: Observer für Pipeline-Ausgaben (Status, Log, etc.)
"""

class PipelineOutputHandler:
    """Observer für Pipeline-Ausgaben (Status, Log, etc.).
    Kann für CLI, TUI oder andere Oberflächen verwendet werden.
    """
    def __init__(self, on_status=None, on_log=None, on_task_status=None, on_pipeline_status=None):
        self.on_status = on_status
        self.on_log = on_log
        self.on_task_status = on_task_status
        self.on_pipeline_status = on_pipeline_status

    def notify_status(self, status):
        if self.on_status:
            self.on_status(status)

    def notify_log(self, message):
        if self.on_log:
            self.on_log(message)

    def notify_task_status(self, task_name, status):
        if self.on_task_status:
            self.on_task_status(task_name, status)

    def notify_pipeline_status(self, pl_name, status):
        if self.on_pipeline_status:
            self.on_pipeline_status(pl_name, status)
