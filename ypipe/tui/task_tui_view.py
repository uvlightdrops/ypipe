from textual.widget import Widget
from textual.widgets import Static
import yaml

class TaskView(Static):
    def __init__(self, task_def):
        super().__init__()
        self.task_def = task_def
        self.update_content()

    def update_content(self):
        # YAML-Darstellung für das gesamte task_def-Dictionary
        pretty = yaml.dump(self.task_def, allow_unicode=True, sort_keys=False, indent=2)
        self.update(pretty)
