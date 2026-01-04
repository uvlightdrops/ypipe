from textual.widget import Widget
from textual.widgets import Static
import yaml

class TaskView(Static):
    def __init__(self, task_def):
        super().__init__()
        self.task_def = task_def
        self.update_content()

    def update_content(self):
        # Nur ausgewählte Keys anzeigen
        keys = ["name", "action", "args", "provides", "req_resources"]
        filtered = {k: self.task_def.get(k) for k in keys if k in self.task_def}
        pretty = yaml.dump(filtered, allow_unicode=True, sort_keys=False, indent=2)
        self.update(pretty)
