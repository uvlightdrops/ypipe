from yaml_config_support.YamlConfigSupport import YamlConfigSupport
from taskConfig import TaskModel
from pydantic import ValidationError
import os
from typing import List
# from ResourceTask import *
#from tr2FrTask import DumpGroups

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class Task(YamlConfigSupport):
    def __init__(self, name: str, config: dict = None, context: 'Context' = None):
        self.name = name
        self.config = config if config is not None else {}
        #self.pipeline = pipeline
        self.context = context
        logger.debug(self.context.keys())

    def __repr__(self):
        return f"Task(name={self.name}"

    """
    @property
    def app(self):
        return self.pl.app
    """

    @property
    def context(self):
        return self._context

    # a setter for pl
    @context.setter
    def context(self, value):
        self._context = value

    @staticmethod
    def validate_config(t_def) -> TaskModel:
        #print(f"Validating config for task {t_def.get('name', '<unnamed>')}")
        try:
            tc = TaskModel(**t_def)
        except ValidationError as e:
            print(f"Validation error in task: {e}")
            print(e.errors())

    # not needed (yet?)
    @staticmethod
    def load_all_from_dir(config_dir: str) -> List['Task']:
        tasks = []
        for fname in os.listdir(config_dir):
            if fname.endswith('.yml') or fname.endswith('.yaml'):
                try:
                    task = Task(os.path.join(config_dir, fname))
                    tasks.append(task)
                except ValidationError as e:
                    print(f"Fehler in {fname}: {e}")
        return tasks

    def run(self):
        """ subclass """
        return None

