from .tt import *
import inspect
#from .tr2FrTask import *
from .resourceTask import *
from .readerTask import *
from .task import * # Task

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')

# Module-Liste anpassen
#import tr2FrTask
import ypipe.resourceTask as resourceTask
import ypipe.readerTask as readerTask
import ypipe.task as task

import os
import importlib
import inspect



def import_task_modules_from_dir(directory):
    modules = []
    for filename in os.listdir(directory):
        if filename.endswith('.py') and not filename.startswith('__'):
            modulename = filename[:-3]
            logger.debug(f"Importing module {modulename} from {directory}")
            modul = importlib.import_module(f"{directory}.{modulename}")
            logger.debug(f"Imported module: {modul}")
            modules.append(modul)
    return modules


def get_task_classes(modules):
    task_classes = {}
    for module in modules:
        #logger.debug("Inspecting module %s", module)
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj) and name.endswith('Task') and name != 'Task':
                tmp = name[:-4]
                # logger.debug("Found task class: %s", obj)
                key = tmp[0].lower() + tmp[1:]  # Entfernt 'Task' und wandelt in Kleinbuchstaben um
                task_classes[key] = obj
    return task_classes


# Beispiel: tasks aus dem Unterverzeichnis 'extra_tasks' laden
extra_modules = import_task_modules_from_dir('custom_tasks')
all_modules = [resourceTask, readerTask, task] + extra_modules

mapp = get_task_classes(all_modules)
logger.debug(mapp.keys())
#for k, v in mapp.items():
#    logger.debug(f"'{k}' => {v}")

class TaskFactory:

    @staticmethod
    def create_task(t_def, context):
        action = t_def.get('action')
        #logger.debug("Creating task of action type %s", action)

        if action in mapp:
            klass = mapp[action]
            logger.debug(f"Creating task klass %s", klass)
            return klass(t_def['name'], t_def, context)
        elif t_def.get('type') == 'resource':
            logger.debug('task type = resource, so create BASIC resource task')
            return ResourceTask(t_def['name'], t_def, context)
        else:
            logger.debug(f"Unknown task action/type: {action}/{t_def.get('type')}, using base Task class")
            return Task(t_def['name'], t_def, context)

