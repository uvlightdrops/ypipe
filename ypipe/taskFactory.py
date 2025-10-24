from .resourceTask import *
from .frameResourceTasks import *
from .storageResourceTasks import *
from .readerTask import *
from .task import * # Task

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')

# Module-Liste anpassen
#import tr2FrTask
import ypipe.resourceTask as resourceTask
import ypipe.frameResourceTasks as frameResourceTasks
import ypipe.storageResourceTasks as storageResourceTasks
import ypipe.readerTask as readerTask
import ypipe.task as task

from pathlib import Path
import importlib.util
from typing import Any, cast



def import_task_modules_from_dir(directory):
    """
    Lädt alle *.py-Module aus dem gegebenen Verzeichnis.
    Parameter:
      - directory: pathlib.Path oder string zum Verzeichnis
    Rückgabe:
      - Liste importierter Modulobjekte
    """
    modules = []
    base = Path(directory)

    if not base.exists() or not base.is_dir():
        logger.debug(f"Task modules directory `{base}` not found or not a directory, skipping import.")
        return modules

    for path in sorted(base.glob('*.py')):
        if path.name.startswith('__'):
            continue
        modulename = f"{base.name}.{path.stem}"
        logger.debug(f"Importing module {modulename} from {path}")
        try:
            spec = importlib.util.spec_from_file_location(modulename, str(path))
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                modules.append(mod)
                logger.debug(f"Imported module: {modulename}")
            else:
                logger.warning(f"No spec/loader for {path}, skipping.")
        except Exception as e:
            logger.warning(f"Failed to import {path}: {e}")
    return modules


def get_task_classes(modules):
    import inspect
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


# --- Lazy, threadsafe initialization of the task-class mapping ---
from env import project_dir as PROJECT_DIR

_mapp = None


def _init_mapping():
    """Initialize the module->task-class mapping once, using project_dir for extra modules.
    This avoids performing filesystem imports at module import time. No threading lock used.
    """
    global _mapp
    if _mapp is not None:
        return
    extra_modules = []
    try:
        # Prefer project-level `custom_tasks` if it exists, otherwise fall back to package-local
        custom_dir = Path(PROJECT_DIR) / 'custom_tasks'
        if not custom_dir.exists():
            custom_dir = Path(__file__).resolve().parent / 'custom_tasks'
        extra_modules = import_task_modules_from_dir(custom_dir)
    except Exception as e:
        logger.warning(f"Could not load extra task modules from project_dir: {e}")
        extra_modules = []

    all_modules = [resourceTask, frameResourceTasks, storageResourceTasks, readerTask, task] + extra_modules
    _mapp = get_task_classes(all_modules)
    logger.debug("Task mapping initialized: %s", list(_mapp.keys()))


def _get_mapp():
    if _mapp is None:
        _init_mapping()
    return _mapp



class TaskFactory:

    @staticmethod
    def create_task(t_def, context):
        action = t_def.get('action')
        #logger.debug("Creating task of action type %s", action)

        mapp = _get_mapp()
        if action in mapp:
            klass = mapp[action]
            logger.debug(f"Creating task klass %s", klass)
            # klass is loaded dynamically; cast to Any to satisfy static analyzers
            return cast(Any, klass)(t_def['name'], t_def, context)
        elif t_def.get('type') == 'resource':
            logger.debug('task type = resource, so create BASIC resource task')
            return ResourceTask(t_def['name'], t_def, context)
        else:
            logger.debug(f"Unknown task action/type: {action}/{t_def.get('type')}, using base Task class")
            # raise generic exception to find out quicker during dev
            raise Exception(f"Unknown task action/type: {action}/{t_def.get('type')}")
            # return Task(t_def['name'], t_def, context)
