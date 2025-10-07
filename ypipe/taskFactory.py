from tt import *
#from resourceTask import ResourceTask, FrameResourceTask, DebugFrameTask
#from resourceTask import TransformTask, StorageResourceTask
#from resourceTask import StoreFrameResourceTask, ReadFrameResourceTask
#from tr2FrTask import DumpGroups
#from resourceTask import WriteFrameResourceTask
import inspect
from tr2FrTask import *
from resourceTask import *
from readerTask import *
from task import * # Task

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')

# Module-Liste anpassen
import tr2FrTask
import resourceTask
import readerTask
import task


def get_task_classes(modules):
    task_classes = {}
    for module in modules:
        logger.debug("Inspecting module %s", module)
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj) and name.endswith('Task') and name != 'Task':
                tmp = name[:-4]
                # logger.debug("Found task class: %s", obj)
                key = tmp[0].lower() + tmp[1:]  # Entfernt 'Task' und wandelt in Kleinbuchstaben um
                task_classes[key] = obj
    return task_classes

mapp = get_task_classes([tr2FrTask, resourceTask, readerTask, task])
logger.debug(mapp.keys())

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

