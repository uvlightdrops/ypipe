from pydantic import ValidationError
import os, sys
from typing import List
from yaml_config_support.yamlConfigSupport import YamlConfigSupport

from .taskConfig import TaskModel, MtaskModel
# from ResourceTask import *
#from tr2FrTask import DumpGroups

from flowpy.utils import setup_logger

from ypipe.loopMixin import LoopMixin
from ypipe.log_utils import log_context

logger = setup_logger(__name__, __name__+'.log')

# XXX why inherit from YamlConfigSupport?
# overhead !
class Task: #(YamlConfigSupport):
    def __init__(self, name: str, config: dict = None, context: 'Context' = None):
        self.name = name
        self.config = config if config is not None else {}
        #self.pipeline = pipeline
        self.context = context
        #logger.debug(self.context.keys())
        # set item to None for non-looping tasks
        self.item = None

        # XXX args from yaml not confuse with function args
        self.args = self.config.get('args', {})
        self.req_resources = self.config.get('req_resources', [])
        self.provides = self.config.get('provides', [])

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
        #logger.debug(f"Validating config for task {t_def.get('name', '<unnamed>')}")
        try:
            tc = TaskModel(**t_def)
        except ValidationError as e:
            #print(e.errors())
            logger.debug('t_def %s has ValidationError: %s', t_def['name'], e)
            logger.debug('So Now we try validating with MtaskModel')
            tc = MtaskModel(**t_def)
        return tc

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

    def prepare(self):
        """ subclass """
        return None

    def run(self):
        """ subclass """
        return None

    def stats_init(self):
        pass
    def stats_report(self, name):
        pass


class StopTask(Task):
    def run(self):
        logger.info(f"StopTask {self.name} reached, stopping pipeline")
        #raise Exception("StopTask reached, stopping pipeline")
        sys.exit()

class NoopTask(LoopMixin, Task):
    def run(self):
        logger.info(f"NoopTask {self.name} doing nothing")

class EchoTask(LoopMixin, Task):
    def run(self):
        self.prepare()

        value = self.args.get('value', self.item)
        print("ECHO: ", value)


class DebugContextTask(Task):
    def run(self):
        log_context(self.context, f"DebugContextTask {self.name}")
        logger.debug("DebugContextTask context keys: %s", self.context.keys())
        #for k, v in self.context.items():
        #    logger.debug("  %s: %s", k, v)


class DebugContextVarTask(Task):
    def run(self):
        # lookup ctx_key in context and log its value
        ctx_key = self.args.get('ctx_key')
        if ctx_key in self.context:
            logger.debug("DebugContextVarTask context[%s] = %s", ctx_key, self.context[ctx_key])
        else:
            logger.warning("DebugContextVarTask context has no key %s", ctx_key)



class SetContextTask(Task):
    def run(self):
        items = self.args.get('items', {})
        for k, v in items.items():
            if k in self.context:
                logger.debug("SetContextTask overwriting context[%s]: %s -> %s", k, self.context[k], v)
            self.context[k] = v
            logger.debug("SetContextTask set context[%s] = %s", k, v)


class SetFactTask(Task):
    def run(self):

        facts = self.args
        for k, v in facts.items():
            logger.debug("SetFactTask set fact %s = %s", k, v)
            if k not in self.context:
                logger.debug("SetFactTask adding new fact %s to context", k)
                self.context[k] = v
            else:
                logger.warning("SetFactTask overwriting fact %s in context: %s -> %s", k, self.context[k], v)
                self.context[k] = v
