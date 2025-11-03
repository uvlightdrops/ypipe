from pydantic import ValidationError
import os, sys
from typing import List
from yaml_config_support.yamlConfigSupport import YamlConfigSupport

from .taskConfig import TaskModel, MtaskModel
# from ResourceTask import *
#from tr2FrTask import DumpGroups

from flowpy.utils import setup_logger

from ypipe.loopMixin import LoopMixin

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

class EchoTask(LoopMixin, Task):
    def run(self):
        value = self.item
        print("ECHO: ", value)


class FileTask(Task):
    pass

class CopyFileTask(FileTask):
    def run(self):
        import shutil
        from pathlib import Path

        src = self.args.get('src')
        dst = self.args.get('dst')
        if not src or not dst:
            logger.error("CopyFileTask needs src and dst args")
            return
        """
        # Resolve data_path and app_name from context (attr or dict)
        ctx = self.context
        data_path = None
        data_path = self.context.get('data_path')
        logger.debug(f"CopyFileTask using data_path: {data_path}, join {src} -> {dst}")
        # cast to path object if not yet one
        if not isinstance(data_path, Path):
            data_path = Path(data_path)

        app_name = self.context.get('app_name')
        srcpath = data_path.joinpath(src)
        dstpath = data_path.joinpath(dst)
        """
        srcpath = Path(src)
        dstpath = Path(dst)

        try:
            if not srcpath.exists():
                logger.error("Source does not exist: %s", srcpath)
                return

            # Ensure destination directory exists
            dstpath.parent.mkdir(parents=True, exist_ok=True)

            # Atomic copy: copy to a temp file in the destination dir, then replace
            tmp_name = dstpath.name + f'.{os.getpid()}.tmp'
            tmp_path = dstpath.parent.joinpath(tmp_name)
            shutil.copy2(srcpath, tmp_path)
            os.replace(str(tmp_path), str(dstpath))

            logger.info(f"Copied file from {srcpath} to {dstpath}")
        except Exception as e:
            logger.exception("Failed to copy file: %s", e)



