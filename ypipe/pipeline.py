# pipeline.py
from collections import defaultdict
from rich.console import Console
from rich.tree import Tree
from pydantic import ValidationError
from yaml_config_support import YamlConfigSupport
from framecache_support import FrameIOandCacheSupport
from taskConfig import PipelineModel
from task import Task
from yldpipeNG.storageBroker import StorageBroker
from yldpipeNG.storageCache import StorageCache
import yaml
import networkx as nx
from taskFactory import TaskFactory
from flowpy.utils import setup_logger
from pathlib import Path

from context import Context
#from ypipe.test_ypipe import app_name

logger = setup_logger(__name__, __name__+'.log')


pre = 'yp'

class Pipeline(YamlConfigSupport):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self.tasks = {}
        self.task_defs = {}
        self.dependencies = defaultdict(list)
        self.G = nx.DiGraph()
        self.fc = FrameIOandCacheSupport()
        self.storage_broker = StorageBroker()
        self.storage_cache = StorageCache(self.storage_broker.st_class_factory, rw='s')
        # DEV tmp
        self.app_name = kwargs['app_name']
        self.config_dir = kwargs['config_dir']
        logger.debug('Pipeline init, config_dir: %s', self.config_dir)
        self.master_config_dir = kwargs['master_config_dir']
        self.data_path = kwargs['data_path']
        self.phase = ''
        self.sub = self.app_name
        self.options = kwargs['options'] if 'options' in kwargs else {}
        fnlist = ['kp_si']
        self.app_type = 'tree'
        self.cache_configs(fnlist)

    def additional_yaml_config_logic(self):
        pass

    def init_from_app(self):
        self.master_config_dir = self.app.config_dir.joinpath(pre)
        self.config_dir = self.app.config_dir.joinpath(pre)

    def init_fc_from_app(self):
        # random place to set phase
        self.fc.phase = self.app.phase
        self.fc.phase_subdir = self.app.phase_subdir
        kp_list = self.config_list() + ['profile']
        #print("Pipeline init_fc_from_app, copying config params:", kp_list)
        for attr in kp_list:
            setattr(self.fc, 'cfg_'+attr, getattr(self.app, 'cfg_'+attr))
        # legacy name
        setattr(self.fc, 'cfg_si', getattr(self.app, 'cfg_kp_si'))
        self.fc.init_framecache()
        self.fc.build_fieldlists(self.fc.cfg_kp_process_fields)
        self.fc.prep_writer()

    # Main work XXX
    def load_task_definitions(self):
        print("---------- REGISTER TASKS ------------")
        for t_def in self.config.get('tasks', []):
            self.register_task_def(t_def)


    def register_task_def(self, t_def):
        Task.validate_config(t_def)
        name = t_def['name']
        self.task_defs[name] = t_def
        self.G.add_node(name)
        for dep in t_def.get('req', []):
            self.G.add_edge(dep, t_def['name'])

    # XXX replace with method of YamlConfigSupport?
    def load_config_master(self, filename):
        path = self.master_config_dir.joinpath(filename)
        with open(path) as f:
            #logger.debug('loading config file %s from dir %s', f.name, self.config_dir)
            config = yaml.safe_load(f)
        return config

    def load_pipeline_config(self):
        self.config = self.load_config_master(self.name+'.yml')
        try:
            model = PipelineModel(**self.config)
        except ValidationError as e:
            print(f"Validation error in pieline {self.name}: {e}")
            print(e.errors())

    def create_task(self, t_def, context):
        task = TaskFactory.create_task(t_def, context)
        task.config_dir = self.config_dir  # change later
        task.config = t_def
        return task

    def prepare_context(self):
        context = Context()
        context['fc'] = self.fc
        context['app'] = self.app
        context['storage_broker'] = self.storage_broker
        context['storage_cache'] = self.storage_cache
        context['data_path'] = Path(self.data_path).joinpath('data_in', self.app_name)
        context['config_dir'] = self.config_dir
        context['master_config_dir'] = self.master_config_dir
        return context

    def go_run(self):
        start_task = self.options.get('start_task', None)
        if start_task is not None:
            self.run_from_task(start_task)
        else:
            self.run()

    def run(self):
        context = self.prepare_context()
        for name in nx.topological_sort(self.G):
            print(f"Running task: {name}")
            task = self.create_task(self.task_defs[name], context)
            result = task.run()

            logger.debug(f"Task {name} completed ")
            # wenn task einen frame bereitstellt (provide)  im FrameCache speichern
            """
            if task.config.get('provides', None):
                provides = task.config['provides']
                group = task.config.get('args', {}).get('group', 'default')
                self.fc.store_frame(provides[0], group, result)
                # self.fc.store_frame_group(self.tasks[name].config['provides'][0], result)
                logger.debug(f"Stored frame {task.config['provides']} in FrameCache")
            # task stellt csv file bereit
            """
        self.render_dag()

    def run_from_task(self, start_task_name):
        context = self.prepare_context()
        # Topologische Sortierung aller Tasks
        sorted_tasks = list(nx.topological_sort(self.G))
        # Finde den Index des Start-Tasks
        try:
            start_index = sorted_tasks.index(start_task_name)
        except ValueError:
            raise RuntimeError(f"Task {start_task_name} nicht gefunden!")
        # Nur die Tasks ab dem Start-Task ausführen
        for name in sorted_tasks[start_index:]:
            print(f"Running task: {name}")
            task = self.create_task(self.task_defs[name], context)
            result = task.run()
            logger.debug(f"Task {name} completed ")
        self.render_dag()

    def render_dag(self):
        console = Console()
        tree = Tree("Pipeline DAG")

        try:
            for node in nx.topological_sort(self.G):
                node_tree = tree.add(node)
                for succ in self.G.successors(node):
                    dep = node_tree.add(succ)
                    #node.add(dep)
            console.print(tree)
        except nx.NetworkXUnfeasible:
            raise RuntimeError("Zyklische Abhängigkeit entdeckt!")



    def run_OLD(self):
        print("RUN")
        executed = set()
        while len(executed) < len(self.tasks):
            progress = False
            for name, task in self.tasks.items():
                if name in executed:
                    continue
                deps = self.dependencies[name]
                if all(dep in executed for dep in deps):
                    print(f"Running task: {name}")
                    task.run()
                    executed.add(name)
                    progress = True
            if not progress:
                raise RuntimeError("Zyklische Abhängigkeit entdeckt!")


