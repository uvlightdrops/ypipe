# pipeline.py
from collections import defaultdict
from rich.console import Console
from rich.tree import Tree
from pydantic import ValidationError
from pathlib import Path
import yaml
import networkx as nx
from jinja2 import Template

#import sys
#print(sys.path)
from yaml_config_support import YamlConfigSupport
from framecache_support import FrameIOandCacheSupport
from yldpipeNG.storageBroker import StorageBroker
from yldpipeNG.storageCache import StorageCache
from flowpy.utils import setup_logger
from .task import Task
from .taskConfig import PipelineModel
from .taskFactory import TaskFactory
from .context import Context
#from ypipe.test_ypipe import app_name


logger = setup_logger(__name__, __name__+'.log')

DEBUG=True
pre = 'yp'

def get_cfg_context(app):
    return app.config_d
    return {k[4:]: v for k, v in app.__dict__.items() if k.startswith('cfg_')}

def render_template(obj, context):
    if isinstance(obj, str):
        rendered = Template(obj).render(context)
        try:
            parsed = yaml.safe_load(rendered)
            return parsed
        except yaml.YAMLError:
            return rendered
        #return Template(obj).render(context)

    elif isinstance(obj, dict):
        return {k: render_template(v, context) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [render_template(item, context) for item in obj]
    else:
        return obj


class Pipeline(YamlConfigSupport):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self.tasks = {}
        self.task_defs = {}
        self.dependencies = defaultdict(list)

        # sub components
        self.G = nx.DiGraph()
        self.fc = FrameIOandCacheSupport()
        self.storage_broker = StorageBroker()
        self.storage_cache = StorageCache(self.storage_broker.st_class_factory, rw='s')

        # DEV tmp
        # kwargs assignments
        self.app_name = kwargs['app_name']
        #self.project_dir = kwargs['project_dir']
        self.config_dir = kwargs['config_dir']
        self.master_config_dir = kwargs['master_config_dir']
        self.data_path = kwargs['data_path']
        self.sub = self.app_name
        self.phase = ''
        self.options = kwargs['options'] if 'options' in kwargs else {}

        # XXX DEV
        fnlist = self.load_config('fnlist.yml').get('fnlist')
        logger.debug('fnlist: %s', fnlist)
        #fnlist = ['kp_si', 'kp_wanted_logic']
        self.app_type = 'tree'
        # YamlConfigSupport
        #self.cfg_kp_si = self.load_config('kp_si.yml')
        self.use_legacy_app = kwargs.get('use_legacy_app')
        logger.debug(f"Pipeline use_legacy_app: {self.use_legacy_app}")
        if self.use_legacy_app == False:
            self.cache_configs(fnlist)
            self.init_config_profile()

    # XXX remove when YamlConfigSupport is cleaned up
    def additional_yaml_config_logic(self):
        pass

    def init_cfg_from_app(self):
        # random place to set phase
        #self.phase = self.app.phase
        #self.phase_subdir = self.app.phase_subdir
        kp_list = self.config_list() + ['profile']
        logger.debug('kp_list: %s', kp_list)
        for attr in kp_list:
            setattr(self, 'cfg_'+attr, getattr(self.app, 'cfg_'+attr))
        # legacy name
        setattr(self, 'cfg_si', getattr(self.app, 'cfg_kp_si'))

    def init_fc(self):
        if self.use_legacy_app:
            #logger.debug("Pipeline init_fc using legacy app")
            self.init_fc_from_app()
        elif self.use_legacy_app == False:
            #logger.debug("Pipeline init_fc without app")
            self.init_fc_wo_app()
        else:
            raise RuntimeError("Pipeline init_fc: use_legacy_app not set!")

        kp_list = self.config_list() + ['profile']
        logger.debug('kp_list: %s', kp_list)
        for attr in kp_list:
            setattr(self.fc, 'cfg_'+attr, getattr(self, 'cfg_'+attr))
        # legacy name
        setattr(self.fc, 'cfg_si', getattr(self, 'cfg_kp_si'))

        self.fc.init_framecache() # ex
        self.fc.init_fc_bytype()
        self.fc.build_fieldlists(self.fc.cfg_kp_process_fields)

    def init_fc_wo_app(self):
        # XXX dev
        self.fc.phase = 'p1'
        self.fc.phase_subdir = 'p1'

    def init_fc_from_app(self):
        # random place to set phase
        self.fc.phase = self.app.phase
        self.fc.phase_subdir = self.app.phase_subdir


    # Main work XXX
    def load_task_definitions(self):
        print("---------- REGISTER TASKS ------------")
        for t_def in self.config.get('tasks', []):
            cfg_context = self.config_d
            #cfg_context = get_cfg_context(self.app)
            t_def_rendered = render_template(t_def, cfg_context)
            logger.debug(t_def_rendered)
            self.register_task_def(t_def_rendered)


    def register_task_def(self, t_def):
        Task.validate_config(t_def)
        #logger.debug('Registering task: %s', t_def['name'])
        name = t_def['name']
        self.task_defs[name] = t_def
        self.G.add_node(name)
        for dep in t_def.get('req', []):
            self.G.add_edge(dep, t_def['name'])

    # XXX replace with method of YamlConfigSupport?
    """
    def load_config_master(self, filename):
        path = self.master_config_dir.joinpath(filename)
        with open(path) as f:
            #logger.debug('loading config file %s from dir %s', f.name, self.config_dir)
            config = yaml.safe_load(f)
        return config
    """

    def load_pipeline_config(self):
        self.config = self.load_config(self.name+'.yml')
        #self.config = self.load_config_master(self.name+'.yml')
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
        context['result'] = None
        context['fc'] = self.fc
        if self.use_legacy_app:
            context['app'] = self.app
        context['storage_broker'] = self.storage_broker
        context['storage_cache'] = self.storage_cache
        context['data_path'] = Path(self.data_path).joinpath('data_in', self.app_name)
        context['project_dir'] = self.project_dir
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
        #self.render_dag()
        context = self.prepare_context()
        for name in nx.topological_sort(self.G):
            task_def = self.task_defs[name]
            task = self.create_task(task_def, context)
            logger.debug('---------------- NEXT task_defs:')
            logger.debug('task %s, action: %s', name, task_def['action'])
            run_flag = self.task_defs[name].get('run')
            skip_task = False
            #logger.debug(f"Task {name} run flag: {run_flag}")
            #logger.debug(type(run_flag))

            if run_flag == False:
                logger.debug(f"Skipping task {name} as run flag is False")
                print(f"Skipping task: {name}")
                continue


            loop_items = self.task_defs[name].get('loop_items', None)
            # logger.debug(f"Task {name} loop_items: {loop_items}")

            requires = task_def.get('req', [])
            if requires:
                logger.debug('Task %s requires: %s', name, requires)
                for r in requires:
                    if r not in context:
                        logger.error('Task %s requires %s but not in context!', name, r)
                        if DEBUG:
                            logger.debug('Task %s skipping', name)
                            #print(f"Skipping task {name} due to missing requirement: {r}")
                            skip_task = True
                            break
                        raise RuntimeError(f"Task {name} requires '{r}' but it does not exist in context!")
                    else:
                        logger.debug('Task %s got required %s from context', name, r)

            if skip_task:
                continue

            print(f"Running task: {name}")

            if loop_items:
                result = task.run_with_loop()
            else:
                result = task.run()
            assert result is None, f"result SHOULD be None (context store) - res.type is: {type(result)} "
            #logger.debug(f"Task {name} completed ")
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


