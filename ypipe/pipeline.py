# pipeline.py
from collections import defaultdict
from rich.console import Console
from rich.tree import Tree
from pathlib import Path
import yaml
import networkx as nx
from jinja2 import Template

#import sys
#print(sys.path)
from yaml_config_support.yamlConfigSupport import YamlConfigSupport
from framecache_support.frameIOandCacheSupport import FrameIOandCacheSupport
from yldpipeNG.storageBroker import StorageBroker
from yldpipeNG.storageCache import StorageCache
from flowpy.utils import setup_logger
from .task import Task
from .taskFactory import TaskFactory
from .context import Context
#from ypipe.test_ypipe import app_name
from .utils import log_context

logger = setup_logger(__name__, __name__+'.log')
print("Logger for pipeline set up.", logger)

DEBUG=True
pre = 'yp'


def get_cfg_context(app):
    # Return the prepared config dict from app
    return app.config_d

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

class KpctrlBusinessLogic:
    # plugin method for YamlConfigSupport
    def additional_yaml_config_logic(self):
        # groups with own wanted_logic cfg file
        yml_list = self.config_dir.glob('group_logic_*.yml')
        done = []
        for fn in yml_list:
            group_case_name = fn.stem[12:]
            #logger.debug('group_case_name: %s', group_case_name)
            groupname = group_case_name
            done.append(group_case_name)
            # XXX BUG , same key is overridden. rewrite case handling
            # Parse your YAML into a dictionary, then validate against your model.
            with open(fn) as f:
                yml = yaml.load(f, Loader=yaml.FullLoader)
            #logger.debug('groupname: %s', groupname)
            self.cfg_kp_wanted_logic['groups'][groupname] = yml
        # other groups, with simple copyall logic
        simple_list = self.cfg_kp_logic_ctrl_groups.get('loop_copyall', []) # + othres XXX
        # XXX maybe own loop for rec
        simple_list+= self.cfg_kp_logic_ctrl_groups.get('loop_copyall_rec', [])
        for fn in simple_list:
            gl = { 'group_name': {'old': fn, 'new': fn} }
            self.cfg_kp_wanted_logic[fn] = gl
        # logger.debug('HACKED %s', str(done))
            yaml_str = yaml.dump(self.cfg_kp_wanted_logic[fn], default_flow_style=False)
            logger.debug(yaml_str)


class Pipeline(YamlConfigSupport, KpctrlBusinessLogic):
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        logger.debug('kwargs: %s', kwargs)
        self.tasks = {}
        self.task_defs = {}
        self.dependencies = defaultdict(list)

        # sub components
        self.G = nx.DiGraph()
        self.fc = FrameIOandCacheSupport()
        self.storage_broker = StorageBroker()
        self.storage_cache = StorageCache(self.storage_broker.st_class_factory, rws='s')

        # DEV tmp
        # kwargs assignments
        self.app_name = kwargs.get('app_name', 'stubapp')
        self.project_dir = kwargs.get('project_dir', Path.cwd())
        self.master_config_dir = kwargs.get('master_config_dir', Path.cwd().joinpath('data_master'))
        self.config_dir = self.master_config_dir.joinpath(self.app_name)
        self.data_path = kwargs.get('data_path')
        self.plname = kwargs.get('plname', 'yp_default')
        logger.debug(f"Pipeline init: app_name={self.app_name}, pl_name={self.plname}, data_path={self.data_path}")
        self.sub = self.app_name
        self.phase = ''
        self.options = kwargs['options'] if 'options' in kwargs else {}

        # XXX DEV
        fnlist = self.load_config('fnlist.yml').get('fnlist')
        #logger.debug('fnlist: %s', fnlist)
        #fnlist = ['kp_si', 'kp_wanted_logic']
        self.app_type = 'tree'
        # YamlConfigSupport
        #self.cfg_kp_si = self.load_config('kp_si.yml')

        self.use_legacy_app = kwargs.get('use_legacy_app')
        logger.debug(f"Pipeline use_legacy_app: {self.use_legacy_app}")
        if self.use_legacy_app == False:
            # so we use out own config loading from YamlConfigSupport
            self.cache_configs(fnlist)
            self.init_config_profile()

        if not self.config_dir.joinpath(self.plname + '.yml').exists():
            raise RuntimeError(f"Pipeline init: config file {self.plname + '.yml'} not found in {self.config_dir}!")

        self.config = self.load_config(self.plname + '.yml')


    # XXX ok to have the KpctrlBusinessLogic class and include the method from there?
    #def additional_yaml_config_logic(self):
    #    pass

    def init_fc(self):
        if self.use_legacy_app:
            self.fc.phase = self.app.phase
            self.fc.phase_subdir = self.app.phase_subdir
        elif self.use_legacy_app == False:
            self.fc.phase = 'p1'
            self.fc.phase_subdir = 'p1'
        else:
            raise RuntimeError("Pipeline init_fc: use_legacy_app not set!")

        kp_list = self.config_list() + ['profile']
        #logger.debug('kp_list: %s', kp_list)
        for attr in kp_list:
            setattr(self.fc, 'cfg_'+attr, getattr(self, 'cfg_'+attr))
        # legacy name
        setattr(self.fc, 'cfg_si', getattr(self, 'cfg_kp_si'))

        self.fc.init_framecache() # ex
        self.fc.init_fc_bytype()
        self.fc.build_fieldlists(self.fc.cfg_kp_process_fields)


    # Main work XXX
    def load_task_definitions(self):
        print("---------- REGISTER TASKS ------------")
        dummy_ctx = self.prepare_context()
        for t_def in self.config.get('tasks', []):
            # Build minimal template context: start with config_d, then add a few
            # scalar values from the runtime context so templates can use
            # {{data_in_path}}, {{data_out_path}}, {{app_name}}, etc.
            templ_d = dict(self.config_d) if isinstance(self.config_d, dict) else {}
            for key in ('data_path', 'data_in_path', 'data_out_path', 'project_dir', 'config_dir', 'master_config_dir', 'app_name'):
                if key in dummy_ctx:
                    templ_d[key] = dummy_ctx[key]

            t_def_rendered = render_template(t_def, templ_d)
            yd = yaml.dump(t_def_rendered)
            #logger.debug(yd)
            self.register_task_def(t_def_rendered)


    def register_task_def(self, t_def):
        Task.validate_config(t_def)
        #logger.debug('Registering task: %s', t_def['name'])
        # XXX click.secho later in two colors split by underscore
        name = t_def['name']
        self.task_defs[name] = t_def
        self.G.add_node(name)
        for dep in t_def.get('req_tasks', []):
            self.G.add_edge(dep, t_def['name'])


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
        context['data_path'] = self.data_path
        context['data_in_path'] = Path(self.data_path).joinpath('data_in', self.app_name)
        context['data_out_path'] = Path(self.data_path).joinpath('data_out', self.app_name)
        context['project_dir'] = self.project_dir
        context['config_dir'] = self.config_dir
        context['master_config_dir'] = self.master_config_dir
        context['config_d'] = self.config_d
        # expose app_name for tasks
        context['app_name'] = self.app_name

        return context

    """
    def go_run(self):
        start_task = self.options.get('start_task', None)
        if start_task is not None:
            self.run_from_task(start_task)
        else:
            self.run()
    """


    # Run a specific task by name, including its requirements
    # context is prepared here if not given by caller (eg for run_all)
    def run_task_by_name(self, task_name):
        context = self.prepare_context()
        #self.walk_dependencies(task_name, context)
        self.walk_resource_dependencies(task_name, context)

    def walk_dependencies(self, task_name, context):
        task_def = self.task_defs[task_name]

        # Run requirements # XXX if needed?
        req_tasks = task_def.get('req_tasks', [])
        if not req_tasks:
            logger.debug(f"Task {task_name} has no requirements, running directly")
            self._run_task(task_name, context)
            return
        else:
            for dep_name in req_tasks:
                self.walk_dependencies(dep_name, context)

    def walk_resource_dependencies(self, task_name, context, stack=None, done=None):
        """
        Rekursive Abarbeitung von resourcen-basierten Abhängigkeiten.
        - 'stack' ist die aktuelle Rekursionskette (Liste) zur Zykluserkennung
        - 'done' ist ein Set der bereits ausgeführten Tasks (verhindert Doppel-Run)
        """
        # Initialisiere Tracking-Objekte
        if stack is None:
            stack = []
        if done is None:
            done = set()

        # Wenn Task bereits fertig, nichts zu tun
        if task_name in done:
            logger.debug(f"Task {task_name} already handled; skipping")
            return

        # Zyklus-Detektion: wenn Task bereits im aktuellen Stack, haben wir eine Schleife
        if task_name in stack:
            cycle_path = ' -> '.join(stack + [task_name])
            raise RuntimeError(f"Resource dependency cycle detected: {cycle_path}")

        # Markiere Task im aktuellen Stack
        stack.append(task_name)

        # Hole die Definition (wird KeyError werfen, falls nicht vorhanden)
        task_def = self.task_defs[task_name]

        req_resources = task_def.get('req_resources', [])
        if not req_resources:
            logger.debug(f"Task {task_name} no res.req, run now and return")
            # Führe Task aus, markiere als done
            self._run_task(task_name, context)
            done.add(task_name)
            stack.pop()
            return

        # Für jede benötigte Resource: finde Provider und verarbeite deren Abhängigkeiten zuerst
        for res_name in req_resources:
            logger.debug(f"Task {task_name} requires resource {res_name}, checking providers")
            provider_tasks = [
                t_name for t_name, t_def in self.task_defs.items()
                if t_name != task_name and res_name in t_def.get('provides', [])
            ]
            if not provider_tasks:
                # Keine Provider gefunden -> Fehler
                raise RuntimeError(f"No task provides required resource '{res_name}' for task '{task_name}'")

            for prov_task in provider_tasks:
                if prov_task in done:
                    logger.debug(f"Provider {prov_task} already done; skipping")
                    continue
                logger.debug(f"check provtask %s for its deps", prov_task)
                # Rekursiver Aufruf mit geteiltem 'stack' und 'done'
                self.walk_resource_dependencies(prov_task, context, stack, done)

        # Nachdem alle Provider gelaufen sind, noch einmal sicherstellen, dass die Task selbst nur einmal läuft
        if task_name not in done:
            self._run_task(task_name, context)
            done.add(task_name)

        # Entferne Task aus aktuellem Stack beim Zurückkehren
        stack.pop()

    def run_all(self):
        print("RUN ORDER:")
        for name in nx.topological_sort(self.G):
            print(name)
        print("-----------------")
        #self.render_dag()

        context = self.prepare_context()

        for name in nx.topological_sort(self.G):
            self._run_task(name, context)
            #self.run_task_by_name(name) #, context=context)


    def _run_task(self, name, context):

        task_def = self.task_defs[name]

        logger.debug('---------------- NEXT task %s, action: %s', name, task_def['action'])
        log_context(context, 'Before run')

        task = self.create_task(task_def, context)
        run_flag = self.task_defs[name].get('run')
        skip_task = False
        #logger.debug(f"Task {name} run flag: {run_flag}")
        #logger.debug(type(run_flag))

        if run_flag == False:
            logger.debug(f"Skipping task {name} as run flag is False")
            print(f"Skipping task: {name}")
            return

        loop_items = self.task_defs[name].get('loop_items', None)
        #logger.debug(f"Task {name} loop_items: {loop_items}")

        requires = task_def.get('req', [])
        #logger.debug('Task %s requires: %s', name, requires)
        for req in requires:
            if req not in context:
                logger.error('Task %s requires %s but not in context!', name, req)
                if DEBUG:
                    logger.debug('Task %s skipping', name)
                    skip_task = True
                    break
                raise RuntimeError(f"Task {name} requires '{req}' but it does not exist in context!")
            else:
                logger.debug('Task %s got required %s from context', name, req)

        if skip_task:
            return

        print(f"Running task: {name}")
        logger.debug(f"Start {task.__class__}")
        logger.debug(f"loop_items: {loop_items}")
        if loop_items:
            task.run_with_loop()
        else:
            task.run()

        #assert result is None, f"result SHOULD be None (context store) - res.type is: {type(result)} "
        #logger.debug(f"Task {name} completed ")
            # wenn task einen frame bereitstellt (provide)  im FrameCache speichern
        log_context(context, 'Task done')

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
                    #node_tree.add(dep)
            console.print(tree)
        except nx.NetworkXUnfeasible:
            raise RuntimeError("Zyklische Abhängigkeit entdeckt!")
