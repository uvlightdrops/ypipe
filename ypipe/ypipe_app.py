"""
YpipeApp: Zentrale Backend-Klasse für ypipe
Verwaltet Pipelines, Konfiguration und Status
"""
import yaml

from ypipe.pipeline import Pipeline
from ypipe.pipeline_output_handler import PipelineOutputHandler
from framecache_support.frameIOandCacheSupport import FrameIOandCacheSupport
from yaml_config_support.yamlConfigSupport import YamlConfigSupport

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__ + '.log')


class KpctrlBusinessLogic:
    # plugin method for YamlConfigSupport
    def additional_yaml_config_logic(self):
        # Only apply additional logic for apps that use 'tree' storage
        if getattr(self, 'app_type', None) != 'tree':
            logger.debug('Skipping additional_yaml_config_logic because app_type != tree')
            return
        # groups with own wanted_logic cfg file
        yml_list = self.config_dir.glob('groups/group_logic_*.yml')
        done = []
        for fn in yml_list:
            group_case_name = fn.stem[12:]
            groupname = group_case_name
            done.append(group_case_name)
            with open(fn) as f:
                yml = yaml.load(f, Loader=yaml.FullLoader)
            self.cfg_kp_wanted_logic['groups'][groupname] = yml
        simple_list = self.cfg_kp_logic_ctrl_groups.get('loop_copyall', [])
        simple_list+= self.cfg_kp_logic_ctrl_groups.get('loop_copyall_rec', [])
        for fn in simple_list:
            gl = { 'group_name': {'old': fn, 'new': fn} }
            self.cfg_kp_wanted_logic[fn] = gl
            yaml_str = yaml.dump(self.cfg_kp_wanted_logic[fn], default_flow_style=False)
            logger.debug(yaml_str)


class YpipeApp(KpctrlBusinessLogic, YamlConfigSupport):
    def __init__(self, app_name=None, output_handler=None, **kwargs):
        self.app_name = app_name
        self.pipelines = {}  # plname -> Pipeline
        self.active_pipeline = None
        self.status = "idle"

        kws = ['repo', 'data_path', 'master_config_dir', 'project_dir', 'plname']
        logger.debug('kwargs: %s', kwargs)
        for key in kws:
            logger.debug(f"Setze Attribut {key} auf {kwargs[key]}")
            setattr(self, key, kwargs[key])

        self.config_dir = self.master_config_dir.joinpath(self.app_name)

        # Zentrale Default-Attribute als dict
        self.defaults = {
            'phase': '',
            'sub': self.app_name,
            'app_type': 'tree',
            'options': {},
            'forwarded_resources': [],
        }
        # Werte aus kwargs übernehmen, sonst Default
        for key, default in self.defaults.items():
            setattr(self, key, kwargs.get(key, default))

        # --- Zentrale Attribute, die vorher in Pipeline waren ---
        self.config = None
        self.style = ""
        # --------------------------------------------------------

        fnlist = self.load_config('fnlist.yml').get('fnlist')
        self.cache_configs(fnlist)
        self.init_config_profile()

        self.fc = FrameIOandCacheSupport()
        self.init_fc()

        # config_d initialisieren: Dictionary mit allen geladenen Konfigurationsdaten aus fnlist
        self.config_d = {}
        for key in fnlist:
            attr_name = f'cfg_{key}'
            self.config_d[key] = getattr(self, attr_name, None)
        # Beispielhafte weitere keys wie oben explizit:
        for key in ['profile', 'kp_logic_ctrl_groups', 'kp_wanted_logic']:
            if key not in self.config_d:
                self.config_d[key] = getattr(self, f'cfg_{key}', None)

        self.output_handler = output_handler or PipelineOutputHandler()

    def load_pipeline(self, plname, pipeline_cfg=None, repo=None):
        """Pipeline laden und registrieren. repo jetzt Pflichtparameter."""

        logger.debug(f"Lade Pipeline {plname} mit repo={repo} und app_name={self.app_name}")
        if plname not in self.pipelines:
            # Konfiguration laden, falls nicht explizit übergeben
            if pipeline_cfg is None:
                pipeline_cfg = self.load_config(f"{plname}.yml", phase_subdir='yp')
            pipeline = Pipeline(
                config=pipeline_cfg,
                config_d=self.config_d,
                repo=repo,
                app_name=self.app_name,
                plname=plname,
                data_path=self.data_path,
                master_config_dir=self.master_config_dir,
            )
            self.pipelines[plname] = pipeline

        self.active_pipeline = self.pipelines[plname]
        self.active_pipeline.ypipe_app = self  # Rückverweis setzen
        return self.active_pipeline

    def start_pipeline(self, plname=None):
        """Pipeline starten."""
        if plname is None:
            plname = self.active_pipeline.plname if self.active_pipeline else None
        if plname and plname in self.pipelines:
            self.status = "running"
            self.pipelines[plname].run_all()
            self.status = "done"
        else:
            raise ValueError(f"Pipeline {plname} nicht geladen.")

    def stop_pipeline(self, plname=None):
        """Pipeline stoppen (Stub, Logik ggf. ergänzen)."""
        if plname is None:
            plname = self.active_pipeline.plname if self.active_pipeline else None
        if plname and plname in self.pipelines:
            self.pipelines[plname].stop()
            self.status = "stopped"
        else:
            raise ValueError(f"Pipeline {plname} nicht geladen.")

    def get_status(self):
        return self.status

    def get_active_pipeline(self):
        return self.active_pipeline

    def register_pipeline_status_callback(self, plname, callback):
        """Registriert einen Callback für Pipeline-Statusänderungen."""
        pipeline = self.pipelines.get(plname)
        if pipeline:
            pipeline.register_pipeline_status_callback(callback)
        else:
            raise ValueError(f"Pipeline {plname} nicht geladen.")

    def register_task_status_callback(self, plname, callback):
        """Registriert einen Callback für Task-Statusänderungen."""
        pipeline = self.pipelines.get(plname)
        if pipeline:
            pipeline.register_task_status_callback(callback)
        else:
            raise ValueError(f"Pipeline {plname} nicht geladen.")

    def get_subpipelines(self, plname=None):
        """Gibt Subpipelines der angegebenen Pipeline zurück (Stub, Logik ggf. ergänzen)."""
        if plname is None:
            plname = self.active_pipeline.plname if self.active_pipeline else None
        pipeline = self.pipelines.get(plname)
        if pipeline and hasattr(pipeline, 'subpipelines'):
            return pipeline.subpipelines
        return []

    def get_pipeline_status(self, plname=None):
        """Gibt den Status der angegebenen Pipeline zurück."""
        if plname is None:
            plname = self.active_pipeline.plname if self.active_pipeline else None
        pipeline = self.pipelines.get(plname)
        if pipeline:
            return getattr(pipeline, 'status', None)
        return None

    def run_subpipeline(self, parent_pipeline, subpipeline_name, subpipeline_cfg,
                        parent_components=None, repo=None, app_name=None):
        """Lädt, registriert und startet eine Subpipeline. Gibt die Subpipeline-Instanz zurück."""

        pipeline = self.load_pipeline(subpipeline_name, subpipeline_cfg, repo)
        pipeline.register_task_defs_from_list(subpipeline_cfg.get('tasks', []))
        pipeline.run_all()

        return pipeline

    def init_fc(self):
        self.fc.phase = 'p1'
        self.fc.phase_subdir = 'p1'
        kp_list = []
        if hasattr(self, 'config_list'):
            kp_list = self.config_list() + ['profile']
        logger.debug('kp_list: %s', kp_list)
        self.fc.configure(cfg_kp_frames=getattr(self, 'cfg_kp_frames', None),
                          cfg_profile=getattr(self, 'cfg_profile', None),
                          cfg_kp_si=getattr(self, 'cfg_kp_si', None),
                          cfg_kp_process_fields=getattr(self, 'cfg_kp_process_fields', None))
        self.fc.init_framecache()
        self.fc.init_fc_bytype()
        self.fc.build_fieldlists(getattr(self.fc, 'cfg_kp_process_fields', None))

    # Weitere Methoden für Subpipelines, Events, etc. können ergänzt werden
