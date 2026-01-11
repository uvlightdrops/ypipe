"""
YpipeApp: Zentrale Backend-Klasse für ypipe
Verwaltet Pipelines, Konfiguration und Status
"""


from ypipe.pipeline import Pipeline

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__ + '.log')


class YpipeApp:
    def __init__(self, app_name=None, **kwargs):
        self.app_name = app_name
        self.pipelines = {}  # plname -> Pipeline
        self.active_pipeline = None
        self.status = "idle"
        kws = ['data_path', 'master_config_dir', 'project_dir', 'plname']
        logger.debug('kwargs: %s', kwargs)
        for key in kws:
            if key in kwargs.keys():
                logger.debug(f"Setze Attribut {key} auf {kwargs[key]}")
                setattr(self, key, kwargs[key])


    def load_pipeline(self, plname, pipeline_cfg=None, repo=None):
        """Pipeline laden und registrieren. repo jetzt Pflichtparameter."""

        logger.debug(f"Lade Pipeline {plname} mit repo={repo} und app_name={self.app_name}")
        if plname not in self.pipelines:
            pipeline = Pipeline(
                cfg=pipeline_cfg,
                repo=repo,
                app_name=self.app_name,
                plname=plname,
                data_path=self.data_path,
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

        pipeline.run_all()

        return pipeline

    # Weitere Methoden für Subpipelines, Events, etc. können ergänzt werden

