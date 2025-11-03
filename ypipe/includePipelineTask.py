import yaml
from pathlib import Path
from collections import defaultdict
import networkx as nx

from .log_utils import log_context
from .pipeline import Pipeline, render_template
from .task import Task
from flowpy.utils import setup_logger
import pprint

logger = setup_logger(__name__, __name__ + '.log')


class IncludePipelineTask(Task):
    """Create and run a lightweight Pipeline from an included YAML.

    Design goals (points 1-4 considered):
    - Semantics: use Pipeline's API to register and run tasks (TaskFactory etc.).
    - Context sharing: share resource-heavy components (fc, storage) from parent context.
    - Namespacing: set plname to the include file stem to avoid collisions.
    - Lifecycle: build a minimal Pipeline instance, register tasks and run it, then return status.
    """

    def __init__(self, name: str = "include_pipeline", config: dict = None, context: 'Context' = None):
        super().__init__(name, config, context)

    def run(self):

        include = self.args.get('include')
        if not include:
            logger.error("IncludePipelineTask needs 'include' arg")
            return False

        include_path = self.context['config_dir'].joinpath(include).with_suffix('.yml')
        logger.debug("IncludePipelineTask loading include from: %s", include_path)
        # resolve include path and load YAML (reuse Pipeline.from_config_file if convenient)
        try:
            if include_path.exists():
                # load the whole doc and create sub-pipeline
                with open(include_path, 'r', encoding='utf-8') as fh:
                    sub_doc = yaml.safe_load(fh)
            else:
                sub_doc = yaml.safe_load(include)
        except Exception as e:
            logger.exception("Failed to load include YAML '%s': %s", include, e)
            return False

        # normalize to list or doc with 'tasks'
        if isinstance(sub_doc, dict) and 'tasks' in sub_doc and isinstance(sub_doc['tasks'], list):
            task_defs = sub_doc['tasks']
        else:
            raise ValueError("IncludePipelineTask include YAML must be a dict with 'tasks' list")
        sub_doc = {
            'tasks': task_defs,
            'config_d': self.context['config_d'],
        }

        # create sub-pipeline via factory, sharing heavy components
        parent_components = {
            'fc': self.context.get('fc'),
            'storage_broker': self.context.get('storage_broker'),
            'storage_cache': self.context.get('storage_cache'),

        }
        sub_plname = (include_path.stem)
        sub_pipeline = Pipeline.from_config_doc(sub_doc,
                            repo=self.context.get('repo'),
                            app_name=self.context.get('app_name'),
                            data_path=self.context.get('data_path'),
                            plname=sub_plname,
                            parent_components=parent_components)

        sub_pipeline._parent_ctx = self.context

        sub_pipeline.context = self.context.copy()

        # register/render tasks and run using Pipeline API
        # templ_ctx = sub_pipeline.prepare_context()
        #parent_view = self.context.copy()
        #sub_view = {**parent_view, **(self.context)}
        #log_context(sub_view, 'IncludePipelineTask subview')

        sub_pipeline.is_subpipeline = True
        sub_pipeline.register_task_defs_from_list(task_defs) #, templ_d=sub_view)


        ### RUN ALL TASKS IN SUB-PIPELINE
        try:
            logger.debug(f'subpipe {sub_plname} starting run_all()')
            result_context = sub_pipeline.run_all()
            if result_context is not None:
                logger.debug(f'subpipe {sub_plname} run_all() returned context')
                log_context(result_context, 'IPP --------------- result_context')

            # last task of the pipeline has current context


            #log_context(sub_pipeline.context_result_subpipeline, msg='context result')
            #result = getattr(sub_pipeline, 'context_result_subpipeline', None)
            #log_context(result, 'IncludePP sub-pipeline result')
        except Exception:
            raise
            # return False


        # nur erlaubte Keys übernehmen, z.B. kp_src, kp_dst, oder ein whitelist var
        for k in ('kp_src', 'kp_dst'):
            if k in result_context:
                logger.debug(f'IncludePP copying context key: {k}')
                self.context[k] = result_context[k]

        logger.info("Included pipeline completed")
        log_context(self.context, 'IncludePP sub-pipeline after run')

        return
