import yaml
from pathlib import Path
from collections import defaultdict
import networkx as nx

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
            if self.args.get('on_error', 'stop') == 'stop':
                raise
            return False

        # normalize to list or doc with 'tasks'
        if isinstance(sub_doc, dict) and 'tasks' in sub_doc and isinstance(sub_doc['tasks'], list):
            task_defs = sub_doc['tasks']
        elif isinstance(sub_doc, list):
            task_defs = sub_doc
            sub_doc = {'tasks': task_defs}
        else:
            logger.error("Included YAML must be a list or contain top-level 'tasks' list")
            if self.args.get('on_error', 'stop') == 'stop':
                raise ValueError("invalid include content")
            return False
        logger.debug('sub_doc keys: %s', sub_doc.keys())

        # Ensure sub-pipeline has access to parent's config dicts for template rendering
        parent_config_d = self.context.get('config_d', {})
        logger.debug("Parent config_d for include: keys: %s", parent_config_d.keys())
        # Merge parent config into sub_doc.config_d, giving precedence to sub_doc values
        sub_config_d = sub_doc.get('config_d', {}) if isinstance(sub_doc.get('config_d', {}), dict) else {}
        lm = pprint.pformat(sub_config_d)
        logger.debug(lm)
        merged_config_d = dict(parent_config_d)
        merged_config_d.update(sub_config_d)
        sub_doc['config_d'] = merged_config_d
        #logger.debug("Merged config_d for included pipeline: keys: %s", merged_config_d.keys())

        # create sub-pipeline via factory, sharing heavy components
        parent_components = {
            'fc': self.context.get('fc'),
            'storage_broker': self.context.get('storage_broker'),
            'storage_cache': self.context.get('storage_cache'),

        }
        sub_plname = (include_path.stem if 'include_path' in locals() else 'included_pipeline')
        sub_pipeline = Pipeline.from_config_doc(sub_doc,
                            repo=self.context.get('repo'),
                            app_name=self.context.get('app_name'),
                            data_path=self.context.get('data_path'),
                            plname=sub_plname,
                            parent_components=parent_components)

        # register/render tasks and run using Pipeline API
        templ_ctx = sub_pipeline.prepare_context()

        sub_pipeline.register_task_defs_from_list(task_defs, templ_ctx=templ_ctx)
        #sub_pipeline.is_subpipeline = True

        try:
            logger.debug(f'subpipe {sub_plname} starting run_all()')
            sub_pipeline.run_all()
        except Exception:
            logger.exception("Included pipeline run failed")
            if self.args.get('on_error', 'stop') == 'stop':
                raise
            return False

        logger.info("Included pipeline completed")
        return True
