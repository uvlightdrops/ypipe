import yaml
from copy import deepcopy
from ypipe.taskFactory import TaskFactory
from ypipe.task import Task
from flowpy.utils import setup_logger
from pathlib import Path
# XXX make global setting
DEBUG = True
if DEBUG:
    import pprint

logger = setup_logger(__name__, __name__ + '.log')


class IncludeTaskgroupTask(Task):
    def __init__(self, name: str = "include", config: dict = None, context: 'Context' = None):
        super().__init__(name, config, context)

    def _deep_merge(self, base: dict, override: dict) -> dict:
        """recursively merge override into base (override wins)"""
        if not isinstance(base, dict) or not isinstance(override, dict):
            return deepcopy(override)
        out = deepcopy(base)
        for k, v in override.items():
            if k in out and isinstance(out[k], dict) and isinstance(v, dict):
                out[k] = self._deep_merge(out[k], v)
            else:
                out[k] = deepcopy(v)
        return out

    def run(self):
        include = self.args.get('include')
        if not include:
            logger.error("IncludeTaskgroupTask needs 'include' arg")
            return False

        include_path = self.context['config_dir'].joinpath(include).with_suffix('.yml')

        # load YAML from file path or string
        try:
            if include_path is not None and include_path.exists():
                with open(include_path, 'r', encoding='utf-8') as fh:
                    doc = yaml.safe_load(fh)
            else:
                # treat include as raw YAML string
                doc = yaml.safe_load(include)
        except Exception as e:
            logger.exception("Failed to load include YAML '%s': %s", include, e)
            if self.args.get('on_error', 'stop') == 'stop':
                raise
            return False

        # normalize to list of task defs
        if isinstance(doc, dict) and 'tasks' in doc and isinstance(doc['tasks'], list):
            task_defs = doc['tasks']
        elif isinstance(doc, list):
            task_defs = doc
        else:
            logger.error("Included YAML must be a list or contain top-level 'tasks' list")
            if self.args.get('on_error', 'stop') == 'stop':
                raise ValueError("invalid include content")
            return False

        # optional overrides applied to each task (analogy to kwargs)
        overrides = self.args.get('args', {})
        logger.debug("IncludeTaskgroupTask applying overrides: %s", overrides)

        # recursion guard in context
        seen = self.context.get('_include_seen', set())
        if isinstance(include, str):
            include_id = include
        if include_id in seen:
            logger.error("Recursive include detected: %s", include_id)
            raise RuntimeError("recursive include")
        seen = set(seen)  # copy to avoid mutating external unintentionally
        seen.add(include_id)
        # push seen into a transient child context for included tasks
        child_context = dict(self.context)
        child_context['_include_seen'] = seen


        failures = 0
        on_error = self.args.get('on_error', 'stop')

        for t_def in task_defs:
            try:
                # apply overrides (merge 'args' inside task defs)
                merged = deepcopy(t_def)
                if overrides:
                    # If override intended for whole task, merge at top-level; commonly for 'args' do:
                    if 'args' in merged and isinstance(merged['args'], dict):
                        merged['args'] = self._deep_merge(merged['args'], overrides)
                    else:
                        merged = self._deep_merge(merged, {'args': overrides})

                # validate and create task using existing factory
                # TaskFactory.create_task expects (t_def, context)
                logger.debug('merged task definition for include:')
                lm = pprint.pformat(merged)
                logger.debug(lm)

                task = TaskFactory.create_task(merged, child_context)

                # prepare & run as normal tasks (sequential)
                try:
                    pass
                    task.prepare()
                except Exception:
                    logger.exception("prepare() failed for included task %s", merged.get('name'))
                    if on_error == 'stop':
                        raise

                try:
                    task.run()
                except Exception:
                    logger.exception("run() failed for included task %s", merged.get('name'))
                    failures += 1
                    if on_error == 'stop':
                        raise
            except Exception as e:
                logger.exception("Error while handling included task: %s", e)
                failures += 1
                if on_error == 'stop':
                    raise

        if failures:
            logger.warning("IncludeTaskgroupTask finished with %d failures", failures)
            return False
        logger.info("IncludeTaskgroupTask completed successfully")
        return True

