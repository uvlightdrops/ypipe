from flowpy.utils import setup_logger

from ypipe.log_utils import log_context

logger = setup_logger(__name__, __name__ + '.log')

# Missing imports and module-scoped defaults required by helper functions
from pathlib import Path
import importlib
import importlib.util
import inspect
from typing import Any, cast, Dict

# Mapping cache
_mapp: Dict[str, Any] = None
_mapp_tr: Dict[str, Any] = None

# Base Task classes placeholders; will try to import the real ones lazily
Task = None
ResourceTask = None





# for custom_tasks directory loading
def import_task_modules_from_dir(directory):
    """
    Lädt alle *.py-Module aus dem gegebenen Verzeichnis.
    Parameter:
      - directory: pathlib.Path oder string zum Verzeichnis
    Rückgabe:
      - Liste importierter Modulobjekte
    """
    modules = []
    base = Path(directory)
    logger.debug(f"Importing task modules from directory: {base}")
    if not base.exists() or not base.is_dir():
        logger.debug(f"Task mod directory `{base}` not found or not a dir, skipping import.")
        return modules

    for path in sorted(base.glob('*.py')):
        #logger.debug(f"Importing modules from {path}")
        if path.name.startswith('__'):
            continue
        modulename = f"{base.name}.{path.stem}"
        #logger.debug(f"Importing module {modulename} from {path}")
        try:
            spec = importlib.util.spec_from_file_location(modulename, str(path))
            if spec and spec.loader:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                modules.append(mod)
                logger.debug(f"Imported module: {modulename}")
            else:
                logger.warning(f"No spec/loader for {path}, skipping.")
        except Exception as e:
            logger.warning(f"Failed to import {path}: {e}")
    return modules


# Simple implementation to collect task classes from modules.
# This is intentionally conservative: it picks up classes whose name ends with
# 'Task' and maps a simple action key (lowercase name without 'Task') to the class.
def get_task_classes(modules, suffix='Task'):
    mapping = {}
    for mod in modules:
        if not mod:
            continue
        for name, obj in inspect.getmembers(mod, inspect.isclass):
            # Skip classes that are defined elsewhere (not in this module)
            if getattr(obj, '__module__', None) != getattr(mod, '__name__', None):
                continue
            if name == suffix:
                continue  # skip base Task class itself
            if name.endswith(suffix):
                #logger.debug(f"Discovered class: {name} in module {mod.__name__}")
                name = name.rstrip(suffix)
                # lowercase first letter, rest as is
                key = name[0].lower() + name[1:]
                mapping[key] = obj
    return mapping


def _init_mapping(repo):
    """Initialize the module->task-class mapping once, using project_dir for extra modules.
    This avoids performing filesystem imports at module import time. No threading lock used.

    Key responsibilities:
    - Try to locate a project-local `custom_tasks` directory (via `env.project_dir`).
    - Fall back to a repository-local `custom_tasks` if the project one is missing.
    - Import any task modules found there and include them when building the mapping.
    """
    # XXX cleanup copilot stuff

    global _mapp, Task, ResourceTask
    if _mapp is not None:
        # Mapping already initialized — nothing to do.
        return
    extra_modules = []

    try:
        # Use a local import for Path to keep module-level imports minimal and
        from pathlib import Path as _Path

        # Try to import the project-specific `env` module which may define
        # `project_dir`. This import is attempted only here (lazy resolution).
        try:
            # The inner try/except prevents choking when `env` is not available
            # (for example in test or when merely showing CLI help).
            from env import project_dir as PROJECT_DIR_LOCAL  # type: ignore
            # If env is present, prefer project-local custom_tasks.
            custom_tasks_dir = _Path(PROJECT_DIR_LOCAL) / 'custom_tasks'
            logger.debug("Resolved PROJECT_DIR from env module: %s", PROJECT_DIR_LOCAL)
        except Exception:
            logger.debug("Could not import project-local `env` module; falling back to repo-level heuristics for `custom_tasks` directory.")
            # pkg_dir is expected to be .../ypipe/ypipe; go two levels up to reach repo root
            custom_tasks_dir = repo.joinpath('custom_tasks')

        if custom_tasks_dir.exists():
            logger.debug(f"Loading extra task modules from custom_dir: {custom_tasks_dir}")
            # Discover and import any task modules under the selected custom_dir.
            # This function may return an empty list if no modules found.
            extra_modules = import_task_modules_from_dir(custom_tasks_dir)

    except Exception as e:
        # Don't raise here: failing to load extra modules is non-fatal. Log a
        # warning so the user can debug missing custom task discovery.
        logger.warning(f"Could not load extra modules from project_dir: {e}")
        extra_modules = []

    # Combine core task modules with any project-specific ones and create the mapping.
    # Note: the order here determines lookup precedence; core modules are first.
    # Attempt to import core modules (best-effort); allow failures but continue.
    core_modules = []
    core_names = ['resourceTask',
                  'frameResourceTask',
                  'iaFrameResourceTask',
                  'storageResourceTask',
                  'readerTask',
                  'includeTaskgroupTask',
                  'includePipelineTask',
                  'fileTask',
                  'task']
    pkg = __package__
    logger.debug(f"Importing core task modules from package: {pkg}")
    for name in core_names:
        try:
            if pkg:
                mod = importlib.import_module(f'.{name}', pkg)
                #logger.debug(f"Imported core task module: {pkg}.{name}")
            else:
                mod = importlib.import_module(name)
            core_modules.append(mod)
        except Exception as e:
            logger.debug("Could not import core task module %s: %s", name, e)

    # Try to bind Task and ResourceTask from the imported 'task' module if available.
    for mod in core_modules:
        #logger.debug(f"Checking core module for Task classes: {getattr(mod, '__name__', None)}")
        if not mod:
            continue
        if getattr(mod, '__name__', '').endswith('.task') or getattr(mod, '__name__', '') == 'task':
            Task = getattr(mod, 'Task', Task)
            ResourceTask = getattr(mod, 'ResourceTask', ResourceTask)

    all_modules = [m for m in core_modules if m is not None] + extra_modules

    # Build the mapping of action name -> class. This may import dynamic classes
    # from the modules discovered above and can be moderately expensive, hence
    # the laziness and single initialization.
    _mapp = get_task_classes(all_modules)
    logger.debug("Task mapping initialized: %s", list(_mapp.keys()))


def _get_mapp(repo):
    # Small helper to ensure mapping is initialized before use.
    if _mapp is None:
        _init_mapping(repo)
    return _mapp



class TaskFactory:

    @staticmethod
    def create_task(t_def, context):
        """Factory entry point: create an instance for the task definition.

        Behavior summary:
        - Look up by action name in the pre-built mapping and instantiate the class.
        - If the task is marked as type 'resource', create a basic ResourceTask.
        - If no mapping is found, raise an exception to surface unknown actions.
        """
        action = t_def.get('action')
        #logger.debug("Creating task of action type %s", action)

        # Ensure mapping is ready and then attempt to resolve the action to a class.
        mapp = _get_mapp(context['repo'])
        if t_def['name'] == 'initSR_kp_src':
            logger.debug('TaskFactory creating initSR_kp_src task')
            #log_context(context, "TaskFactory context")


        if action in mapp:
            klass = mapp[action]
            # The resolved klass is likely loaded dynamically from a module; log it.
            logger.debug(f"Creating task klass %s", klass)
            # We intentionally instantiate the class with the canonical constructor
            # signature used across task classes: (name, definition, context).
            return cast(Any, klass)(t_def['name'], t_def, context)
        elif t_def.get('type') == 'resource':
            # Resource tasks are treated specially and created from a base resource class.
            # This branch ensures backward compatibility with configs that define resources
            # but do not have a dedicated 'action' mapping.
            logger.debug('task type = resource, so create BASIC resource task')
            return ResourceTask(t_def['name'], t_def, context)
        else:
            # Unknown action/type — raise early to help developers discover misconfig.
            logger.debug(f"Unknown task action/type: {action}/{t_def.get('type')}, using base Task class")
            # Raising an explicit exception here is intentional during development so
            # that configuration issues are caught rather than silently ignored.
            raise Exception(f"Unknown task action/type: {action}/{t_def.get('type')}")
            # If you prefer a softer failure, replace the raise with returning
            # a generic Task instance instead (commented out below):
            # return Task(t_def['name'], t_def, context)
