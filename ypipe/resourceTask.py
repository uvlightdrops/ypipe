from task import Task
from loopMixin import LoopMixin
from flowpy.utils import setup_logger

logger = setup_logger(__name__, __name__+'.log')

# additional resources can be
# the kp.....yml files
# data files like xlsx, csv, db, ...of course

class ResourceTask(Task):
    resource = None
    def __init__(self, *args):
        super().__init__(*args)
        self.storage_cache = self.context['storage_cache']
        self.sc = self.storage_cache

    def get_resource(self):
        return self.resource
    def run(self):
        pass
        #return self.resource


class StorageResourceTask(ResourceTask):
    def __init__(self, *args):
        super().__init__(*args)
        type = self.config['args']['type']
        name = self.config['name']
        creds_file = self.config['args']['creds_file']
        src = self.context['data_path'].joinpath(self.config['args']['fn'])
        pw = open(self.context['config_dir'].joinpath(creds_file)).read().strip()
        logger.debug('res %s type %s from %s', name, type, src)
        #logger.debug('pw from file %s: %s', creds_file, pw)

        resource = self.sc.get_resource(name, type=type, pw=pw)
        resource.src_or_dst = 'src'
        resource.set_src(src)
        # Das hier ist net am rechten platz, sollte temporär nur für run methode nötig sein
        self.context[name] = resource
        self.resource = resource


class FrameResourceTask(LoopMixin, ResourceTask):
    """ Provides a frame resource from frame cache"""
    def __init__(self, *args):
        super().__init__(*args)
        self.frame_group = self.config['args']['frame_group']
        self.group = self.config['args'].get('group', None)

    def run(self):
        self.resource = self.context['fc'].get_frame(self.frame_group, self.group)
        self.context[self.config['name']] = self.resource
        #return self.resource

class StoreFrameResourceTask(FrameResourceTask):
    def run(self):
        frame = self.context[self.config['args']['in']]
        self.context['fc'].store_frame(self.frame_group, self.group, frame)
        self.context[self.config['name']] = frame

class ReadFrameResourceTask(FrameResourceTask):
    def run(self):
        frame_group = self.config['args']['frame_group']
        group = self.config['args'].get('group', None)
        self.frame = self.context['fc'].get_frame(frame_group, group)
        self.context[self.config['name']] = self.frame
        #return self.frame


class DebugFrameResourceTask(FrameResourceTask):
    def __init__(self, *args):
        super().__init__(*args)

    def run(self):
        frame_group = self.config['args']['frame_group']
        group = self.config['args'].get('group', None)
        self.frame = self.context['fc'].get_frame(frame_group, group)
        #logger.debug(type(self.frame[group]))
        #logger.debug(self.frame[group].head(3))
        #return self.frame

class TransformTask(Task):
    def __init__(self, *args):
        super().__init__(*args)
        #self.config = config

    def run(self):
        # Transformation durchführen
        pass

class WriteFrameResourceTask(FrameResourceTask):
    def __init__(self, *args):
        super().__init__(*args)

    def run(self):
        frame_group = self.config['args']['frame_group']
        fg = self.context['fc'].get_frame_group(frame_group)
        #logger.debug(fg['Alfresco'].head(3))
        logger.debug('framegroup %s, keys to write: %s', frame_group, fg.keys())
        self.context['fc'].write_frame_group(frame_group, fg)
        logger.debug("writeFrameGroupTask wrote frame group %s ", frame_group)
        #return fg

class ExternalResourceTask(ResourceTask):
    def __init__(self, *args):
        super().__init__(*args)
        #self.config = config

    def run(self):
        # Zugriff auf externe Resource
        pass
