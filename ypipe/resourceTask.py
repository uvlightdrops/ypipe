from task import Task
from flowpy.utils import setup_logger

logger = setup_logger(__name__, __name__+'.log')

# additional resources can be
# the kp.....yml files
# data files like xlsx, csv, db, ...of course

class ResourceTask(Task):
    resource = None
    def __init__(self, *args):
        super().__init__(*args)
        self.sc = self.context['storage_cache']

    def get_resource(self):
        return self.resource
    def run(self):
        return self.resource


class StorageResourceTask(ResourceTask):
    def __init__(self, *args):
        super().__init__(*args)
        type = self.config['args']['type']
        name = self.config['name']
        src = self.context['data_path'].joinpath(self.config['args']['fn'])
        pw = open(self.context['config_dir'].joinpath('creds.txt')).read().strip()

        resource = self.sc.get_resource(type, pw=pw)
        resource.src_or_dst = 'src'
        resource.set_src(src)
        self.context[name] = resource
        self.resource = self.sc.get_resource(type)


class FrameResourceTask(ResourceTask):
    """ Provides a frame resource from frame cache"""
    def __init__(self, *args):
        super().__init__(*args)
        self.frame_group = self.config['args']['frame_group']
        self.group = self.config['args'].get('group', None)

    def run(self):
        self.resource = self.context['fc'].get_frame(self.frame_group, self.group)
        self.context[self.config['name']] = self.resource
        return self.resource

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
        return self.frame

class CreateFrameTask(Task):
    def __init__(self, *args):
        super().__init__(*args)

    def run(self):
        # Frame erstellen
        pass

class DebugFrameTask(ResourceTask):
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
        logger.debug("writeFrameGroupTask running")
        frame_group = self.config['args']['frame_group']
        frame = self.context['fc'].get_frame_group(frame_group)
        self.context['fc'].write_frame_group(frame_group, frame)
        logger.debug("writeFrameGroupTask wrote frame group %s ", frame_group)
        return frame

class ExternalResourceTask(ResourceTask):
    def __init__(self, *args):
        super().__init__(*args)
        #self.config = config

    def run(self):
        # Zugriff auf externe Resource
        pass
