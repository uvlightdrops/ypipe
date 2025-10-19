
from .resourceTask import ResourceTask
from .loopMixin import LoopMixin
from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class FrameResourceTask(LoopMixin, ResourceTask):
    """ Provides a frame resource from frame cache"""
    def __init__(self, *args):
        super().__init__(*args)
        logger.debug("FrameResourceTask init")
        # FrameResourceTask special
        self.frame_group_name = self.args.get('frame_group_name', None)
        self.group = self.args.get('group', None)

    def run(self):
        logger.debug("FrameResourceTask OR should run subclass run()??")
        self.prepare()

        self.resource = self.context['fc'].get_frame(self.frame_group_name, self.group)

        self.context[ self.config['name'] ] = self.resource


class StoreFrameResourceTask(FrameResourceTask):
    """ Store a frame resource to frame cache"""
    # XXX this init is new, is it needed??
    def __init__(self, *args):
        super().__init__(*args)
    def run(self):
        logger.debug("StoreFrameResourceTask storing frame resource to frame cache")
        self.prepare()

        # d.h. ganze framegroup speichern
        if self.config.get('frame_group_d', None):
            fg_d = self.config['frame_group_d']
            self.context['fc'].store_frame_group(self.frame_group_name, fg_d)
            logger.debug("StoreFrameResourceTask also stored frame %s in frame group %s", frame_name, v)

        # XXX return success flag? and store in context?
        else:
            logger.debug("store simple frame in frame group %s, group %s",  self.frame_group_name, self.group)
            frame = self.context[self.config['args']['in']]
            self.context['fc'].store_frame(self.frame_group_name, self.group, frame)
            self.context[self.provides[0]] = frame


class ReadFrameResourceTask(FrameResourceTask):
    """ Read a frame resource from frame cache"""
    def run(self):
        self.prepare()
        frame = self.context['fc'].get_frame(self.frame_group_name, self.group)
        self.context[self.config['name']] = frame


class DebugFrameResourceTask(FrameResourceTask):
    def run(self):
        self.prepare()
        logger.debug("DebugFrameResourceTask frame group %s, group %s", self.frame_group_name, self.group)
        frame = self.context['fc'].get_frame(self.frame_group_name, self.group)
        logger.debug(frame.head(3))

class DebugFrameGroupResourceTask(FrameResourceTask):
    def run(self):
        self.prepare()
        logger.debug("DebugFrameGroupResourceTask self.frame_group_name %s", self.frame_group_name)
        # if no frame_group_name arg use group = corresponds to loop item
        frame_group_name = self.args.get('frame_group_name', None) or self.group
        logger.debug("DebugFrameGroupResourceTask frame group name %s", frame_group_name)
        fg = self.context['fc'].get_frame_group(frame_group_name)
        for k, v in fg.items():
            logger.debug("frame %s: rows %d, cols %d", k, v.shape[0], v.shape[1])
            logger.debug(v.head(3))

# means output to file or db
class WriteFrameResourceTask(FrameResourceTask):
    def __init__(self, *args):
        super().__init__(*args)

    def run(self):
        self.prepare()
        #fg = self.context['fc'].get_frame_group(self.frame_group_name)
        #logger.debug('framegroup %s, keys to write: %s', self.frame_group_name, fg.keys())
        self.context['fc'].write_frame_group(self.frame_group_name)
        #self.context['fc'].write_frame_group(self.frame_group_name, fg)
        logger.debug("writeFrameGroupTask wrote frame group %s ", self.frame_group_name)
