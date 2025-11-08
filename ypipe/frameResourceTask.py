
from .resourceTask import ResourceTask
from .loopMixin import LoopMixin
from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class FrameResourceTask(LoopMixin, ResourceTask):
    """ Provides a frame resource from frame cache"""
    def __init__(self, *args):
        super().__init__(*args)
        #logger.debug("FrameResourceTask init")
        # FrameResourceTask special
        self.frame_group_name = self.args.get('frame_group_name', None)
        self.group = self.args.get('group', None)
        logger.debug("%s FRT init frame_group_name: %s, group: %s",
                     self.name, self.frame_group_name, self.group)

    def run(self):
        logger.debug("FrameResourceTask OR should run subclass run()??")
        self.prepare()

        self.resource = self.context['fc'].get_frame(self.frame_group_name, self.group)

        # XXX this was a pure guess and should be checked
        self.context[ self.config['name'] ] = self.resource


class StoreFrameResourceTask(FrameResourceTask):
    """ Store a frame resource to frame cache"""
    # XXX this init is new, is it needed??
    def __init__(self, *args):
        super().__init__(*args)
    def run(self):
        logger.debug("33 StoreFrameResourceTask storing frame resource to frame cache")
        self.prepare()

        # d.h. ganze framegroup speichern XXX rename use_frame_group_dict ??
        frame_group_dict = self.args.get('frame_group_dict', None)

        if frame_group_dict:
            logger.debug("39 store full frame group %s",  self.frame_group_name)
            fg_d = self.context[self.frame_group_name]

            logger.debug('42 frame group to store - keys: %s, %s', fg_d.keys(), self.name)
            #logger.debug(fg_d['Weblogic'].head(3))
            self.context['fc'].store_frame_group(self.frame_group_name, fg_d)

        else:
            logger.debug("store simple frame in frame group %s, group %s",  self.frame_group_name, self.group)
            frame = self.context[self.config['args']['in']]

            self.context['fc'].store_frame(self.frame_group_name, self.group, frame)
            logger.debug('provides: %s', self.provides)
            if self.provides:
                self.context[self.provides['main']] = frame


class StoreFrameGroupResourceTask(FrameResourceTask):
    """ Store a frame group resource to frame cache"""
    def __init__(self, *args):
        super().__init__(*args)
    def run(self):
        logger.debug("StoreFrameGroupResourceTask storing frame group resource to frame cache")
        self.prepare()

        frame_group_name = self.frame_group_name or self.item
        frame_group = self.context.get(frame_group_name, None)
        #frame_group_dict = self.context[self.config['args']['in']]

        #logger.debug('frame group to store - keys: %s, %s', frame_group_dict.keys(), self.name)
        self.context['fc'].store_frame_group(frame_group_name, frame_group)


class ReadFrameResourceTask(FrameResourceTask):
    """ Read a frame resource from frame cache"""
    def run(self):
        self.prepare()
        frame_group_name = self.frame_group_name
        group = self.group or self.item
        logger.debug("RFRT frame group name %s, group %s", frame_group_name, group)
        frame = self.context['fc'].get_frame(frame_group_name, group)

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

        # if no frame_group_name arg use group = corresponds to loop item
        frame_group_name = self.args.get('frame_group_name', None) or self.group
        logger.debug("DebugFrameGroupResourceTask frame group name %s", frame_group_name)
        fg = self.context['fc'].get_frame_group(frame_group_name)
        for k, v in fg.items():
            logger.debug("frame %s: rows %d, cols %d", k, v.shape[0], v.shape[1])
            #logger.debug(v.head(3))


# XXX currently gets date from context , NOT from frame cache
# means output to file or db just one sheet
class WriteFrameResourceTask(FrameResourceTask):
    def __init__(self, *args):
        super().__init__(*args)
        logger.debug("WriteFrameResourceTask init frame_group_name: %s", self.frame_group_name)
        self.writer = self.context['fc'].get_writer(self.group)

    def run(self):
        self.prepare()

        group = self.group or self.item
        self.writer.set_dst(self.context['data_out_path'].joinpath(group))
        buffer = self.context.get(group, None)
        #buffer = self.context['fc'].get_frame(group, None)
        if self.provide_main:
            p_key_main = self.provide_main.get('key')
        if buffer is None:
            logger.error("WFRTask: %s no buffer in context for group %s", self.config['name'], group)
            return
        #logger.debug(buffer.head(3))
        self.writer.set_buffer(group, buffer)
        self.writer.init_writer_all()
        self.writer.write()

        #fg = self.context['fc'].get_frame_group(self.frame_group_name)
        #logger.debug('framegroup %s, keys to write: %s', self.frame_group_name, fg.keys())
        #self.context['fc'].write_frame_group(self.frame_group_name, fg)
        logger.debug("writeFrameGroupTask wrote frame group %s ", self.frame_group_name)
"""
"""


class WriteFrameGroupResourceTask(FrameResourceTask):
    def __init__(self, *args):
        super().__init__(*args)

    def run(self):
        self.prepare()

        # this seems a fine solution
        frame_group_name = self.frame_group_name or self.item

        fg = self.context['fc'].get_frame_group(frame_group_name)
        logger.debug('"%s", keys to write: %s', frame_group_name, fg.keys())

        self.context['fc'].write_frame_group(frame_group_name)
        logger.debug("WriteFrameGroupResourceTask wrote frame group %s ", frame_group_name)


class ModifyFrameResourceTask(FrameResourceTask):
    def __init__(self, *args):
        super().__init__(*args)

    def run(self):
        self.prepare()

        group = self.group or self.item
        frame = self.context['fc'].get_frame(self.frame_group_name, group)
        logger.debug("ModifyFrameResourceTask modifying frame group %s, group %s", self.frame_group_name, group)


        # store modified frame back to frame cache
        self.context['fc'].store_frame(self.frame_group_name, group, frame)
        logger.debug("ModifyFrameResourceTask stored modified frame for group %s", group)


