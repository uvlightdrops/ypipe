from .resourceTask import ResourceTask
from .loopMixin import LoopMixin
from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')

from ypipe.log_utils import log_context
import pandas as pd


# frame resource is a resource that hold dataframes in groups
# it has the legacy framecache functionality
# tasks that have sth to do with these frames, can either load from
# or store to it.
class FrameResourceTask(LoopMixin, ResourceTask):
    """ Provides a frame resource from frame cache"""
    def __init__(self, *args):
        super().__init__(*args)
        #logger.debug("FrameResourceTask init")
        # FrameResourceTask special
        self.fc = self.context.get('fc')
        self.frame_group_name = self.args.get('frame_group_name', None)
        self.group = self.args.get('group', None)
        logger.debug("%s FRT init frame_group_name: %s, group: %s",
                     self.name, self.frame_group_name, self.group)

    def run(self):
        logger.warning("FrameResourceTask OR should run subclass run()??")



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
            frame = self.context[self.args['in']]

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


class ReadFrameGroupResourceTask(FrameResourceTask):
    def run(self):
        self.prepare()

        frame_group_name = self.args.get('frame_group_name', None) or self.group
        logger.debug("ReadFrameGroupResourceTask frame group name %s", frame_group_name)
        fg = self.context['fc'].get_frame_group(frame_group_name)

        self.context[self.config['name']] = fg


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
        self.writer = self.context['fc'].get_writer(self.group, writer_type=self.args.get('writer_type', None))

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
        self.writer.out_fns = [group]
        self.writer.init_writer_all()
        self.writer.write()

        logger.debug("writeFrameGroupTask wrote frame %s ", group)


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
    """ inline modification of frame in frame cache"""
    def __init__(self, *args):
        super().__init__(*args)

    def run(self):
        self.prepare()

        group = self.group or self.item
        frame = self.context['fc'].get_frame(self.frame_group_name, group)
        logger.debug("ModifyFrameResourceTask modifying frame group %s, group %s", self.frame_group_name, group)

        # stub base class does nothing
        # subclasses implement business logic here

        # store modified frame back to frame cache
        self.context['fc'].store_frame(self.frame_group_name, group, frame)
        logger.debug("ModifyFrameResourceTask stored modified frame for group %s", group)



class MergeFrameResourceTask(FrameResourceTask):
    def __init__(self, *args):
        super().__init__(*args)

    def run(self):
        log_context(self.context, 'MergeFramesTask run')
        self.prepare()
        # here for now fetch the frames from framecache
        # even while the thought was to have them in context already
        group = self.group or self.item

        self.business_logic()

        # general merge class
        #df1 = self.context['fc'].get_frame(self.args['in1'], gn_old)
        #df2 = self.context['fc'].get_frame(self.args['in2'], self.item)
        # somehow the ent_old_tag_xx is subcatted by case_name., NOT groups_old
        tmp1 = self.context.get(self.args['in1'], {})
        df1 = tmp1.get(self.item, None)
        tmp2 = self.context.get(self.args['in2'], {})
        df2 = tmp2.get(self.item, None)

        msg = 'frame to merge is None: '
        if df1 is None:
            logger.error(msg+'F1')
            raise ValueError(msg+'F1')
        if df2 is None:
            logger.error(msg+'F2')
            raise ValueError(msg+'F2')
        logger.debug('len df1: %s', len(df1))
        logger.debug('len df2: %s', len(df2))
        #logger.debug('df1 columns: %s', df1.columns.tolist())
        #logger.debug('df2 columns: %s', df2.columns.tolist())

        foreign_key = 'item'
        on = self.args.get('on', [foreign_key])
        how = self.args.get('how', 'outer')
        suffixes = self.args.get('suffixes', ('_old', '_new'))
        #logger.debug('Merging on %s, how=%s, suffixes=%s', on, how, suffixes)
        dfm = pd.merge(df1, df2, on=on, how=how, suffixes=suffixes)
        dfm = dfm.fillna('')
        logger.debug('len df merged: %s', len(dfm))

        #logger.debug('provides: %s', self.provides)
        provide = self.provides['main']
        p_key = provide['key']
        logger.debug('providing %s', p_key)

        if not p_key in self.context:
            self.context[p_key] = {}
        self.context[p_key][group] = dfm
