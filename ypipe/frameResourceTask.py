from .resourceTask import ResourceTask
from .loopMixin import LoopMixin
from .transformMixin import TransformMixin
from yldpipeNG.statsSupport import StatsSupport

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')

from ypipe.log_utils import log_context
import pandas as pd


# frame resource is a resource that hold dataframes in groups
# it has the legacy framecache functionality
# tasks that have sth to do with these frames, can either load from
# or store to it.
class FrameResourceTask(LoopMixin, ResourceTask, StatsSupport):
    """ Provides a frame resource from frame cache"""
    def __init__(self, *args):
        super().__init__(*args)
        self.df_tmp = {}
        #logger.debug("FrameResourceTask init")
        # FrameResourceTask special
        self.fc = self.context.get('fc')
        self.frame_group_name_in = self.args.get('frame_group_name_in', None)
        self.frame_group_name_out = self.args.get('frame_group_name_out', None)
        self.group = self.args.get('group', None)
        logger.debug("%s FRT init frame_group_name_in: %s, frame_group_name_out: %s, group: %s",
                     self.name, self.frame_group_name_in, self.frame_group_name_out, self.group)

    def run(self):
        logger.warning("FrameResourceTask OR should run subclass run()??")

    # not sure if this is useful
    def save_frame_to_tmp_d(self, df, case_name):
        # the resulting frame should be stored as flat frame to fc
        self.df_tmp[case_name] = df
        """
        p_key = self.provide_main.get('key', None)
        if p_key not in self.df_d:
            self.df_d[p_key] = {}
        self.df_d[p_key][case_name] = df
        """



class StoreFrameResourceTask(FrameResourceTask):
    """ Store a frame resource to frame cache"""
    # XXX this init is new, is it needed??
    def __init__(self, *args):
        super().__init__(*args)
    def run(self):
        logger.debug("StoreFrameResourceTask storing frame resource to frame cache")
        self.prepare()

        # d.h. ganze framegroup speichern XXX rename use_frame_group_dict ??
        frame_group_dict = self.args.get('frame_group_dict', None)

        if frame_group_dict:
            logger.debug("StoreFrameResourceTask storing full frame group %s", self.frame_group_name_out)
            fg_d = self.context.get_frame_group(self.frame_group_name_out)
            logger.debug('Frame group to store - keys: %s, %s', fg_d.keys(), self.name)
            self.fc.store_frame_group(self.frame_group_name_out, fg_d)
        else:
            logger.debug("StoreFrameResourceTask storing simple frame in frame group %s, group %s", self.frame_group_name_out, self.group)
            frame = self.context.get_frame(self.frame_group_name_in, self.group)
            self.fc.store_frame(self.frame_group_name_out, self.group, frame)
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
        logger.debug("run item %s", self.item)
        frame_group_name_out = self.item or self.frame_group_name_out
        fg_d = self.context.get_frame_group(frame_group_name_out)

        logger.debug('Frame group to store - keys: %s, %s', fg_d.keys(), frame_group_name_out)
        self.fc.store_frame_group(frame_group_name_out, fg_d)


class LoadFrameResourceTask(FrameResourceTask):
    """ Load a frame resource into frame cache, uses reader, so should be named ReadFrameResourceFromSourceTask

    """
    def run(self):

        self.prepare()
        group = self.group or self.item
        # XXX create a smaller cfg_si for the reader
        # from the main kp_si in config_d
        logger.debug("LoadFrameResourceTask group %s", group)
        reader_name = group
        fn = self.args.get('fn', None) or group
        reader = self.fc.get_reader(
            reader_name,
            reader_type=self.args.get('reader_type', None),
            cfg_si=self.context['config_d'].get('kp_si')
        )

        reader.set_src_dir(self.context['data_in_path'])
        reader.set_fn(fn)
        reader.init_reader()
        reader.read(fn)
        df = reader.get_buffer(fn)
        p_key = self.provide_main.get('key')
        self.context.store_frame(group, df)



class LoadFrameGroupResourceTask(FrameResourceTask):
    """ Load a frame group resource into frame cache, uses reader, so should be named ReadFrameGroupResourceFromSourceTask
    """
    def run(self):
        self.prepare()
        logger.debug("LoadFrameGroupResourceTask frame group name %s", self.frame_group_name_in)
        reader = self.fc.get_reader_group(
            self.frame_group_name_in,
            reader_type=self.args.get('reader_type', None),
            cfg_si=self.context['config_d'].get('kp_si')
        )
        reader.set_src_dir(self.context['data_in_path'])
        reader.set_fn(self.args.get('fn', None))
        reader.init_reader()
        reader.read_all()
        fg = reader.buffer
        if fg:
            logger.debug('Loaded frame group - keys: %s, %s', fg.keys(), self.name)
        p_key = self.provide_main.get('key')
        self.context.store_frame_group(self.frame_group_name_in, fg)


class ReadFrameResourceTask(FrameResourceTask):
    """ Read a frame resource from frame cache"""
    def run(self):
        self.prepare()
        logger.debug("RFRT frame group name %s, group %s", self.frame_group_name_in, self.group)
        frame = self.fc.get_frame(self.frame_group_name_in, self.group)
        self.context[self.config['name']] = frame



        frame_group_name = self.args.get('frame_group_name', None) or self.group
        logger.debug("ReadFrameGroupResourceTask frame group name %s", frame_group_name)
        fg = self.fc.get_frame_group(frame_group_name)

        self.prepare()
        logger.debug("ReadFrameGroupResourceTask frame group name %s", self.frame_group_name_in)
        fg = self.fc.get_frame_group(self.frame_group_name_in)
        self.context[self.config['name']] = fg
        #XXX wrong


class DebugFrameResourceTask(FrameResourceTask):
    def run(self):
        self.prepare()

        # if no frame_group_name arg use group = corresponds to loop item
        frame_group_name = self.args.get('frame_group_name', None) or self.group
        logger.debug("DebugFrameGroupResourceTask frame group name %s", frame_group_name)
        fg = self.fc.get_frame_group(frame_group_name)
        logger.debug(frame.head(3))

class DebugFrameGroupResourceTask(FrameResourceTask):
    def run(self):
        self.prepare()
        logger.debug("DebugFrameGroupResourceTask frame group name %s", self.frame_group_name_in)
        fg = self.fc.get_frame_group(self.frame_group_name_in)
        for k, v in fg.items():
            logger.debug("frame %s: rows %d, cols %d", k, v.shape[0], v.shape[1])
            #logger.debug(v.head(3))


# XXX currently gets date from context , NOT from frame cache
# means output to file or db just one sheet
class WriteFrameResourceTask(FrameResourceTask):
    def __init__(self, *args):
        super().__init__(*args)
        self.writer = self.fc.get_writer(self.group, writer_type=self.args.get('writer_type', None))

    def run(self):
        self.prepare()
        group = self.group or self.item
        self.writer.set_dst(self.context['data_out_path'].joinpath(group))
        buffer = self.context.get_frame(group)

        if buffer is None:
            logger.error("WFRTask: %s no buffer in context for group %s", self.config['name'], group)
            return
        self.writer.set_buffer(group, buffer)
        self.writer.out_fns = [group]
        self.writer.init_writer_all()
        self.writer.write()
        logger.debug("writeFrameGroupTask wrote frame %s ", group)

# XXX we need to dismiss the password columns here before writing to file/db
class WriteFrameGroupResourceTask(FrameResourceTask):

    def run(self):
        self.prepare()
        # does not use the loopMixin logic to capture the input frame group data
        # this seems a fine solution
        frame_group_name = self.frame_group_name_out or self.item

        fg = self.fc.get_frame_group(frame_group_name)

        logger.debug('"%s", keys to write: %s', frame_group_name, fg.keys())
        self.fc.write_frame_group(frame_group_name)
        logger.debug("WriteFrameGroupResourceTask wrote frame group %s ", frame_group_name)


class ModifyFrameResourceTask(FrameResourceTask):

    def run(self):
        self.prepare()

        group = self.group or self.item
        frame = self.fc.get_frame(self.frame_group_name_in, group)
        logger.debug("ModifyFrameResourceTask modifying frame group %s, group %s", self.frame_group_name_in, group)


        self.fc.store_frame(self.frame_group_name_out, group, frame)
        logger.debug("ModifyFrameResourceTask stored modified frame for group %s", group)

    def update_rows(self):
        # Loop über DataFrame, jede Zeile als dict
        for i, row in self.df.iterrows():
            row_dict = row.to_dict()
            self.update_logic(row_dict)

        logger.debug("ModifyFrameResourceTask updated rows: %s", len(self.df))



        # here for now fetch the frames from framecache
        # even while the thought was to have them in context already
class MergeFrameResourceTask(FrameResourceTask):

    def __init__(self, *args):
        super().__init__(*args)

    def run(self):
        self.prepare()
        group = self.group or self.item

        in_items = self.args.get('in')
        logger.debug('Merging framegroups: %s , here group %s', in_items, group)
        in1 = in_items[0]
        in2 = in_items[1]

        logger.debug('self.df_in keys: %s', self.df_in.keys())
        df1 = self.df_in[in1][group]
        df2 = self.df_in[in2][group]

        # somehow the ent_old_tag_xx is subcatted by case_name., NOT groups_old
        # self.business_logic()

        msg = 'frame to merge is None: '
        if df1 is None:
            logger.error(msg+'F1')
            raise ValueError(msg+'F1')
        if df2 is None:
            logger.error(msg+'F2')
            raise ValueError(msg+'F2')
        logger.debug('len df1: %s', len(df1))
        logger.debug('len df2: %s', len(df2))

        foreign_key = 'item'
        on = self.args.get('on', [foreign_key])
        how = self.args.get('how', 'outer')
        suffixes = self.args.get('suffixes', ('_left', '_right'))
        logger.debug('Merging on %s, how=%s, suffixes=%s', on, how, suffixes)
        dfm = pd.merge(df1, df2, on=on, how=how, suffixes=suffixes)
        dfm = dfm.fillna('')
        logger.debug('len df merged: %s', len(dfm))
        #self.context.store_frame(self.frame_group_name_out, group, dfm)
        self.save_frame_to_tmp_d(dfm, group)

class TransformFrameResourceTask(TransformMixin, FrameResourceTask):

    def __init__(self, *args):
        super().__init__(*args)

    def run(self):
        self.prepare()

        group = self.group or self.item

        df = self.context.get_frame(self.frame_group_name_in, group)
        transform = self.args.get('transform', None)
        if transform is None:
            return df

        cfg_key = 'kp_transform_'+self.frame_group_name_in
        cfg = self.context['config_d'][cfg_key]
        df = self.apply_transformations(df, cfg)
        p_key = self.provide_main.get('key')
        self.context.store_frame(self.frame_group_name_out, group, df)
        logger.debug("TransformFrameResourceTask stored transformed frame for group %s", group)


class TransformFrameGroupResourceTask(TransformMixin, FrameResourceTask):
    def __init__(self, *args):
        super().__init__(*args)

    def run(self):
        self.prepare()

        cfg_key = 'kp_transform_'+frame_group_name
        log_context(self.context, 'TransformFrameGroupResourceTask run')

        self.prepare()
        if fg is None:
            logger.error("TransformFGRTask: no frame group %s in context ", self.frame_group_name_in)
            return
        cfg_key = 'kp_transform_'+self.frame_group_name_in
        if p_key not in self.context:
            self.context[p_key] = {}
        self.context[p_key] = fg
        logger.debug("TransformFGRTask stored transf frame group for name %s", frame_group_name)
        logger.debug("TransformFrameGroupResourceTask transforming frame group %s, group %s", self.frame_group_name_in, group)
        df = self.apply_transformations(df, cfg)
        fg[group] = df
        self.context.store_frame_group(self.frame_group_name_out, fg)
        logger.debug("TransformFGRTask stored transf frame group for name %s", self.frame_group_name_out)

