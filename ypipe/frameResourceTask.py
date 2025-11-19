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
        self.fg_accumulate = True
        self.df_tmp = {}
        self.df_tmp2 = {}
        # new dict of tmp framegroups
        self.df_tmp_d = {}

        #logger.debug("FrameResourceTask init")
        # FrameResourceTask special
        self.fc = self.context.get('fc')
        self.frame_group_name_in = self.args.get('frame_group_name_in', None)
        self.frame_group_name_out = self.args.get('frame_group_name_out', None)
        self.group = self.args.get('group', None)
        #logger.debug("FRT init frame_group_name_in: %s, frame_group_name_out: %s, group: %s",
                     #self.frame_group_name_in, self.frame_group_name_out, self.group)

    def run(self):
        logger.warning("FrameResourceTask OR should run subclass run()??")


    def save_frame_to_df_tmp_d(self, df, tkey, case_name):
        # store frame in dict of framegroups
        #df = self.drop_cols(df)
        #logger.debug("%s save F%s  %s %s", self.config['name'], df.shape, tkey, case_name)
        if tkey not in self.df_tmp_d:
            self.df_tmp_d[tkey] = {}
        self.df_tmp_d[tkey][case_name] = df
        #logger.debug("-- CHECK df_tmp_d keys: %s", self.df_tmp_d.keys())
    # not sure if this is useful
    def save_frame_to_tmp_d(self, df, case_name):
        # tkey bestimmen
        logger.debug('frame_group_name_out: %s', self.frame_group_name_out)
        self.save_frame_to_df_tmp_d(df, self.frame_group_name_out, case_name)
        return
        df = self.drop_cols(df)
        logger.debug("%s save fr to df_tmp, shape %s for  %s", self.config['name'], df.shape, case_name)
        self.df_tmp[case_name] = df

    """
    def save_frame_to_tmp2_d(self, df, case_name):
        self.save_frame_to_df_tmp_d(df, self.args['out'][1], case_name)
        return
        #df = self.drop_cols(df)
        logger.debug("saving fr to df_tmp2, shape %s for  %s", df.shape, case_name)
        self.df_tmp2[case_name] = df
    """

    def drop_cols(self, df):
        drop_field_l = self.fc.frame_fields.get('drop_'+self.frame_group_name_out+'_table', None)
        #logger.debug("drop cols FG %s: %s", self.frame_group_name_out, drop_field_l)
        if drop_field_l:
            df = df.drop(columns=drop_field_l)
            logger.debug("dropped cols FG %s: %s", self.frame_group_name_out, drop_field_l)
        drop_field_cfg = self.args.get('drop_fields',  None)
        if drop_field_cfg:
            df = df.drop(columns=drop_field_cfg)
            logger.debug("dropped cols FG %s from cfg: %s", self.frame_group_name_out, drop_field_cfg)
        return df



class StoreFrameGroupResourceTask(FrameResourceTask):
    # Store a frame group resource to frame cache
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
        logger.debug("LoadFrameResourceTask read %s for group %s", fn, group)
        df = reader.get_buffer(fn)
        # not useful we have storageCache
        #self.context.store_item(group, reader)
        #logger.debug(df.tail(10))
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
    def __init__(self, *args):
        super().__init__(*args)
        self.fg_accumulate = False

    def run(self):
        self.prepare()
        # does not use the loopMixin logic to capture the input frame group data
        # this seems a fine solution
        frame_group_name = self.frame_group_name_out or self.item

        fg = self.fc.get_frame_group(frame_group_name)

        self.fc.write_frame_group(frame_group_name)
        logger.debug("WFGRT wrote fg %s ", frame_group_name)
        #logger.debug("WFGRT wrote fg %s keys %s", frame_group_name, fg.keys())


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
        item = self.group or self.item

        in_items = self.args.get('in')
        in1 = in_items[0]
        in2 = in_items[1]

        # dont need to pass case_name, we have self.item
        gid_1, gid_2 = self.business_logic(item)
        logger.debug('Merging framegroups: %s and %s', in1, in2)
        logger.debug('here case %s with group %s', gid_1, gid_2)
        df1 = self.df_in[in1][gid_1]
        df2 = self.df_in[in2][gid_2]

        msg = 'frame to merge is None: '
        if df1 is None:
            logger.error(msg+'F1')
            raise ValueError(msg+'F1')
        if df2 is None:
            logger.error(msg+'F2')
            raise ValueError(msg+'F2')

        #foreign_key = 'item'
        foreign_key = 'role_index'
        key_on = self.args.get('key_on')
        how = self.args.get('how')
        suffixes = self.args.get('suffixes', ('_left', '_right'))
        #logger.debug('Merging on %s, how=%s, suffixes=%s', key_on, how, suffixes)
        # df1-wanted is left master frame and we join the entries_old from df2 into it
        dfm = pd.merge(df1, df2, on=key_on, how=how, suffixes=suffixes)
        dfm = dfm.fillna('')
        #logger.debug('Merged frame has columns: %s', dfm.columns)
        logger.debug('len df merged: %s | of %s -- %s ', len(dfm), len(df1), len(df2))
        #self.save_frame_to_tmp_d(dfm, item)
        self.save_frame_to_df_tmp_d(dfm, 'main', item)
        # I need to save the non-matching rows of df2 too
        if how in ('left', 'outer'):
            unmatched = pd.merge(df1, df2, on=key_on, how='right', suffixes=suffixes, indicator=True)
            unmatched = unmatched[unmatched['_merge'] == 'right_only']

            #df2_notin_df1 = df2[~df2[foreign_key].isin(df1[foreign_key])]
            logger.debug('len df2 not in df1: %s', len(unmatched))
            self.save_frame_to_df_tmp_d(unmatched, 'non_matching', item)


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

