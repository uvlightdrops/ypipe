from task import Task
from flowpy.utils import setup_logger, log_memory_usage
logger = setup_logger(__name__, __name__+'.log')
lg = logger
import pandas as pd


class Tr2FrTask(Task):
    def __init__(self, *args):
        super().__init__(*args)

    def run(self):
        pass

class DumpGroupTask(Tr2FrTask):

    def __init__(self, *args):
        super().__init__(*args)
        app = self.context['app']
        self.kp_src = self.context['kp_src']
        #logger.debug('self.kp_src: %s', self.kp_src)

        self.cfg_kp_process_fields = app.cfg_kp_process_fields
        self.cfg_kp_wanted_logic = app.cfg_kp_wanted_logic
        self.buffer_names_d = app.buffer_names_d
        # XXX check for memory use
        self.frame_fields = self.context['fc'].frame_fields
        self.count = 0

    def run(self):
        app = self.context['app']
        table_def_name = 'entries_old' + '_table'  # XXX check out legacy
        #logger.debug('self.frame_fields keys: %s', self.frame_fields.keys())
        self.frame_fields_cur = self.frame_fields[table_def_name]
        group_name_old = self.config['args']['group']
        # group_name_old is meanwhile rather a group identificator, can be a list / path (useful?)
        kp_pf = self.cfg_kp_process_fields
        logger.debug('___Entering. group_name_old: %s', group_name_old)

        group_obj_old = self.kp_src._find_group_by_path(group_name_old)
        #logger.debug('group_obj_old: %s', group_obj_old)
        if group_obj_old is None:
            logger.error('find groups returns None for %s', group_name_old)
            return  # skip this groupname
        #logger.debug(log_memory_usage())
        # attrs = kp_pf['kp_old_fields'] + kp_pf['kp_same_fields']
        # All attributes of ususal keepass entry
        attrs_entries_old = kp_pf['kp_pure_fields'] + kp_pf['kp_same_fields'] + kp_pf['kp_extra_fields']
        df_entries = pd.DataFrame(columns=attrs_entries_old)
        # logger.debug('df_entries cols: %s', df_entries.columns)
        df_entries_tagged = pd.DataFrame(columns=self.frame_fields_cur)

        entries = group_obj_old.entries
        lg.debug('len entries: %s', len(entries))

        for entry in entries:
            row = {}
            row['status'] = 'NEW'
            row['status_info'] = ''
            # logger.debug('entry title: %s', entry.title)
            # discover termn in all entries of current group
            row_tagged = app.check_entry_for_prominent_terms(entry, row)
            # row_tagged['sig_app'] = role_prefix
            # XXX can check_entry_for_prominent_terms be after the old val assertion?

            # XXX not very exact
            # get old attributes, their original names of course miss the _old suffix
            for attr in self.cfg_kp_process_fields['kp_old_fields']:
                row[attr] = getattr(entry, attr[:-4])
                # logger.debug('OLD attr: %s, val: %s', attr, row[attr])

            for attr in attrs_entries_old:
                row[attr] = getattr(entry, attr)

            if 'path_old' in row:
                row['path_old'] = str(row['path_old'])  # ahy? XXX dont need

            row['pk'] = self.count
            self.count += 1
            ldf = len(df_entries)

            df_entries.loc[ldf] = row
            df_entries_tagged.loc[ldf] = row_tagged

        #self.app.stats_report(name='10_dump_'+group_name_old)

        ## df_entries = df_entries.sort_values(by=self.cfg_kp_wanted_logic['sort'])
        df = df_entries.fillna('')
        logger.debug('len df: %s', len(df))

        self.context[self.config['provides'][0]] = df
        return df
