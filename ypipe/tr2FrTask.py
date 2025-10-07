from task import Task
from loopMixin import LoopMixin
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

        provide = self.config['provides'][0]
        logger.debug('providing %s', provide)
        self.context[provide] = df
        self.context['result'] = df
        #return df


class CreateFrameTask(Task):
    def __init__(self, *args):
        super().__init__(*args)



import string
#class PermutationsCreateFrameTask(LoopMixin, CreateFrameTask):
class PermutationsCreateFrameTask(LoopMixin, Task):

    def __init__(self, *args):
        logger.debug('___Entering. __init__ %s ', self.__class__.__name__)
        super().__init__(*args)
        self.cfg_kp_wanted_logic = self.context['fc'].cfg_kp_wanted_logic
        self.frame_fields = self.context['fc'].frame_fields
        self.cfg_age = self.context['app'].cfg_age
        #self.broker = self.context['app'].broker
        self.reader = self.context['fc'].get_reader('wanted')
        self.reader.set_src_dir(self.context['app'].cfg_si['data_in_sub'])
        self.count = 0
        self.count_suc = 0
        self.stats_init()
        self.df_d = self.context['fc'].df_d

    def prepare(self):
        logger.debug('using loop_item from context: %s', self.context['loop_item'])
        if self.context.get('loop_item', None):
            self.group = self.context['loop_item']
        else:
            self.group = self.config['args']['group']

    def run(self):
        # ie get current loop item from context
        self.prepare()

        group_name_new = self.group
        case_name = group_name_new
        group_logic = self.cfg_kp_wanted_logic['groups'].get(group_name_new)
        #logger.debug('group_logic: %s', group_logic)
        self.group_generate_wanted_table(group_name_new, case_name, group_logic)


    def group_generate_wanted_table(self, group_name_new, case_name, group_logic):
        # dev XXX
        group_name_old = group_logic.get('src_group')

        fieldnames = self.frame_fields['wanted_table']
        df = pd.DataFrame(columns=fieldnames)
        generate_wanted = group_logic.get('generate_wanted', True)
        if generate_wanted == False:
            self.df_d['wanted'][case_name] = df
            return

        items = group_logic.get('items')
        # lg.debug('items: %s', items)
        # itemmap = group_logic.get('map') #get or {}
        app = group_logic.get('app')
        dst = group_logic.get('dst')
        group_path_new = [group_name_new]
        lg.debug('case_name: %s => group_path_new: %s', case_name, group_path_new)
        # dst is a optional destination subgroup
        age_pattern = group_logic.get('age')
        age_template = string.Template(age_pattern)
        sub_all = group_logic.get('sub_all')
        attrs_new_pat_d = group_logic.get('attrs_new_pat_d')
        # lg.debug('attrs_new_pat_d: %s', attrs_new_pat_d)
        behoerde = ''
        self.stats_init()
        for crit in self.cfg_age['env']:
            lvlrow = {
                'crit': crit,
                'app': app,
                'group_new': group_name_new,
                'group_old': group_name_old,
                #'group_old': group_obj_old.name,
                'status': 'NEW',
            }
            for item in items:
                row = lvlrow.copy()
                for sub in sub_all:
                    if group_logic['loop_gericht']:
                        behoerde = sub
                    # logger.debug('sub: %s', sub)
                    roledict = {'app': app, 'behoerde': behoerde, 'crit': crit}
                    role = age_template.substitute(roledict)
                    #logger.debug('role: %s', role)
                    #hostname_list = self.broker.call_method('get_data_for_one', role)
                    hostname_list = self.reader.read_sql('abc_id', role)['hostname']
                    #logger.debug('role: %s, hnl: %s', role, hostname_list)
                    if hostname_list is not None and len(hostname_list) > 0:
                        #row['hostname'] = hostname_list[0]  # XXX this is not exact, info is lost
                        row['hostname'] = hostname_list[0]
                        row['vm'] = row['hostname'][7:]
                    else:
                        row['hostname'] = 'host-NA'
                        row['vm'] = 'vmname-NA'
                    row['hostname_list'] = hostname_list
                    row['behoerde'] = sub  # XXX use behoerde here
                    row['item'] = item
                    # row['mapped'] = itemmap.get(item, item)
                    row['fk'] = self.count
                    row['role_index'] = '%s_%s' % (role, item)
                    # logger.debug('role_index: %s', row['role_index'])
                    # row['title_new'] = '%s %s' % (crit, item)
                    for key, anp_item in attrs_new_pat_d.items():
                        # logger.debug('check-item: %s -- key: %s', item, key)
                        if 'all_items' in attrs_new_pat_d[key].keys():
                            pattern = attrs_new_pat_d[key]['all_items']
                        else:
                            pattern = anp_item.get(item, '')
                        row[key] = string.Template(pattern).substitute(row)
                        # row[key+'_new'] = string.Template(pattern).substitute(row)

                    # for bmarks_eip
                    if group_logic.get('use_subgroups', None):
                        group_path_new = [group_name_new, sub]
                    else:
                        if dst:
                            group_path_new = [group_name_new, dst]

                    # logger.debug('group_path_new: %s', group_path_new)
                    row['group_path_new'] = group_path_new
                    self.count += 1
                    self.count_suc += 1
                    ldf = len(df)
                    df.loc[ldf] = row
        self.stats_report(name='30_generate_wanted_table_' + case_name)
        # df_wanted = self.df_wanted[case_name].copy()
        # Apply the function to each row and create a new column
        # XXX outcomment for debugging the other call

        # self.stats_init()
        # 03-17 , calc role_index in loop
        # df['role_index'] = df.fillna('').apply(self.calc_role_index, sig='', axis=1)
        # self.stats_report(name='33_wanted_calc_roleindex_'+case_name)

        # df_wanted = self.df_wanted[case_name].copy()
        self.df_d['wanted'][case_name] = df.sort_values(by='role_index')  # df.fillna('')
        self.context['fc'].buffer_names_d['wanted'][case_name] = case_name
        lg.debug('len df wanted: %s', len(self.df_d['wanted'][case_name]))
