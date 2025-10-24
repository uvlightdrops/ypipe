from .resourceTask import ResourceTask
from .loopMixin import LoopMixin
from flowpy.utils import setup_logger
import pandas as pd
logger = setup_logger(__name__, __name__+'.log')
from .utils import log_context


def run_modify_and_register(self):
    # self.resource ist das aktuell geladene ResourceWrapper-Objekt (oder rohes Objekt)
    # erzeugen einer neuen Version (immutable/versioned approach)
    if isinstance(self.resource, ResourceWrapper):
        new_res = self.resource.clone()
    else:
        # falls noch kein Wrapper verwendet wird, wrap + clone
        new_res = ResourceWrapper(self.resource, self.provides[0]).clone()

    # perform modifications an new_res.inner ...
    # z.B. new_res.inner.modify(...)

    # neuen key erzeugen und in context sowie (optional) storage_cache speichern
    new_key = f"{self.provides[0]}_mod"  # oder f"{self.provides[0]}_v{new_res.version}"
    self.context[new_key] = new_res
    # falls StorageCache genutzt wird, registriere neue Version dort (erweitere API ggf.)
    try:
        self.sc._cache[new_key] = new_res
    except Exception:
        logger.debug("StorageCache direct write failed - erwäge API zum registrieren einer neuen resource")

    # optional: als provides den neuen key deklarieren (oder in task-config angeben)
    logger.info("Registered modified resource under context key %s", new_key)



class StorageResourceTask(ResourceTask):
    """ A Resource with tree like behaviour to store data hierarchical
    """
    def __init__(self, *args):
        super().__init__(*args)
        # XXX move vars to ResourceTask? init
        # StorageResourceTask special

    # XXX fetch a resource from context or cache, change to fetch_sth...
    # used in Modify and Write tasks
    def fetch(self, *args, **kwargs):
        resource = self.context.get(self.ctx_key, None)

        logger.debug("SRT - fetch resource from context key %s: %s", self.ctx_key, resource)
        if not resource:
            # generic resource loading / creating in StorageBroker if not exist
            logger.error("SRT - resource %s not in context", self.ctx_key)
            resource = self.sc.get_resource(self.req_resources[0], *args, type=self.type, **kwargs)
            logger.debug("SRT - resource %s found in cache, assign to self", resource)
        self.resource = resource

    def run(self):
        # resource not fetched here but created from start
        log_context(self.context, 'StorageResourceTask.run')
        #name = self.config['name']
        creds_file = self.args.get('creds_file', None)
        if creds_file:
            #pw = open(self.context['config_dir'].joinpath(creds_file)).read().strip()
            pw = open(self.context['project_dir'].joinpath(creds_file)).read().strip()
        logger.debug('SRTR - res %s type %s from %s', self.name, self.type, self.fn)
        #logger.debug('pw from file %s: %s', creds_file, pw)
        # capsulate pw in new kwargs
        kwargs = {'pw': pw, } #'filename': self.f}
        # key of cache is name of resource, only here
        resource = self.sc.get_resource(self.name, type=self.type, **kwargs)
        sod = self.args.get('src_or_dst', 'src')

        sod_path_key = 'data_in_path' if sod == 'src' else 'data_out_path'
        path = self.context[sod_path_key].joinpath(self.fn)
        resource.set_src(path)
        resource.src_or_dst = sod

        # Das hier ist net am rechten platz, sollte temporär nur für run methode nötig sein
        logger.debug("SRT - resource assign to context key %s", self.provides[0])
        self.context[self.provides[0]] = resource
        logger.debug("SRT - task %s provides resource %s ", self.name, resource)
        if self.type == 'kdbx':
            logger.debug("SRT - len groups: %s", len(resource.groups))
        self.resource = resource


class ModifyStorageResourceTask(StorageResourceTask):
    def __init__(self, *args):
        super().__init__(*args)
        self.data = self.args.get('fn_data', None)
        self.yml_key = self.args.get('yml_key', None)

    def run(self):
        self.fetch(filename=self.fn)

        backup = self.config.get('backup', None)
        if backup:
            # XXX make a copy not only a reference
            # but not for production use
            self.context[backup[0]] = self.resource

        yml = self.context['config_d'][self.yml_key]
        #logger.debug('yml: %s', yml)
        attrs = self.context['config_d']['kp_process_fields']['kp_same_fields']
        self.resource.create_tree_from_yaml(yml, attrs)

        self.resource.generate_pykeepass_tree()

        #logger.debug(self.resource.groups)
        self.context[self.provides[0]] = self.resource
        print(self.context[backup[0]] == self.resource)


class WriteStorageResourceTask(StorageResourceTask):
    def __init__(self, *args):
        super().__init__(*args)

    def run(self):
        self.fetch(filename=self.fn)
        self.resource.do_save()
        logger.debug("WriteStorageResourceTask: resource %s saved", self.resource)


class CopyStorageDataTask(LoopMixin, StorageResourceTask):
    def __init__(self, *args):
        super().__init__(*args)
        self.kp_src = self.context['kp_src']
        self.kp_dst = self.context['kp_dst']
        self.loop_copy_bypath = self.args.get('loop_copy_bypath', [])
        #logger.debug(self.kp_src.groups)

    def run(self):
        # some tasks use framecache even if they belong to storage taks category
        self.prepare()

        fc = self.context['fc']
        self.prepare()
        item = self.item

        p = ['DB', 'Produktiv']
        group_src = self.kp_src._find_group_by_path(p)
        #group_src = self.kp_src._find_group_by_path(item['src'])
        group_dst = self.kp_dst._find_group_by_path(item['dst'])
        if not group_src:
            logger.error('group_src not found: %s', item['src'])
        if not group_dst:
            logger.error('group_dst not found: %s', item['dst'])
        logger.debug("group_src: %s, group_dst: %s", group_src.path, group_dst.path)

        # self.group_do_entries_copyall(group_src, group_dst)

        logger.debug('entries count: %s', len(group_src.entries))
        table_name = 'entries_raw_table'
        kp_process_fields = self.context['config_d']['kp_process_fields']
        df = pd.DataFrame(columns=kp_process_fields[table_name])
        self.stats_init()
        attrs = (kp_process_fields['kp_pure_fields'] +
                 kp_process_fields['kp_same_fields'] +
                 kp_process_fields['kp_extra_fields'] )
        # lg.debug('attrs: %s', attrs)
        # XXX use dataframe to update the table
        for entry in group_src.entries:
        #for entry in group_src.children:
            row = {}
            for attr in attrs:
                if attr.endswith('_old'):
                    row[attr] = getattr(entry, attr[:-4])
                else:
                    row[attr] = getattr(entry, attr)
            row['group_path_new'] = group_dst.path

            ldf = len(df)
            df.loc[ldf] = row
            self.count += 1
        dt_fields = kp_process_fields.get('dt_fields', [])
        if dt_fields is None:
            dt_fields = []
        for dt_field in dt_fields:
            df[dt_field] = df[dt_field].dt.tz_localize(None)

        fc.store_frame('copyall', group_dst.name, df)
        # not needed is done in store_frame
        #fc.buffer_names_d['copyall'][group_dst.name] = group_dst.name
        #self.stats_report(name='copyall_'+group_dst.name)
####
