from .task import Task
from .loopMixin import LoopMixin
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

        self.fn = self.context['data_path'].joinpath(self.args.get('fn', ''))
        self.type = self.args.get('type', None)
        self.ctx_key = self.args.get('ctx_key', None)

    def get_resource(self):
        return self.resource

    def run(self):
        logger.debug('SPARE run')
        pass
        #return self.resource

# was is mit config resourcen als weitere kategorie?
# Das würde evtl schon Sinn machen. Es sind jeweils yaml string und geparsed
# dann natürlich dicts von xyz.
# zb create_tree_from_yaml mit input aus config resourcen
# Andererseits ist es ja auch nur ein spezieller Fall von args
# und könnte auch so gehandhabt werden.
# Vielleicht ist es übersichtlicher, wenn es eine eigene Kategorie ist.
# Es ist ja eigentlich auch etwas dass gecached vorgehalten werden sollte.


class StorageResourceTask(ResourceTask):
    """ A Resource with tree like behaviour to store data hierarchical
    """
    def __init__(self, *args):
        super().__init__(*args)
        # XXX move vars to ResourceTask? init
        # StorageResourceTask special

    def fetch(self):
        resource = self.context.get(self.ctx_key, None)
        if not resource:
            logger.error("ModifyStorageResourceTask: resource %s not in context", self.name)
            resource = self.sc.get_resource(self.req[0], type=self.type)
            logger.debug("ModifyStorageResourceTask: resource %s found in cache, modify", resource)
        self.resource = resource

    def run(self):
        #name = self.config['name']
        creds_file = self.args.get('creds_file', None)
        if creds_file:
            #pw = open(self.context['config_dir'].joinpath(creds_file)).read().strip()
            pw = open(self.context['project_dir'].joinpath(creds_file)).read().strip()
        logger.debug('res %s type %s from %s', self.name, self.type, self.fn)
        #logger.debug('pw from file %s: %s', creds_file, pw)
        # capsulate pw in new kwargs
        kwargs = {'pw': pw}
        # key of cache is name of resource, only here
        resource = self.sc.get_resource(self.name, type=self.type, **kwargs)
        # random
        resource.src_or_dst = 'src'
        resource.set_src(self.fn)
        # Das hier ist net am rechten platz, sollte temporär nur für run methode nötig sein
        self.context[self.name] = resource
        self.resource = resource


class ModifyStorageResourceTask(StorageResourceTask):
    def __init__(self, *args):
        super().__init__(*args)
        self.data = self.args.get('fn_data', None)
        self.yml_key = self.args.get('yml_key', None)

    def run(self):
        self.fetch()
        yml = self.config_d[self.yml_key]
        #logger.debug('yml: %s', yml)
        attrs = self.config_d['kp_process_fields']['kp_same_fields']

        self.resource.create_tree_from_yaml(yml, attrs)
        self.resource.generate_pykeepass_tree()

        logger.debug(self.resource.groups)
        self.context[self.name] = self.resource


class WriteStorageResourceTask(StorageResourceTask):
    def __init__(self, *args):
        super().__init__(*args)
    def run(self):
        self.fetch()
        self.resource.do_save()
        logger.debug("WriteStorageResourceTask: resource %s saved", self.resource)


class FrameResourceTask(LoopMixin, ResourceTask):
    """ Provides a frame resource from frame cache"""
    def __init__(self, *args):
        super().__init__(*args)
        # FrameResourceTask special
        self.frame_group = self.args['frame_group']
        self.group = self.args.get('group', None)

    def run(self):
        self.prepare()
        self.resource = self.context['fc'].get_frame(self.frame_group, self.group)

        self.context[ self.config['name'] ] = self.resource


class StoreFrameResourceTask(FrameResourceTask):
    """ Store a frame resource to frame cache"""
    def run(self):
        self.prepare()

        # d.h. ganze framegroup speichern
        if self.config.get('frame_group_d', None):
            fg_d = self.config['frame_group_d']
            self.context['fc'].store_frame_group(self.frame_group, )
            logger.debug("StoreFrameResourceTask also stored frame %s in frame group %s", frame_name, v)
        # XXX return success flag? and store in context?
        else:
            frame = self.context[self.config['args']['in']]
            self.context['fc'].store_frame(self.frame_group, self.group, frame)
            self.context[self.config['name']] = frame


class ReadFrameResourceTask(FrameResourceTask):
    """ Read a frame resource from frame cache"""
    def run(self):
        frame_group = self.config['args']['frame_group']
        group = self.config['args'].get('group', None)
        self.frame = self.context['fc'].get_frame(frame_group, group)
        self.context[self.config['name']] = self.frame
        #return self.frame


class DebugFrameResourceTask(FrameResourceTask):
    def run(self):
        frame_group = self.config['args']['frame_group']
        group = self.config['args'].get('group', None)
        self.frame = self.context['fc'].get_frame(frame_group, group)
        #logger.debug(type(self.frame[group]))
        #logger.debug(self.frame[group].head(3))
        #return self.frame


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
