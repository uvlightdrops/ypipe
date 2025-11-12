from .task import Task
from flowpy.utils import setup_logger, log_memory_usage
logger = setup_logger(__name__, __name__+'.log')


# this is essentially a StorageResourceTask ? XXX
class ReaderTask(Task):
    def __init__(self, *args):
        super().__init__(*args)
        self.sc = self.context['storage_cache']
        self.fc = self.context['fc']


# DbStorageResourceTask ? XXX
class DbReaderTask(ReaderTask):
    def __init__(self, *args):
        super().__init__(*args)
        type = self.config['args']['type']
        self.resource = self.sc.get_resource(self.config['args']['in'], type=type, rws='r')
        self.resource.set_src_dir(self.context['data_in_path'])
        self.resource.set_outfiles( [self.args['fn']] )

    def run(self):
        self.resource.read(self.args['fn'])
        self.context[self.provide_main['key']] = self.resource.df


# I need to refactor this; We have FC which holds the single_readers ,
# Can we read with them?

class SingleReaderTask(ReaderTask):
    def __init__(self, *args):
        super().__init__(*args)
        # gets a reader for a singe frame

    def run(self):
        reader = self.fc.get_reader(self.args['in'], reader_type=self.args.get('reader_type', None))
        reader.set_src_dir(self.context['data_in_path'])
        reader.read(self.args['fn'])

        df = reader.get_buffer(self.args['fn'])
        logger.debug('SingleReaderTask read df shape %s', df.shape)
        self.context[self.provide_main['key']] = df
