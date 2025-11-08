from .task import Task
from flowpy.utils import setup_logger, log_memory_usage
logger = setup_logger(__name__, __name__+'.log')



class ReaderTask(Task):
    def __init__(self, *args):
        super().__init__(*args)
        self.sc = self.context['storage_cache']



class DbReaderTask(ReaderTask):
    def __init__(self, *args):
        super().__init__(*args)
        type = self.config['args']['type']
        self.resource = self.sc.get_resource(self.config['args']['in'], type=type, rws='r')
        self.resource.set_src_dir(self.context['data_in_path'])

    def run(self):
        self.resource.read(self.args['fn'])
        self.context[self.provides['main']['key']] = self.resource
