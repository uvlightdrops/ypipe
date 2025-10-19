from .task import Task
from .loopMixin import LoopMixin
from flowpy.utils import setup_logger
import pandas as pd

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

        self.fn = self.args.get('fn', '')
        self.path = self.context['data_in_path'].joinpath(self.fn)
        self.type = self.args.get('type', None)
        self.ctx_key = self.args.get('ctx_key', None)

        self.count = 0

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

