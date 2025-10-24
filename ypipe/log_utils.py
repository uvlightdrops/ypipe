
import logging
from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log', level=logging.DEBUG)




def log_context(context, msg):
    if len(msg) <= 20:
        msg = msg + ' ' * (20 - len(msg))
    else:
        msg = msg[:20]
    keys_hide = set(['result', 'fc', 'app', 'storage_broker', 'storage_cache', 'data_path', 'project_dir', 'config_dir',
                     'master_config_dir', 'config_d'])
    #logger.debug('---> %s ctx %s', msg, [k for k in context.keys() if k not in keys_hide] )

