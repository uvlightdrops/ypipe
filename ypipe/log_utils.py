from .context import Context
import logging
from flowpy.utils import setup_logger

logger = setup_logger(__name__, __name__ + '.log', level=logging.DEBUG)


def _load_context_keys():
    try:
        from .context_keys import context_keys
        return context_keys
    except Exception:
        # fallback minimal keys to avoid hard failure if pipeline import is not ready
        return {
            'obj': {'fc', 'app', 'storage_broker', 'storage_cache'},
            'cfg': {'config_d'},
        }


def log_context(context, msg, show_subkeys=False):
    mlen = 45
    if len(msg) <= mlen:
        msg = msg + ' ' * (mlen - len(msg))
    else:
        msg = msg[:mlen]
    context_keys = _load_context_keys()
    keys_obj = set(context_keys.get('obj', []))
    keys_cfg = set(context_keys.get('cfg', []))
    keys_path = set(context_keys.get('path', []))
    keys_result = set(context_keys.get('result', []))
    keys_fc = set(context_keys.get('fc', []))
    keys_meta = set(context_keys.get('meta', []))

    keys_hide = keys_obj  | keys_path | keys_result | keys_fc | keys_cfg | keys_meta
    keys_hide = keys_hide.union({'kp_src'})
    #logger.debug('---> %s ctx %s', msg, [k for k in context.keys() if k not in keys_hide])
    logger.debug('----------- %s ==== %s', type(context), msg.strip())
    for k in context.keys():
        if k not in keys_hide:
            logger.debug('%s:      %s', k, type(context[k]))
            if type(context[k]) == type({}) and show_subkeys:
                logger.debug('     keys: %s', context[k].keys())
    for item in ['frames', 'frame_groups']:
        logger.debug(' %s keys: %s', item, context[item].keys())
    logger.debug('loop item: %s', context.get('loop_item', None))

    if not isinstance(context, Context):
        raise TypeError("context must be a Context instance, not dict")
