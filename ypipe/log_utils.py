import logging
from flowpy.utils import setup_logger

logger = setup_logger(__name__, __name__ + '.log', level=logging.DEBUG)


def _load_context_keys():
    try:
        from .pipeline import context_keys
        return context_keys
    except Exception:
        # fallback minimal keys to avoid hard failure if pipeline import is not ready
        return {
            'obj': {'fc', 'app', 'storage_broker', 'storage_cache'},
            'cfg': {'config_d'},
        }


def log_context(context, msg):
    mlen = 45
    if len(msg) <= mlen:
        msg = msg + ' ' * (mlen - len(msg))
    else:
        msg = msg[:mlen]
    context_keys = _load_context_keys()
    keys_obj = set(context_keys.get('obj', []))
    keys_cfg = set(context_keys.get('cfg', []))
    keys_path = set(context_keys.get('path', []))

    keys_hide = keys_obj | keys_cfg | keys_path
    logger.debug('---> %s ctx %s', msg, [k for k in context.keys() if k not in keys_hide])
