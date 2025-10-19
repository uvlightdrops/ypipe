
from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class LoopMixin:
    """
    def run_with_loop(self):
        results = []
        items = self.config.get('loop_items', [])
        for item in items:
            self.context['loop_item'] = item
            results.append(self.run())
        return results
    """

    ## XXX rename to prepare_loop or so
    def prepare(self, *args, **kwargs):
        #logger.debug("LoopMixin.prepare args: %s, kwargs: %s", args
        logger.debug('using loop_item from context: %s', self.context['loop_item'])
        if self.context.get('loop_item', None):
            self.group = self.context['loop_item']
        else:
            self.group = self.config['args']['group']

    def run_with_loop(self):
        results_d = {}
        # XXX relate to inits from base classes later
        items = self.config.get('loop_items', [])
        logger.debug("LoopMixin.run_with_loop items: %s", items)
        provide_dict = self.config.get('provide_dict', False)

        if self.provides:
            provide = self.provides[0]
            self.context[provide] = None

        if provide_dict:
            self.context[provide + '_d'] = {}

        if not items:
            logger.error("No loop_items defined in config for task %s", self.config.get('name', 'unknown'))

        for item in items:
            self.item = item
            self.context['loop_item'] = item
            logger.debug("call run - and loop_item is %s", item)

            self.run()

            if provide_dict:
                self.context[provide+'_d'][item] = self.context['result']
                logger.debug("Storing result in context[%s][%s]", provide+'_d', item)

                """
                for p in provides:
                    logger.debug("Storing stub for provided %s in context", p)
                    self.context[p] = 'Stub'
                """
            # XXX useless ? or at beinning?
            self.context['result'] = None

        logger.debug(self.context['loop_item'])
        #self.context.pop('loop_item', None)
