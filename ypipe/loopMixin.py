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

    ## XXX rename to prepare_loop_step or so
    def prepare(self, *args, **kwargs):
        #logger.debug("LoopMixin.prepare args: %s, kwargs: %s", args
        if self.context.get('loop_item', None):
            # XXX not nice, get rid of it
            self.group = self.context['loop_item']
            # DONT
            #self.item = self.context['loop_item']
            #logger.debug('%s using loop_item from ctx: %s', self.name, self.context['loop_item'])
        else:
            # non-looping task
            self.group = self.args.get('group', None)

    def run_with_loop(self):
        # XXX relate to inits from base classes later
        items = self.config.get('loop_items', [])
        if not items:
            logger.error("No loop_items defined in config for task %s", self.config.get('name', 'unknown'))

        logger.debug("superclass: %s", self.__class__.__bases__[-1].__name__)
        #logger.debug("items: %s", items)

        #logger.debug("frame_group_name_in: %s", self.frame_group_name_in)
        if self.frame_group_name_in:
            self.df_d = self.context.get_frame_group(self.frame_group_name_in)
            logger.debug("%s Initial df_d keys: %s", self.frame_group_name_in, self.df_d.keys())

        # if more than one input frame(group) is needed, load them here
        if self.args.get('in', None):
            self.df_in = {}
            for fg_in in self.args.get('in'):
                self.df_in[fg_in] = self.context.get_frame_group(fg_in)
                logger.debug("Initial df_in[%s] : %s", fg_in, self.df_in[fg_in].keys())

        if self.args.get('out', None):
            self.df_out = {}
            for fg_out in self.args.get('out'):
                self.df_out[fg_out] = None  # initialize empty


        for item in items:
            # the current item is stored in self.item for use in run()
            # AND in context['loop_item'] for access from other methods
            self.item = item
            self.context['loop_item'] = item
            #logger.debug("call run - loop_item : %s", item)

            # Call the actual run method of the subclass
            self.run()


        # here at the end of all loop items, we can store the final result
        # via context api
        # Nur speichern, wenn das aktuelle Objekt von FrameResourceTask abgeleitet ist
        if self.fg_accumulate:
            """   
            if self.frame_group_name_out is not None:
                logger.debug("store FG %s final df_tmp: keys %s", self.frame_group_name_out, self.df_tmp.keys())
                self.context.store_frame_group(self.frame_group_name_out, self.df_tmp)
                self.fc.store_frame_group(self.frame_group_name_out, self.df_tmp)

            if self.args.get('out', None):
                #for fg_out in self.args.get('out'):
                out = self.args.get('out')
                logger.debug("store FG %s final df_tmp2 keys %s", out[1], self.df_tmp2.keys())
                self.context.store_frame_group(out[1], self.df_tmp2)
                self.fc.store_frame_group(out[1], self.df_tmp2)
            """
            # speichere für jede fg in das entsprechende provide item
            logger.debug("-- CHECK df_tmp_d keys: %s", self.df_tmp_d.keys())
            #logger.debug("provides items: %s", self.provides)
            for key, item in self.provides.items():
                fg_name = item.get('key', None)
                logger.debug("provide item: %s - %s", key, fg_name)
                if key in self.df_tmp_d.keys():
                    logger.debug("FG %s final df_tmp: keys %s", fg_name, self.df_tmp_d[key].keys())
                    self.context.store_frame_group(fg_name, self.df_tmp_d[key])
                    self.fc.store_frame_group(fg_name, self.df_tmp_d[key])

        # reset df_d for next use, but new task instances will have their own df_d anyway
        self.df_d = None
