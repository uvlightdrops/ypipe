import re
from yldpipeNG.transformFunc import TransformFunc
#from custom_transformations.hostTransformations import HostTransformations


from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')


class TransformMixin(TransformFunc): #, HostTransformations):
    def df_global_transformations(self, df, meta_cfg):
        #logger.debug("Processing META configuration: %s", meta_cfg)

        for meta_key, meta_val in meta_cfg.items():
            if meta_key == 'remove_lines':
                first_col = df.columns[0]
                first_act_on = meta_val.pop('FIRST', None)
                if first_act_on:
                    logger.debug("Processing FIRST actions for remove_lines: %s", first_act_on)
                    # if this is configured we like to add to the following loop as first entry
                    # with the first column name as key
                    meta_val[first_col] = first_act_on

                for colname, values in meta_val.items():
                    logger.debug("Processing remove_lines for column: %s with values: %s", colname, values)

                    astring_l = values.get('startswith', None)
                    if astring_l:
                        for astring in astring_l:
                            #logger.debug("Removing lines starting with: %s in col %s", astring, colname)
                            df = df[~df[colname].astype(str).str.startswith(astring)]

                    equal_l = values.get('equal', None)
                    if equal_l is not None:
                        for estring in equal_l:
                            #logger.debug("Removing lines equal to: %s in col %s", estring, colname)
                            df = df[df[colname].astype(str) != estring]

                    if values.get('if_empty', False):
                        #logger.debug("Removing lines where column %s is empty.", colname)
                        logger.debug("DataFrame shape before removing empty lines in column %s: %s", colname, df.shape)
                        df = df[df[colname].astype(str).str.strip() != '']
                        logger.debug("DataFrame shape after removing empty lines in column %s: %s", colname, df.shape)


            if meta_key == 'remove_columns':
                cols_to_remove = meta_val.get('columns', [])
                logger.debug("Removing columns as per META configuration: %s", cols_to_remove)
                df = df.drop(columns=cols_to_remove, errors='ignore')
        return df



    def apply_transformations(self, df, cfg):
        """
        Apply transformations and validations to the DataFrame based on the task's configuration.
        :param df: Input DataFrame.
        :return: Transformed DataFrame.
        """
        # test if there is a nan value in df
        if df.isnull().values.any():
            logger.info("DataFrame contains NaN values, proceeding to fill them.")
        # check on nan values in df and replace them
        df = df.fillna('')

        # Evaluate the META key of cfg seperately
        meta_cfg = cfg.pop('META', None)
        if meta_cfg:
            df = self.df_global_transformations(df, meta_cfg)


        for colname in df.columns:
            colcfg = cfg.get(colname)
            if not colcfg:
                logger.debug("No transformation config for column: %s, skipping.", colname)
                continue
            logger.debug("Processing column: %s with config: %s", colname, colcfg)
            #if colname not in df.columns:
            #    logger.warning("Column %s not in DataFrame, skipping.", colname)
            #    continue
            df[colname] = df[colname].apply(lambda val: self.gener(val, colcfg, colname))

        return df
