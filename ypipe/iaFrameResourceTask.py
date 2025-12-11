from ypipe.frameResourceTask import FrameResourceTask, MergeFrameResourceTask
from ypipe.consoleMixin import ConsoleMixin

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.text import Text

from ypipe.tableApp import TableApp


class IaMergeFrameResourceTask(ConsoleMixin, MergeFrameResourceTask):
    """ Interaktiver Merge von zwei single Frames mit Auswahl per Konsole
        legt im context ab als FG subframe  """
    def __init__(self, *args):
        #super().__init__(*args)
        MergeFrameResourceTask.__init__(self, *args)
        ConsoleMixin.__init__(self, *args)

    def run(self):
        self.prepare()

        item = self.group or self.item

        self.prepare_input()

        merged = self.merge()

        kp_pf = self.context.get('config_d').get('kp_process_fields')
        cols = self.fc.frame_fields.get('ia_merge_table')
        # remove all dismiss_fields
        cols = [col for col in cols if col not in kp_pf['dismiss_fields']]

        logger.debug('ia merge cols: %s', cols)
        pk_col = 'role_index'
        # XXX remove pk_col from params
        add_data = {
            'group_path_new': merged['group_path_new'].iloc[0],
            'group': item,
        }
        app = TableApp(merged, columns=cols, pk_col=pk_col,
                       add_data=add_data)

        app.run()
        # Die ausgewählten Indizes stehen in app.selected
        confirmed_rows = [merged.iloc[idx] for idx in app.selected]
        logger.debug('ia merge len confirmed rows: %s', len(confirmed_rows))
        # confirmed_rows als DataFrame ablegen
        confirmed_df = pd.DataFrame(confirmed_rows)
        self.save_frame_to_df_tmp_d(confirmed_df, 'main', item)



class IaFrameResourceTask(ConsoleMixin, FrameResourceTask):
    """ Interaktive Bearbeitung eines Frames per Konsole """
    def __init__(self, *args):
        #super().__init__(*args)
        ConsoleMixin.__init__(self, *args)
        FrameResourceTask.__init__(self, *args)

    def run(self):
        self.prepare()
        group = self.group or self.item

        if self.args.get('from', None):
            if self.args.get('from', None) == 'context':

                logger.debug('Lade Frame %s aus context', self.args['in'])
                df = self.context.get(self.args['in'], None)
            else:
                df = self.fc.get_frame(self.frame_group_name, self.args['in'])
        #self.edit_table(df)

        # Ergebnis-DataFrame
        self.print(f'[green]Bearbeitung abgeschlossen. Verbleibende Zeilen: {len(df)}[/green]')
