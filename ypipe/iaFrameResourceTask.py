
from ypipe.frameResourceTask import FrameResourceTask
from ypipe.consoleMixin import ConsoleMixin

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__+'.log')
import pandas as pd
#from rich.console import Console
#from rich.table import Table
#from rich.prompt import Prompt
#from textual.widgets import RichLog



class IaMergeFrameResourceTask(ConsoleMixin, FrameResourceTask):
    """ Interaktiver Merge von zwei single Frames mit Auswahl per Konsole
        legt im context ab als FG subframe  """
    def __init__(self, *args):
        super().__init__(*args)
        ConsoleMixin.__init__(self, *args)
        FrameResourceTask.__init__(self, *args)

    def run(self):
        self.prepare()

        group = self.group or self.item
        # Frames laden
        tmp1 = self.context.get(self.args['in1'], {})
        df1 = tmp1.get(self.item, None)
        tmp2 = self.context.get(self.args['in2'], {})
        df2 = tmp2.get(self.item, None)
        if df1 is None or df2 is None:
            self.print('[red]Fehler: Einer der Frames ist None[/red]')
            return
        df_merged = self.ia_merge(df1, df2, group)

        p_key = self.provide_main.get('key')
        if p_key not in self.context:
            self.context[p_key] = {}
        self.context[p_key][group] = df_merged
        self.print(f'[green]Merge abgeschlossen. Übernommene Zeilen: {len(df_merged)}[/green]')



class IaFrameResourceTask(ConsoleMixin, FrameResourceTask):
    """ Interaktive Bearbeitung eines Frames per Konsole """
    def __init__(self, *args):
        super().__init__(*args)
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
        p_key = self.provide_main.get('key')
        if p_key not in self.context:
            self.context[p_key] = {}
        self.context[p_key][group] = df
        self.print("DOES NOTHING YET")
        self.print(f'[green]Bearbeitung abgeschlossen. Verbleibende Zeilen: {len(df)}[/green]')

