from rich.console import Console
from rich.text import Text

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__ + '.log')


class ConsoleOutputHandler:
    def __init__(self):
        self.console = Console()
        self.style = "bold yellow"
        self.con_colors = ['blue', 'cyan', 'green', 'yellow', 'red', 'magenta']

    def notify_log(self, msg, task_name=None, **kwargs):
        # Standardausgabe wie bisher in pipeline.py
        # check list for kwargs
        params = {}
        for key in ['pllvl', 'color', 'idx', 'plname', 'is_subpipeline']:
            if key in kwargs:
                params[key] = kwargs.pop(key)
            else:
                params[key] = None
            #logger.debug('set %s to %s', key, params[key])

        pllvl = params['pllvl']
        color = params['color']
        idx = params['idx']
        plname = params['plname']

        if plname is not None and task_name is None:
            # Nur Pipeline-Name übergeben
            out_plname = f"  {plname}{'.' * (18 - len(plname))}"
            con = Text(f"{out_plname} - {msg}")
            self.console.print(con, style='bold white')
            return

        if task_name is None:
            con = f"{msg}"
            self.console.print(con, style=self.style)
            return

        logger.debug("= LOG plname=%s task_name=%s", plname, task_name)
        #if pllvl is None:
        #    pllvl = 0
        color = self.con_colors[color % len(self.con_colors)]
        extrastyle = "bold " + color
        #self.style = style

        if msg == 'RUNNING':
            con = Text(f"Running task: {name}", style="bold green")

        if msg == 'START':
            #outsub = 'MAIN'
            #if params['is_sub_pipeline']:
            #    outsub = 'SUB'
            fstr = f"->{plname.upper():<20}{'    ' * pllvl} Level {pllvl:<2} "  # ({outsub:<4})"
            logger.debug("= PL %s", fstr)
            con = Text(fstr, style=self.style)
        if msg == 'ASS':
            out_task = Text(f"{task_name}", style=extrastyle)
            out_plname = Text(f"  {plname}{'.' * (18 - len(plname))}")
            out_idx = Text(f"  {'    '*pllvl}[#{idx}]", style=extrastyle)
            con = Text.assemble(out_plname, out_idx, out_task)

        if msg == 'WARN':
            con = Text(f"WARN: skip task: {name}", style="bold red")

        if msg == 'END':
            con = Text(f"  {plname:<20}{'    '*pllvl} END", style=self.style)

        self.console.print(con, style=self.style)

        # Generische Fallback-Ausgabe
    #    self.console.print("XX ", kwargs)


    def on_task_start(self, task):
        # Beispiel: Task-Start ausgeben
        self.console.print(f"[bold green]Starte Task:[/] {task.name}")

    def on_task_done(self, task):
        # Beispiel: Task-Ende ausgeben
        self.console.print(f"[bold blue]Fertig:[/] {task.name}")

    def notify_status(self, msg, style=None):
        # Generische Statusausgabe
        if style:
            self.console.print(msg, style=style)
        else:
            self.console.print(msg)

    # Optional: weitere Methoden für andere Events
    # def notify_error(self, ...):
    #     ...
