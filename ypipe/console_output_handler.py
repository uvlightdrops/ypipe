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
        # set some default params
        params = {
            'color': 0,
            'pllvl': 0,
            'is_subpipeline': False,
        }

        for key in ['pllvl', 'color', 'idx', 'plname', 'is_subpipeline']:
            if key in kwargs:
                params[key] = kwargs.pop(key)
            else:
                if not key in params:
                    debug = key
                    params[key] = None
            #logger.debug('set %s to %s', key, params[key])
        logger.debug(params)
        pllvl = params['pllvl']
        color = params['color']
        idx = params['idx']
        plname = params['plname']
        logger.debug("= LOG plname=%s task_name=%s", plname, task_name)

        # create each line from a few parts
        # first is a tabulator depending on pl level, we make a dot for each nested level
        # then the pipeline name in uppercase, left aligned to 20 chars, then level
        # or for task running message another tab and the task idx and name
        tab = f"{'    ' * pllvl}"
        out_plname = Text(f"  {plname}{'.' * (18 - len(plname))}")
        out_emp_plname = f"  {' ' * 18 }"
        t_emp_plname = Text(f"  {' ' * 18 }")
        spaces_nr = 18

        color_str = self.con_colors[pllvl % len(self.con_colors)]
        extrastyle = "bold " + color_str

        #pl_str = f"{tab}-> {plname.upper():<20} Lvl:{pllvl:<2} "
        out_pl_new = f"-> {plname.upper():<14} (L{pllvl:<1})"
        out_start  = f"{plname.upper():<14} (L{pllvl:<1})"

        out_task = f"{task_name}"
        t_task = Text(f"{task_name}", style=extrastyle)

        out_idx = f"[#{idx}]"
        t_idx = Text(f"  {'    ' * pllvl}[#{idx}]", style=extrastyle)
        out_lvl = f"{pllvl*'     '}"
        out_lvl_pl = f"{pllvl*'  '}"
        ## use case distinction for different messages
        total = ''


        if msg == 'RUNNING':
            con = Text(f"task: {name}", style="bold green")

        elif msg == 'NEW PIPELINE':
            return
            #con = Text(out_pl_new, style=self.style)

        elif msg == 'START' and task_name is None:
            con = Text(f"{out_lvl_pl}{out_start}", style=self.style)

        elif msg == 'ASS':
            con = Text.assemble(t_emp_plname, t_idx, t_task)
            total = f"{out_emp_plname}{out_lvl} {out_idx}{out_task}"
            con = Text(total, style=extrastyle)

        elif msg == 'WARN':
            t_skip = Text('SKIP ', style="bold red")
            t_spc = Text(out_lvl)
            t_emp_plname = Text(f"{15*' '}")
            t_it = Text(f" {out_idx}{out_task}", style=extrastyle)
            con = Text.assemble(t_emp_plname, t_spc, t_skip, t_it)

        elif msg == 'END':
            return
            con = Text(f" {out_plname}END", style=self.style)

        else:
            con = "DEFAULT CASE: " + msg

        outcon = con
        if type(con) == str:
            outcon = Text(con)
        self.console.print(outcon, style=self.style)

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
