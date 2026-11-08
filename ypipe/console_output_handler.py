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
        color_str = self.con_colors[pllvl % len(self.con_colors)]
        extrastyle = "bold " + color_str

        #tab = f"{'    ' * pllvl}"
        #out_plname = f"  {plname}{'.' * (18 - len(plname))}"
        #out_emp_plname = f"  {' ' * 18 }"
        #t_emp_plname = Text(f"  {' ' * 18 }")
        spaces_nr = 14
        pl_indent = 3

        num_spaces = 14 - (pl_indent * (pllvl+1))

        def pt_str(pllvl):
            t_list = []
            for l in range(pllvl+1):
                c_idx = l % len(self.con_colors)
                es = "bold " + self.con_colors[c_idx]
                t_list.append(Text("|   ", style=es))
            return Text.assemble(*t_list)

        t_pt = pt_str(pllvl)

        out_task = f"{task_name}"
        out_idx = f"[#{idx}]"
        num_indent = 4
        char_indent = '.'
        out_lvl = f"{pllvl * num_indent * char_indent}"
        t_spc   = Text(out_lvl)
        t_empty = Text("")
        t_skip  = t_empty
        t_it    = t_empty

        ## use case distinction for different messages
        if msg == 'RUN':
            t_it = Text(f"task: {name}", style="bold green")

        elif msg == 'NEW PIPELINE':
            out_pl_new = f"->{plname.upper():<14} NEW"
            t_pt = Text(f"{out_pl_new}", style=self.style)

        elif msg == 'START' and task_name is None:
            return
            t_pt = pt_str(pllvl-1)
            t_spc = t_empty
            out_start = f"{plname.upper():<12} (L{pllvl:<1})"
            t_it = Text(f"{out_start}", style=self.style)

        elif msg == 'ASS':
            out_lvl = f"{pllvl * num_indent * char_indent}"
            t_skip = Text('run  ', style="bold green")
            t_spc = Text(out_lvl)
            t_it = Text(f"{out_idx}{out_task}", style=extrastyle)

        elif msg == 'WARN':
            t_skip = Text('skip ', style="bold red")
            t_it = Text(f"{out_idx}{out_task}", style=extrastyle)

        elif msg == 'END':
            return
            con = Text(f" {out_plname}END", style=self.style)

        else:
            con = "DEFAULT CASE: " + msg

        con = Text.assemble(t_pt, t_spc, t_skip, t_it)
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
