import os
from pathlib import Path
import yaml
import click
from typing import Optional
from ypipe.ypipe_app import YpipeApp

from flowpy.utils import setup_logger
logger = setup_logger(__name__, __name__ + '.log')

# Projekt-Root ist immer das aktuelle Arbeitsverzeichnis
REPO_ROOT = Path.cwd()

def load_app_config(master_cfg, app_name: str):
    APPS_CONF_DIR = master_cfg

    conf_file = APPS_CONF_DIR.joinpath(f"{app_name}.yml")
    if not conf_file.exists():
        raise FileNotFoundError(f"App config not found: {conf_file}")
    with open(conf_file) as f:
        return yaml.safe_load(f)


def create_ypipe_app(app_name, plname, **kwargs) -> 'YpipeApp':
    """Erzeuge eine Pipeline-Instanz über YpipeApp (zentralisiertes Backend)."""
    repo = REPO_ROOT
    master_cfg = repo.joinpath('data_master')
    cfg = load_app_config(master_cfg, app_name)
    logger.debug('cfg', cfg)
    data_path = Path(cfg.get('data_path', 'ypipe_data'))
    master_cfg_abs = repo.joinpath('data_master')
    print('master_cfg_abs:', master_cfg_abs)
    data_path_abs = data_path
    project_dir_abs = repo.joinpath(app_name)

    # YpipeApp als zentrale Instanz
    ypipe_app = YpipeApp(
        app_name=app_name,
        repo=repo,
        data_path=data_path_abs,
        master_config_dir=master_cfg_abs,
        project_dir = project_dir_abs,
        plname=plname,
    )
    return ypipe_app

    """
    pipeline = ypipe_app.load_pipeline(
        repo=repo,
        plname=plname,
        data_path = data_path,
        pipeline_cfg=cfg,
    )
    # Kontext-Objekte für nachfolgende Kommandos bereitstellen
    pipeline.project_dir = project_dir_abs
    return pipeline
    """

# --- CLI-Kommandos und Gruppen ---
@click.group()
@click.option('--app', 'app_name', default=None, help="App name (overrides YLDPIPE_APP_NAME)")
@click.option('--plname', 'plname', default='yp_default', help="Set the pipeline name")
@click.pass_context
def cli(ctx, app_name, plname):
    ctx.ensure_object(dict)
    ctx.obj['DEBUG'] = True
    app_env = os.environ.get('YLDPIPE_APP_NAME', None)
    app_name = app_name or app_env
    print("CLI app_name env:", app_name, ", plname:", plname)
    ctx.obj['app_name'] = app_name
    ctx.obj['plname'] = plname


@cli.group()
@click.pass_context
@click.option('--phase', 'phase', default='', help="Set the phase with predefined configuration")
def config(ctx, phase):
    """ Manage config items """
    phase_ui = phase
    if phase == '':
        phase_ui = None
    click.echo("Config group entered, DEBUG=%s, phase=%s" %(str(ctx.obj.get('DEBUG')), phase_ui))
    app_name = ctx.obj.get('app_name')
    ctx.obj['pl'] = create_ypipe_app(app_name=app_name, plname=ctx.obj.get('plname'))
    ctx.obj['pl'].load_pipeline(ctx.obj['pl'].plname)
    ctx.obj['pl'].load_task_definitions()

@cli.group()
@click.pass_context
def work(ctx):
    """ Run data pipeline """
    app_name = ctx.obj.get('app_name')
    plname = ctx.obj.get('plname')
    logger.debug("work group: app_name=%s, plname=%s", app_name, plname)
    ctx.obj['pl'] = create_ypipe_app(app_name=app_name, plname=plname)
    ctx.obj['pl'].load_pipeline(plname)
    ctx.obj['pl'].active_pipeline.load_task_definitions()
    ctx.obj['pl'].init_fc() #framecache()

@cli.group()
@click.pass_context
def pipeline(ctx):
    """ pipeline manipulation commands """
    app_name = ctx.obj.get('app_name', None)
    app_name = ctx.obj.get('app_name')
    if not app_name:
        raise click.UsageError('App name not set; provide --app or set YLDPIPE_APP_NAME')
    ctx.obj['pl'] = create_ypipe_app(app_name=app_name, plname=ctx.obj.get('plname'))

@pipeline.command()
@click.pass_context
def list_tasks(ctx):
    click.secho("Registering tasks: ", fg="yellow", bold=True)
    pl = ctx.obj['pl']
    pl.load_pipeline(pl.plname)
    pl.active_pipeline.load_task_definitions()
    for key in pl.task_defs.keys():
        click.echo(key)
    click.secho("CLI Done", fg="green", bold=True)

@pipeline.command()
@click.pass_context
def show(ctx):
    pl = ctx.obj['pl']
    pl.load_pipeline(pl.plname)
    pl.load_task_definitions()
    click.secho("Pipeline file", fg="yellow", bold=True)
    click.echo(pl.plname)
    click.secho("Pipeline config", fg="yellow", bold=True)
    click.echo(yaml.dump(pl.cfg_profile, sort_keys=False))

@config.command()
@click.pass_context
def list_sections(ctx):
    click.secho("All sections", fg="yellow", bold=True)
    for key in ctx.obj['pl'].config_list():
        click.echo(key)

@config.command()
@click.pass_context
def render(ctx):
    pl = ctx.obj['pl']
    pl.render_dag()

@work.command()
@click.pass_context
@click.argument('name', required=True)
def single(ctx, name):
    pl = ctx.obj['pl']
    pl.active_pipeline.run_task_by_name(name)
    print("FINISHED")

@work.command()
@click.pass_context
def all(ctx):
    pl = ctx.obj['pl']
    pl.active_pipeline.run_all()
    print("FINISHED all")

"""
@click.command()
@click.option('--app', 'app_name', default=None, help='App name (overrides env)')
@click.option('--plname', 'plname', default=None, help='Pipeline name (plname.yml)')
def main(app_name, plname):
    app_name = app_name or os.environ.get('YLDPIPE_APP_NAME')
    if not app_name:
        print('Error: must provide --app or set YLDPIPE_APP_NAME')
        raise SystemExit(2)
    pl = create_ypipe_app(app_name, plname=plname)
    print('Created Pipeline:', pl.app_name, 'plname=', pl.plname)
"""
