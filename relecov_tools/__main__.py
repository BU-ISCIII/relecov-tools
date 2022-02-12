#!/usr/bin/env python

from click.types import File
from rich import print
from rich.prompt import Confirm
import click
import rich.console
import rich.logging
import rich.traceback
import sys

def run_relecov_workflow():
      # Set up rich stderr console
    stderr = rich.console.Console(stderr=True, force_terminal=nf_core.utils.rich_force_colors())

    # Set up the rich traceback
    rich.traceback.install(console=stderr, width=200, word_wrap=True, extra_lines=1)

    # Print nf-core header
    stderr.print("\n[green]{},--.[grey39]/[green],-.".format(" " * 42), highlight=False)
    stderr.print("[blue]          ___     __   __   __   ___     [green]/,-._.--~\\", highlight=False)
    stderr.print("[green]                                          `._,._,'\n", highlight=False)
    stderr.print("[grey39]    nf-core/tools version {}".format(nf_core.__version__), highlight=False)
    try:
    
 # Lanch the click cli
    nf_core_cli()




@click.group(cls=CustomHelpOrder)
@click.version_option(nf_core.__version__)
@click.option("-v", "--verbose", is_flag=True, default=False, help="Print verbose output to the console.")
@click.option("-l", "--log-file", help="Save a verbose log to a file.", metavar="<filename>")
def nf_core_cli(verbose, log_file):

    # Set the base logger to output DEBUG
    log.setLevel(logging.DEBUG)
    
    
