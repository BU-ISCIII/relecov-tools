#!/usr/bin/env python
import os
import logging

# from rich.prompt import Confirm
import click
import rich.console
import rich.logging
import rich.traceback

import relecov_tools.utils
import relecov_tools.read_metadata
import relecov_tools.sftp
import relecov_tools.create_xml

log = logging.getLogger()


def run_relecov_tools():

    # Set up rich stderr console
    stderr = rich.console.Console(
        stderr=True, force_terminal=relecov_tools.utils.rich_force_colors()
    )

    # Set up the rich traceback
    rich.traceback.install(console=stderr, width=200, word_wrap=True, extra_lines=1)

    # Print nf-core header
    # stderr.print("\n[green]{},--.[grey39]/[green],-.".format(" " * 42), highlight=False)
    stderr.print(
        "[blue]                ___   ___       ___  ___  ___                           ",
        highlight=False,
    )
    stderr.print(
        "[blue]   \    |-[grey39]-|  [blue] |   \ |    |    |    |    |   | \      /  ",
        highlight=False,
    )
    stderr.print(
        "[blue]    \   \  [grey39]/ [blue]  |__ / |__  |    |___ |    |   |  \    /   ",
        highlight=False,
    )
    stderr.print(
        "[blue]    /  [grey39] / [blue] \   |  \  |    |    |    |    |   |   \  /    ",
        highlight=False,
    )
    stderr.print(
        "[blue]   /   [grey39] |-[blue]-|   |   \ |___ |___ |___ |___ |___|    \/     ",
        highlight=False,
    )

    # stderr.print("[green]                                          `._,._,'\n", highlight=False)
    __version__ = "0.0.1"
    stderr.print(
        "[grey39]    RELECOV-tools version {}".format(__version__), highlight=False
    )

    # Lanch the click cli
    relecov_tools_cli()


# Customise the order of subcommands for --help
class CustomHelpOrder(click.Group):
    def __init__(self, *args, **kwargs):
        self.help_priorities = {}
        super(CustomHelpOrder, self).__init__(*args, **kwargs)

    def get_help(self, ctx):
        self.list_commands = self.list_commands_for_help
        return super(CustomHelpOrder, self).get_help(ctx)

    def list_commands_for_help(self, ctx):
        """reorder the list of commands when listing the help"""
        commands = super(CustomHelpOrder, self).list_commands(ctx)
        return (
            c[1]
            for c in sorted(
                (self.help_priorities.get(command, 1000), command)
                for command in commands
            )
        )

    def command(self, *args, **kwargs):
        """Behaves the same as `click.Group.command()` except capture
        a priority for listing command names in help.
        """
        help_priority = kwargs.pop("help_priority", 1000)
        help_priorities = self.help_priorities

        def decorator(f):
            cmd = super(CustomHelpOrder, self).command(*args, **kwargs)(f)
            help_priorities[cmd.name] = help_priority
            return cmd

        return decorator


@click.group(cls=CustomHelpOrder)
@click.version_option(relecov_tools.__version__)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help="Print verbose output to the console.",
)
@click.option(
    "-l", "--log-file", help="Save a verbose log to a file.", metavar="<filename>"
)
def relecov_tools_cli(verbose, log_file):

    # Set the base logger to output DEBUG
    log.setLevel(logging.DEBUG)

    # Set up logs to a file if we asked for one
    if log_file:
        log_fh = logging.FileHandler(log_file, encoding="utf-8")
        log_fh.setLevel(logging.DEBUG)
        log_fh.setFormatter(
            logging.Formatter(
                "[%(asctime)s] %(name)-20s [%(levelname)-7s]  %(message)s"
            )
        )
        log.addHandler(log_fh)


# pipeline list
@relecov_tools_cli.command(help_priority=1)
@click.argument("keywords", required=False, nargs=-1, metavar="<filter keywords>")
@click.option(
    "-s",
    "--sort",
    type=click.Choice(["release", "pulled", "name", "stars"]),
    default="release",
    help="How to sort listed pipelines",
)
@click.option("--json", is_flag=True, default=False, help="Print full output as JSON")
@click.option(
    "--show-archived", is_flag=True, default=False, help="Print archived workflows"
)
def list(keywords, sort, json, show_archived):
    """
    List available bu-isciii workflows used for relecov.
    Checks the web for a list of nf-core pipelines with their latest releases.
    Shows which nf-core pipelines you have pulled locally and whether they are up to date.
    """
    pass


# sftp
@relecov_tools_cli.command(help_priority=2)
@click.option("-u", "--user", help="User name for login to sftp server")
@click.option("-p", "--password", help="password for the user to login")
@click.option(
    "-f",
    "--conf_file",
    help="Configuration file Create Nextflow command with params (no params file)",
)
def sftp(user, password, conf_file):
    """Download files located in sftp server."""
    sftp_connection = relecov_tools.sftp.SftpHandle(user, password, conf_file)
    sftp_connection.download_from_sftp()


# metadata
@relecov_tools_cli.command(help_priority=3)
@click.option(
    "-m",
    "--metadata_file",
    type=click.Path(),
    default=None,
    help="file containing metadata",
)
@click.option(
    "-s",
    "--sample_list_file",
    type=click.Path(),
    default=os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "assets",
        "additional_metadata.json",
    ),
    help="Json with the additional metadata to add to the received user metadata",
)
@click.option(
    "-o", "--metadata-out", type=click.Path(), help="Path to save output  metadata file"
)
def read_metadata(metadata_file, sample_list_file, metadata_out):
    """
    Create the json complaining the relecov schema from the Metadata file.
    """
    new_metadata = relecov_tools.read_metadata.RelecovMetadata(
        metadata_file, sample_list_file, metadata_out
    )
    relecov_json = new_metadata.create_metadata_json()
    return relecov_json


# validation
@relecov_tools_cli.command(help_priority=4)
@click.argument("pipeline", required=False, metavar="<pipeline name>")
@click.option(
    "-r", "--revision", help="Release/branch/SHA of the project to run (if remote)"
)
@click.option("-i", "--id", help="ID for web-gui launch parameter set")
@click.option(
    "-c",
    "--command-only",
    is_flag=True,
    default=False,
    help="Create Nextflow command with params (no params file)",
)
@click.option(
    "-o",
    "--params-out",
    type=click.Path(),
    default=os.path.join(os.getcwd(), "nf-params.json"),
    help="Path to save run parameters file",
)
def validation(host, port, user, passwd):
    """Download files located in sftp server."""
    relecov_json = relecov_tools.validation_jsons.ValidationJson(
        host, port, user, passwd
    )
    relecov_json.open()


@click.option("-s", "--source_json", help="Where the validated json is")
@click.option("-o", "--output_path", help="Output folder for the xml generated files")
@click.option("-a", "--action", help="ADD or MODIFY")
def xml(source_json, output_path, action):
    """Parsed data to create xml files to upload to ENA"""
    xml_creation = relecov_tools.create_xml.XmlCreation(
        source_json, output_path, action
    )
    xml_creation.generate_xml()


if __name__ == "__main__":
    run_relecov_tools()
