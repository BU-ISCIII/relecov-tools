#!/usr/bin/env python
import os
import logging

# import re

# from rich.prompt import Confirm
import click
import rich.console
import rich.logging
import rich.traceback

import relecov_tools.utils
import relecov_tools.read_lab_metadata
import relecov_tools.sftp_handle
import relecov_tools.ena_upload
import relecov_tools.json_validation
import relecov_tools.map_schema
import relecov_tools.feed_database
import relecov_tools.read_bioinfo_metadata
import relecov_tools.long_table_parse
import relecov_tools.metadata_homogeneizer
import relecov_tools.gisaid_upload

log = logging.getLogger()

# Set up rich stderr console
stderr = rich.console.Console(
    stderr=True, force_terminal=relecov_tools.utils.rich_force_colors()
)


def run_relecov_tools():

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
    __version__ = "0.0.4"
    stderr.print(
        "\n" "[grey39]    RELECOV-tools version {}".format(__version__), highlight=False
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


# sftp
@relecov_tools_cli.command(help_priority=2)
@click.option("-u", "--user", help="User name for login to sftp server")
@click.option("-p", "--password", help="password for the user to login")
@click.option("-r_u", "--user_relecov", help="User name for updating data to relecov")
@click.option("-p_r", "--password_relecov", help="password for relecov user")
@click.option(
    "-f",
    "--conf_file",
    help="Configuration file (no params file)",
)
def download(user, password, conf_file, user_relecov, password_relecov):
    """Download files located in sftp server."""
    sftp_connection = relecov_tools.sftp_handle.SftpHandle(
        user, password, conf_file, user_relecov, password_relecov
    )
    sftp_connection.download()


# metadata
@relecov_tools_cli.command(help_priority=3)
@click.option(
    "-m",
    "--metadata_file",
    type=click.Path(),
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
def read_lab_metadata(metadata_file, sample_list_file, metadata_out):
    """
    Create the json compliant to the relecov schema from the Metadata file.
    """
    new_metadata = relecov_tools.read_lab_metadata.RelecovMetadata(
        metadata_file, sample_list_file, metadata_out
    )
    relecov_json = new_metadata.create_metadata_json()
    return relecov_json


# validation
@relecov_tools_cli.command(help_priority=4)
@click.option("-j", "--json_file", help="Json file to validate")
@click.option("-s", "--json_schema", help="Json schema")
@click.option(
    "-m",
    "--metadata",
    type=click.Path(),
    help="Origin file containing metadata",
)
@click.option("-o", "--out_folder", help="Path to save validate json file")
def validate(json_file, json_schema, metadata, out_folder):
    """Validate json file against schema."""
    (
        validated_json_data,
        invalid_json,
        errors,
    ) = relecov_tools.json_validation.validate_json(json_file, json_schema, out_folder)
    if len(invalid_json) > 0:
        log.error("Some of the samples in json metadata were not validated")
        stderr.print("[red] Some of the Samples are not validate")
        if not os.path.isfile(metadata):
            log.error("Metadata file %s does not exist", metadata)
            stderr.print(
                "[red] Unable to create excel file for invalid samples. Metadata file ",
                metadata,
                " does not exist",
            )
            exit(1)
        relecov_tools.json_validation.create_invalid_metadata(
            metadata, invalid_json, out_folder
        )

    else:
        log.info("All data in json were validated")


# mapping to ENA schema
@relecov_tools_cli.command(help_priority=5)
@click.option("-p", "--origin_schema", help="File with the origin (relecov) schema")
@click.option("-j", "--json_data", help="File with the json data to convert")
@click.option(
    "-d",
    "--destination_schema",
    type=click.Choice(["ENA", "GISAID", "other"], case_sensitive=True),
    help="schema to be mapped",
)
@click.option("-f", "--schema_file", help="file with the custom schema")
@click.option("-o", "--output", help="File name and path to store the mapped json")
def map(origin_schema, json_data, destination_schema, schema_file, output):
    """Convert data between phage plus schema to ENA, GISAID, or any other schema"""
    new_schema = relecov_tools.map_schema.MappingSchema(
        origin_schema, json_data, destination_schema, schema_file, output
    )
    new_schema.map_to_data_to_new_schema()


# upload to ENA
@relecov_tools_cli.command(help_priority=6)
@click.option("-u", "--user", help="user name for login to ena")
@click.option("-p", "--password", help="password for the user to login")
@click.option("-c", "--center", help="center name")
@click.option("-e", "--ena_json", help="where the validated json is")
@click.option("-s", "--study", help="study/project name to include in xml files")
@click.option(
    "-a",
    "--action",
    type=click.Choice(["add", "modify", "cancel", "release"], case_sensitive=False),
    help="select one of the available options",
)
@click.option("--dev", is_flag=True, default=False)
@click.option("-o", "--output_path", help="output folder for the xml generated files")
def upload_to_ena(user, password, center, ena_json, dev, study, action, output_path):
    """parsed data to create xml files to upload to ena"""

    upload_ena = relecov_tools.ena_upload.EnaUpload(
        user, password, center, ena_json, dev, study, action, output_path
    )
    upload_ena.upload()


# upload to GISAID
@relecov_tools_cli.command(help_priority=7)
@click.option("-u", "--user", help="user name for login")
@click.option("-p", "--password", help="password for the user to login")
@click.option("-c", "--client_id", help="client-ID provided by clisupport@gisaid.org")
@click.option("-t", "--token", help="path to athentication token")
@click.option("-e", "--gisaid_json", help="path to validated json mapped to GISAID")
@click.option(
    "-i",
    "--input_path",
    help="path to fasta or multifasta file",
)
@click.option("-o", "--output_path", help="output folder for log")
@click.option(
    "-f",
    "--frameshift",
    type=click.Choice(["catch_all", "catch_none", "catch_novel"], case_sensitive=False),
    help="frameshift notification",
)
@click.option(
    "-x",
    "--proxy_config",
    help="introduce your proxy credentials as: username:password@proxy:port",
    required=False,
)
@click.option(
    "--single",
    is_flag=True,
    default=False,
    help="Default input is a multifasta.",
)
def upload_to_gisaid(
    user,
    password,
    client_id,
    token,
    gisaid_json,
    input_path,
    output_path,
    frameshift,
    proxy_config,
    single,
):
    """parsed data to create files to upload to gisaid"""
    upload_gisaid = relecov_tools.gisaid_upload.GisaidUpload(
        user,
        password,
        client_id,
        token,
        gisaid_json,
        input_path,
        output_path,
        frameshift,
        proxy_config,
        single,
    )
    upload_gisaid.gisaid_upload()


# launch
@relecov_tools_cli.command(help_priority=8)
@click.option("-u", "--user", help="user name for connecting to the server")
def launch(user):
    """launch viralrecon in hpc"""
    pass


# update_db
@relecov_tools_cli.command(help_priority=9)
@click.option("-j", "--json", help="data in json format")
@click.option("-s", "--schema", help="json schema if relecov is not used")
@click.option(
    "-t",
    "--type",
    type=click.Choice(["sample", "bioinfodata", "variantdata"]),
    multiple=False,
    help="Select the type of information to upload to database",
)
@click.option(
    "-d",
    "databaseServer",
    type=click.Choice(
        [
            "iskylims",
            "relecov",
        ]
    ),
    multiple=False,
    help="name of the server which information is defined in config file",
)
@click.option("-u", "--user", help="user name for login")
@click.option("-p", "--password", help="password for the user to login")
def update_db(user, password, json, schema, type, databaseServer):
    """feed database with json"""
    feed_database = relecov_tools.feed_database.FeedDatabase(
        user, password, json, schema, type, databaseServer
    )
    feed_database.store_data()


# read metadata bioinformatics
@relecov_tools_cli.command(help_priority=10)
@click.option(
    "-m",
    "--metadata_file",
    type=click.Path(),
    help="file containing metadata",
)
@click.option("-i", "--input-folder", type=click.Path(), help="Path to input files")
@click.option(
    "-o", "--metadata-out", type=click.Path(), help="Path to save output  metadata file"
)
@click.option(
    "-p",
    "--mapping-illumina",
    type=click.Path(),
    help="Name of the mapping_illumina file",
)
def read_bioinfo_metadata(metadata_file, input_folder, metadata_out, mapping_illumina):
    """
    Create the json compliant  from the Bioinfo Metadata.
    """

    new_bioinfo_metadata = relecov_tools.read_bioinfo_metadata.BioinfoMetadata(
        metadata_file, input_folder, metadata_out, mapping_illumina
    )

    new_bioinfo_metadata.bioinfo_parse(metadata_file)


# read metadata bioinformatics
@relecov_tools_cli.command(help_priority=11)
@click.option(
    "-l",
    "--longtable_file",
    type=click.Path(),
    help="file containing variant long table ",
)
@click.option("-o", "--output", type=click.Path(), help="Path to save json output")
def long_table_parse(longtable_file, output):
    new_json_parse = relecov_tools.long_table_parse.LongTableParse(
        longtable_file, output
    )
    """Create Json file from long table"""
    new_json_parse.parsing_csv()


# read metadata bioinformatics
@relecov_tools_cli.command(help_priority=12)
@click.option(
    "-i",
    "--institution",
    type=click.Choice(["isciii", "hugtip", "hunsc-iter"], case_sensitive=False),
    help="select one of the available institution options",
)
@click.option(
    "-d",
    "--directory",
    type=click.Path(),
    help="Folder where are located the additional files",
)
@click.option("-o", "--output", type=click.Path(), help="Path to save json output")
def metadata_homogeneizer(institution, directory, output):
    """Parse institution metadata lab to the one used in relecov"""
    new_parse = relecov_tools.metadata_homogeneizer.MetadataHomogeneizer(
        institution, directory, output
    )
    new_parse.converting_metadata()


if __name__ == "__main__":
    run_relecov_tools()
