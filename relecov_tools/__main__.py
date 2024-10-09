#!/usr/bin/env python
import logging
import os
import json

# from rich.prompt import Confirm
import click
import relecov_tools.download_manager
import relecov_tools.log_summary
import rich.console
import rich.logging
import rich.traceback

import relecov_tools.utils
import relecov_tools.assets.pipeline_utils.viralrecon
import relecov_tools.read_lab_metadata
import relecov_tools.download_manager
import relecov_tools.json_validation
import relecov_tools.map_schema
import relecov_tools.upload_database
import relecov_tools.read_bioinfo_metadata
import relecov_tools.metadata_homogeneizer
import relecov_tools.gisaid_upload
import relecov_tools.upload_ena_protocol
import relecov_tools.pipeline_manager
import relecov_tools.build_schema
import relecov_tools.dataprocess_wrapper

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
    __version__ = "1.2.0"
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
@click.option(
    "-f",
    "--conf_file",
    help="Configuration file (not params file)",
)
@click.option(
    "-d",
    "--download_option",
    default=None,
    multiple=False,
    help="Select the download option: [download_only, download_clean, delete_only]. \
        download_only will only download the files \
        download_clean will remove files from sftp after download \
        delete_only will only delete the files",
)
@click.option(
    "-o",
    "--output_location",
    default=None,
    help="Flag: Select location for downloaded files, overrides config file location",
)
@click.option(
    "-t",
    "--target_folders",
    is_flag=False,
    flag_value="ALL",
    default=None,
    help="Flag: Select which folders will be targeted giving [paths] or via prompt",
)
def download(
    user,
    password,
    conf_file,
    download_option,
    output_location,
    target_folders,
):
    """Download files located in sftp server."""
    download_manager = relecov_tools.download_manager.DownloadManager(
        user,
        password,
        conf_file,
        download_option,
        output_location,
        target_folders,
    )
    download_manager.execute_process()


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
    help="Json with the additional metadata to add to the received user metadata",
)
@click.option(
    "-o", "--metadata-out", type=click.Path(), help="Path to save output metadata file"
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
    validation = relecov_tools.json_validation.SchemaValidation(
        json_file, json_schema, metadata, out_folder
    )
    validation.validate()


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
@click.option("-t", "--template_path", help="Path to ENA templates folder")
@click.option(
    "-a",
    "--action",
    type=click.Choice(["ADD", "MODIFY", "CANCEL", "RELEASE"], case_sensitive=False),
    help="select one of the available options",
)
@click.option("--dev", is_flag=True, default=False, help="Test submission")
@click.option("--upload_fastq", is_flag=True, default=False, help="Upload fastq files")
@click.option("-m", "--metadata_types", help="List of metadata xml types to submit")
@click.option("-o", "--output_path", help="output folder for the xml generated files")
def upload_to_ena(
    user,
    password,
    center,
    ena_json,
    template_path,
    dev,
    action,
    metadata_types,
    upload_fastq,
    output_path,
):
    """parse data to create xml files to upload to ena"""
    upload_ena = relecov_tools.upload_ena_protocol.EnaUpload(
        user=user,
        passwd=password,
        center=center,
        source_json=ena_json,
        template_path=template_path,
        dev=dev,
        action=action,
        metadata_types=metadata_types,
        upload_fastq=upload_fastq,
        output_path=output_path,
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
    help="path to fastas folder or multifasta file",
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
    help="input is a folder with several fasta files. Default: False",
)
@click.option(
    "--gzip",
    is_flag=True,
    default=False,
    help="input fasta is gziped. Default: False",
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
    gzip,
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
        gzip,
    )
    upload_gisaid.gisaid_upload()


@relecov_tools_cli.command(help_priority=9)
@click.option("-j", "--json", help="data in json format")
@click.option(
    "-t",
    "--type",
    type=click.Choice(["sample", "bioinfodata", "variantdata"]),
    multiple=False,
    default=None,
    help="Select the type of information to upload to database",
)
@click.option(
    "-plat",
    "--platform",
    type=click.Choice(
        [
            "iskylims",
            "relecov",
        ]
    ),
    multiple=False,
    default=None,
    help="name of the platform where data is uploaded",
)
@click.option("-u", "--user", help="user name for login")
@click.option("-p", "--password", help="password for the user to login")
@click.option("-s", "--server_url", help="url of the platform server")
@click.option(
    "-f",
    "--full_update",
    is_flag=True,
    default=False,
    help="Sequentially run every update option",
)
def update_db(user, password, json, type, platform, server_url, full_update):
    """upload the information included in json file to the database"""
    update_database_obj = relecov_tools.upload_database.UpdateDatabase(
        user, password, json, type, platform, server_url, full_update
    )
    update_database_obj.update_db()


# read metadata bioinformatics
@relecov_tools_cli.command(help_priority=10)
@click.option(
    "-j",
    "--json_file",
    type=click.Path(),
    help="json file containing lab metadata",
)
@click.option("-i", "--input_folder", type=click.Path(), help="Path to input files")
@click.option("-o", "--out_dir", type=click.Path(), help="Path to save output file")
@click.option("-s", "--software_name", help="Name of the software/pipeline used.")
def read_bioinfo_metadata(json_file, input_folder, out_dir, software_name):
    """
    Create the json compliant  from the Bioinfo Metadata.
    """
    new_bioinfo_metadata = relecov_tools.read_bioinfo_metadata.BioinfoMetadata(
        json_file,
        input_folder,
        out_dir,
        software_name,
    )

    new_bioinfo_metadata.create_bioinfo_file()


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


# creating symbolic links
@relecov_tools_cli.command(help_priority=13)
@click.option(
    "-i",
    "--input",
    type=click.Path(),
    help="select input folder where are located the sample files",
)
@click.option(
    "-t",
    "--template",
    type=click.Path(),
    help="select the pipeline template folder to be copied in the output folder",
)
@click.option(
    "-c",
    "--config",
    type=click.Path(),
    help="select the template config file",
)
@click.option("-o", "--output", type=click.Path(), help="select output folder")
def pipeline_manager(input, template, output, config):
    """
    Create the symbolic links for the samples which are validated to prepare for
    bioinformatics pipeline execution.
    """
    new_launch = relecov_tools.pipeline_manager.PipelineManager(
        input, template, output, config
    )
    new_launch.pipeline_exc()


# schema builder
@relecov_tools_cli.command(help_priority=14)
@click.option(
    "-i",
    "--input_file",
    type=click.Path(),
    help="Path to the Excel document containing the database definition. This file must have a .xlsx extension.",
    required=True,
)
@click.option(
    "-s",
    "--schema_base",
    type=click.Path(),
    help="Path to the base schema file. This file is used as a reference to compare it with the schema generated using this module. (Default: installed schema in 'relecov-tools/relecov_tools/schema/relecov_schema.json')",
    required=False,
)
@click.option(
    "-v",
    "--draft_version",
    type=click.STRING,
    help="Version of the JSON schema specification to be used. Example: '2020-12'. See: https://json-schema.org/specification-links",
)
@click.option(
    "-d",
    "--diff",
    is_flag=True,
    help="Prints a changelog/diff between the base and incoming versions of the schema.",
)
@click.option("-o", "--out_dir", type=click.Path(), help="Path to save output file/s")
def build_schema(input_file, schema_base, draft_version, diff, out_dir):
    """Generates and updates JSON Schema files from Excel-based database definitions."""
    schema_update = relecov_tools.build_schema.SchemaBuilder(
        input_file, schema_base, draft_version, diff, out_dir
    )
    schema_update.handle_build_schema()


@relecov_tools_cli.command(help_priority=15)
@click.option(
    "-l",
    "--lab_code",
    type=click.Path(),
    help="Name for target laboratory in log-summary.json files",
    required=True,
)
@click.option(
    "-o",
    "--output_folder",
    type=click.Path(),
    help="Path to output folder where xlsx file is saved",
    required=False,
)
@click.option(
    "-f",
    "--files",
    help="Paths to log_summary.json files to merge into xlsx file, called once per file",
    required=True,
    multiple=True,
)
def logs_to_excel(lab_code, output_folder, files):
    """Creates a merged xlsx report from all the log summary jsons given as input"""
    all_logs = []
    full_paths = [os.path.realpath(f) for f in files]
    for file in full_paths:
        if not os.path.exists(file):
            stderr.print(f"[red]File {file} does not exist")
            continue
        try:
            with open(file, "r") as f:
                all_logs.append(json.load(f)[lab_code])
        except Exception as e:
            stderr.print(f"[red]Could extract data from {file}: {e}")
    if not all_logs:
        stderr.print("All provided files were empty.")
        exit(1)
    logsum = relecov_tools.log_summary.LogSum(output_location=output_folder)
    merged_logs = logsum.merge_logs(key_name=lab_code, logs_list=all_logs)
    final_logs = logsum.prepare_final_logs(logs=merged_logs)
    excel_outpath = os.path.join(output_folder, lab_code + "_logs_report.xlsx")
    logsum.create_logs_excel(logs=final_logs, excel_outpath=excel_outpath)


@relecov_tools_cli.command(help_priority=16)
@click.option(
    "-c",
    "--config_file",
    type=click.Path(),
    help="Path to config file in yaml format [required]",
    required=True,
)
@click.option(
    "-o",
    "--output_folder",
    type=click.Path(),
    help="Path to folder where global results are saved [required]",
    required=False,
)
def wrapper(config_file, output_folder):
    """Executes the modules in config file sequentially"""
    process_wrapper = relecov_tools.dataprocess_wrapper.ProcessWrapper(
        config_file=config_file, output_folder=output_folder
    )
    process_wrapper.run_wrapper()


if __name__ == "__main__":
    run_relecov_tools()
