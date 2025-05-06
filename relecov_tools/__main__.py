#!/usr/bin/env python
import logging
import os
import json
import sys
from datetime import datetime

# from rich.prompt import Confirm
import click
import relecov_tools.config_json
import relecov_tools.download_manager
import relecov_tools.log_summary
import rich.console
import rich.traceback

import relecov_tools.config_json
import relecov_tools.utils
import relecov_tools.read_lab_metadata
import relecov_tools.download_manager
import relecov_tools.json_validation
import relecov_tools.mail
import relecov_tools.map_schema
import relecov_tools.upload_database
import relecov_tools.read_bioinfo_metadata
import relecov_tools.metadata_homogeneizer
import relecov_tools.gisaid_upload
import relecov_tools.upload_ena_protocol
import relecov_tools.pipeline_manager
import relecov_tools.build_schema
import relecov_tools.dataprocess_wrapper
import relecov_tools.upload_results
import relecov_tools.base_module

log = logging.getLogger()

# Set up rich stderr console
stderr = rich.console.Console(
    stderr=True, force_terminal=relecov_tools.utils.rich_force_colors()
)

__version__ = "1.5.0"


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
    "-l",
    "--log-path",
    default=None,
    help="Creates log file in given folder. Uses default path in config or tmp if empty.",
)
@click.option(
    "-d",
    "--debug",
    is_flag=True,
    default=False,
    help="Show the full traceback on error for debugging purposes.",
)
@click.option(
    "-h",
    "--hex-code",
    default=None,
    help="Define hexadecimal code. This might overwrite existing files with the same hex-code",
)
@click.pass_context
def relecov_tools_cli(ctx, verbose, log_path, debug, hex_code):
    if debug:
        # Set the base logger to output everything
        level = logging.DEBUG
    else:
        # Set the base logger to hide DEBUG messages
        level = logging.INFO
    log.setLevel(level)
    if verbose:
        stream_handler = relecov_tools.base_module.BaseModule.set_log_handler(
            None, level=level, only_stream=True
        )
        log.addHandler(stream_handler)
    if hex_code is not None:
        relecov_tools.base_module.BaseModule._global_hex_code = str(hex_code)
    # Set up logs to a file if we asked for one
    called_module = str(ctx.invoked_subcommand).replace("-", "_")
    config = relecov_tools.config_json.ConfigJson(extra_config=True)
    logs_config = config.get_configuration("logs_config")
    default_outpath = logs_config.get("default_outpath", "/tmp/relecov_tools")
    if log_path is None:
        log_path = logs_config.get("modules_outpath", {}).get(called_module)
        if not log_path:
            log_path = os.path.join(default_outpath, called_module)
    else:
        relecov_tools.base_module.BaseModule._cli_log_path_param = log_path
    current_datetime = datetime.today().strftime("%Y%m%d%-H%M%S")
    log_filepath = os.path.join(
        log_path, "_".join([called_module, current_datetime]) + ".log"
    )

    try:
        os.makedirs(log_path, exist_ok=True)
        log_fh = relecov_tools.base_module.BaseModule.set_log_handler(
            log_filepath, level=level
        )
        log.addHandler(log_fh)
    except Exception:
        log_filepath = os.path.join(
            default_outpath, "_".join([called_module, "temp"]) + ".log"
        )
        os.makedirs(default_outpath, exist_ok=True)
        log_fh = relecov_tools.base_module.BaseModule.set_log_handler(
            log_filepath, level=level
        )
        log.addHandler(log_fh)
        log.warning(f"Invalid --log-path {log_path}. Using {log_filepath} instead")
    relecov_tools.base_module.BaseModule._cli_log_file = os.path.realpath(log_filepath)
    cli_command = " ".join(sys.argv)
    relecov_tools.base_module.BaseModule._cli_command = cli_command
    relecov_tools.base_module.BaseModule._current_version = __version__
    ctx.ensure_object(dict)  # Asegura que ctx.obj es un diccionario
    ctx.obj["debug"] = debug  # Guarda el flag de debug


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
@click.option(
    "-s",
    "--subfolder",
    is_flag=False,
    flag_value="ALL",
    default="RELECOV",
    help="Flag: Specify which subfolder to process (default: RELECOV)",
)
@click.pass_context
def download(
    ctx,
    user,
    password,
    conf_file,
    download_option,
    output_location,
    target_folders,
    subfolder,
):
    """Download files located in sftp server."""
    debug = ctx.obj.get("debug", False)
    try:
        download_manager = relecov_tools.download_manager.DownloadManager(
            user,
            password,
            conf_file,
            download_option,
            output_location,
            target_folders,
            subfolder,
        )
        download_manager.execute_process()
    except Exception as e:
        if debug:
            log.error(f"EXCEPTION FOUND: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


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
@click.option(
    "-f",
    "--files-folder",
    default=None,
    type=click.Path(),
    help="Path to folder where samples files are located",
)
@click.pass_context
def read_lab_metadata(ctx, metadata_file, sample_list_file, metadata_out, files_folder):
    """
    Create the json compliant to the relecov schema from the Metadata file.
    """
    debug = ctx.obj.get("debug", False)
    new_metadata = relecov_tools.read_lab_metadata.RelecovMetadata(
        metadata_file, sample_list_file, metadata_out, files_folder
    )
    try:
        new_metadata.create_metadata_json()
    except Exception as e:
        if debug:
            log.error(f"EXCEPTION FOUND: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


# validation
@relecov_tools_cli.command(help_priority=4)
@click.option("-j", "--json_file", help="Json file to validate")
@click.option(
    "-s", "--json_schema_file", help="Path to the JSON Schema file used for validation"
)
@click.option(
    "-m",
    "--metadata",
    type=click.Path(),
    help="Origin file containing metadata",
)
@click.option("-o", "--out_folder", help="Path to save validate json file")
@click.option(
    "-e",
    "--excel_sheet",
    required=False,
    default=None,
    help="Optional: Name of the sheet in excel file to validate.",
)
@click.pass_context
def validate(ctx, json_file, json_schema_file, metadata, out_folder, excel_sheet):
    """Validate json file against schema."""
    debug = ctx.obj.get("debug", False)
    validation = relecov_tools.json_validation.SchemaValidation(
        json_file, json_schema_file, metadata, out_folder, excel_sheet
    )
    try:
        validation.validate()
    except Exception as e:
        if debug:
            log.error(f"EXCEPTION FOUND: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


# send-email
@relecov_tools_cli.command(help_priority=4)
@click.option(
    "-v",
    "--validate-file",
    required=True,
    type=click.Path(exists=True),
    help="Path to the validation summary json file (validate_log_summary.json)",
)
@click.option(
    "-r",
    "--receiver-email",
    required=False,
    help="Recipient's e-mail address (optional). If not provided, it will be extracted from the institutions guide.",
)
@click.option(
    "-a",
    "--attachments",
    multiple=True,
    type=click.Path(exists=True),
    help="Path to file",
)
@click.option(
    "-t",
    "--template_path",
    type=click.Path(exists=True),
    required=True,
    default=None,
    help="Path to relecov-tools templates folder",
)
@click.option(
    "-p",
    "--email-psswd",
    help="Password for bioinformatica@isciii.es",
    required=False,
    default=None,
)
@click.pass_context
def send_mail(
    ctx, validate_file, receiver_email, attachments, template_path, email_psswd
):
    """
    Send a sample validation report by mail.
    """
    debug = ctx.obj.get("debug", False)
    config_loader = relecov_tools.config_json.ConfigJson()
    config = config_loader.get_configuration("mail_sender")
    if not config:
        raise ValueError(
            "Error: The configuration for 'mail_sender' could not be loaded."
        )

    validate_data = relecov_tools.utils.read_json_file(validate_file)
    batch = os.path.basename(os.path.dirname(os.path.abspath(validate_file)))

    if not validate_data:
        raise ValueError("Error: Validation data could not be loaded.")

    submitting_institution_code = list(validate_data.keys())[0]

    invalid_count = relecov_tools.log_summary.LogSum.get_invalid_count(validate_data)

    email_sender = relecov_tools.mail.EmailSender(config, template_path)

    template_choice = click.prompt(
        "Select the type of template:\n1. Validation with errors\n2. Validation successful",
        type=int,
        default=1,
        show_choices=False,
    )
    if template_choice not in [1, 2]:
        raise ValueError("Error: invalid option.")

    # Determinar el template a usar
    if template_choice == 1:
        template_name = "jinja_template_with_errors.j2"
    else:
        template_name = "jinja_template_success.j2"

    add_info = click.confirm(
        "Would you like to add additional information in the mail?", default=False
    )
    additional_info = ""
    if add_info:
        additional_info = click.prompt("Enter additional information")

    institution_info = email_sender.get_institution_info(submitting_institution_code)
    if not institution_info:
        raise ValueError("Error: Could not obtain institution information.")

    institution_name = institution_info["institution_name"]
    email_receiver_from_json = institution_info["email_receiver"]

    email_body = email_sender.render_email_template(
        additional_info,
        invalid_count=invalid_count,
        submitting_institution_code=submitting_institution_code,
        template_name=template_name,
        batch=batch,
    )

    if email_body is None:
        raise RuntimeError("Error: Could not generate mail.")

    final_receiver_email = None
    if not receiver_email:
        final_receiver_email = [
            email.strip() for email in email_receiver_from_json.split(";")
        ]
    else:
        final_receiver_email = (
            [email.strip() for email in receiver_email.split(";")]
            if isinstance(receiver_email, str)
            else receiver_email
        )

    if not final_receiver_email:
        raise ValueError("Error: Could not obtain the recipient's email address.")

    subject = (
        f"RELECOV - Informe de Validación de Muestras {batch} - {institution_name}"
    )
    try:
        email_sender.send_email(
            final_receiver_email, subject, email_body, attachments, email_psswd
        )
    except Exception as e:
        if debug:
            log.error(f"EXCEPTION FOUND: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


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
@click.pass_context
def map(ctx, origin_schema, json_data, destination_schema, schema_file, output):
    """Convert data between phage plus schema to ENA, GISAID, or any other schema"""
    debug = ctx.obj.get("debug", False)
    new_schema = relecov_tools.map_schema.MappingSchema(
        origin_schema, json_data, destination_schema, schema_file, output
    )
    try:
        new_schema.map_to_data_to_new_schema()
    except Exception as e:
        if debug:
            log.error(f"EXCEPTION FOUND: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


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
@click.pass_context
def upload_to_ena(
    ctx,
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
    debug = ctx.obj.get("debug", False)
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
    try:
        upload_ena.upload()
    except Exception as e:
        if debug:
            log.error(f"EXCEPTION FOUND: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


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
@click.pass_context
def upload_to_gisaid(
    ctx,
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
    debug = ctx.obj.get("debug", False)
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
    try:
        upload_gisaid.gisaid_upload()
    except Exception as e:
        if debug:
            log.error(f"EXCEPTION FOUND: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


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
@click.pass_context
def update_db(ctx, user, password, json, type, platform, server_url, full_update):
    """upload the information included in json file to the database"""
    debug = ctx.obj.get("debug", False)
    update_database_obj = relecov_tools.upload_database.UpdateDatabase(
        user, password, json, type, platform, server_url, full_update
    )
    try:
        update_database_obj.update_db()
    except Exception as e:
        if debug:
            log.error(f"EXCEPTION FOUND: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


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
@click.option(
    "--update",
    is_flag=True,
    default=False,
    help="If the output file already exists, ask if you want to update it.",
)
@click.pass_context
def read_bioinfo_metadata(ctx, json_file, input_folder, out_dir, software_name, update):
    """
    Create the json compliant  from the Bioinfo Metadata.
    """
    debug = ctx.obj.get("debug", False)
    new_bioinfo_metadata = relecov_tools.read_bioinfo_metadata.BioinfoMetadata(
        json_file,
        input_folder,
        out_dir,
        software_name,
        update,
    )
    try:
        new_bioinfo_metadata.create_bioinfo_file()
    except Exception as e:
        if debug:
            log.error(f"EXCEPTION FOUND: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


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
@click.pass_context
def metadata_homogeneizer(ctx, institution, directory, output):
    """Parse institution metadata lab to the one used in relecov"""
    debug = ctx.obj.get("debug", False)
    new_parse = relecov_tools.metadata_homogeneizer.MetadataHomogeneizer(
        institution, directory, output
    )
    try:
        new_parse.converting_metadata()
    except Exception as e:
        if debug:
            log.error(f"EXCEPTION FOUND: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


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
    "--templates_root",
    type=click.Path(),
    help="Path to folder containing the pipeline templates from buisciii-tools",
)
@click.option(
    "-c",
    "--config",
    type=click.Path(),
    help="select the template config file",
)
@click.option("-o", "--output", type=click.Path(), help="select output folder")
@click.option(
    "-f",
    "--folder_names",
    multiple=True,
    default=None,
    help="Folder basenames to process. Target folders names should match the given dates. E.g. ... -f folder1 -f folder2 -f folder3",
)
@click.pass_context
def pipeline_manager(ctx, input, templates_root, output, config, folder_names):
    """
    Create the symbolic links for the samples which are validated to prepare for
    bioinformatics pipeline execution.
    """
    debug = ctx.obj.get("debug", False)
    new_launch = relecov_tools.pipeline_manager.PipelineManager(
        input, templates_root, output, config, folder_names
    )
    try:
        new_launch.pipeline_exc()
    except Exception as e:
        if debug:
            log.error(f"EXCEPTION FOUND: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


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
@click.option("--version", help="Specify the schema version.")
@click.option(
    "-p", "--project", help="Specficy the project to build the metadata template."
)
@click.option(
    "--non-interactive",
    is_flag=True,
    help="Run the script without user interaction, using default values.",
)
@click.option("-o", "--out_dir", type=click.Path(), help="Path to save output file/s")
@click.pass_context
def build_schema(
    ctx,
    input_file,
    schema_base,
    draft_version,
    diff,
    out_dir,
    version,
    project,
    non_interactive,
):
    """Generates and updates JSON Schema files from Excel-based database definitions."""
    debug = ctx.obj.get("debug", False)
    # Build new schema
    try:
        schema_update = relecov_tools.build_schema.SchemaBuilder(
            input_file,
            schema_base,
            draft_version,
            diff,
            out_dir,
            version,
            project,
            non_interactive,
        )

        # Build new schema
        new_schema = schema_update.handle_build_schema()
        if not new_schema:
            log.error("Schema build returned None. Skipping schema summary.")
            return

        # Generate schema summary
        schema_update.summarize_schema(new_schema)
    except Exception as e:
        if debug:
            log.error(f"Error while building schema: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


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
@click.pass_context
def logs_to_excel(ctx, lab_code, output_folder, files):
    """Creates a merged xlsx and Json report from all the log summary jsons given as input"""
    debug = ctx.obj.get("debug", False)
    all_logs = []
    full_paths = [os.path.realpath(f) for f in files]
    for file in full_paths:
        if not os.path.exists(file):
            stderr.print(f"[red]File {file} does not exist")
            log.error(f"File {file} does not exist")
            continue

        if os.path.getsize(file) == 0:
            stderr.print(f"[red]File {file} is empty")
            log.error(f"File {file} is empty")
            continue

        try:
            with open(file, "r") as f:
                content = json.load(f)
                if lab_code not in content:
                    raise KeyError(f"lab_code '{lab_code}' not found in {file}")
                all_logs.append(content[lab_code])
        except Exception as e:
            stderr.print(f"[red]Couldn't extract data from {file}: {e}")
            log.error(f"Couldn't extract data from {file}: {e}")

    if not all_logs:
        msg = f"No logs extracted. Make sure the --lab_code '{lab_code}' exists in the provided files."
        stderr.print(f"[red]{msg}")
        log.error(msg)
        raise ValueError(msg)

    logsum = relecov_tools.log_summary.LogSum(output_location=output_folder)
    try:
        merged_logs = logsum.merge_logs(key_name=lab_code, logs_list=all_logs)
        final_logs = logsum.prepare_final_logs(logs=merged_logs)
        output_filepath = os.path.join(output_folder, lab_code + "_logs_report")
        excel_outpath = output_filepath + ".xlsx"
        logsum.create_logs_excel(logs=final_logs, excel_outpath=excel_outpath)
        json_outpath = output_filepath + ".json"
        relecov_tools.utils.write_json_to_file(final_logs, json_outpath)
        print(f"[green]Successfully created logs Json in {json_outpath}")
    except Exception as e:
        if debug:
            log.error(f"EXCEPTION FOUND: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


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
@click.pass_context
def wrapper(ctx, config_file, output_folder):
    """Executes the modules in config file sequentially"""
    debug = ctx.obj.get("debug", False)
    process_wrapper = relecov_tools.dataprocess_wrapper.ProcessWrapper(
        config_file=config_file, output_folder=output_folder
    )
    try:
        process_wrapper.run_wrapper()
    except Exception as e:
        if debug:
            log.error(f"EXCEPTION FOUND: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


@relecov_tools_cli.command(help_priority=17)
@click.option("-u", "--user", help="User name for login to sftp server")
@click.option("-p", "--password", help="password for the user to login")
@click.option("-b", "--batch_id", help="Batch from....")
@click.option("-t", "--template_path", help="Path to relecov-tools templates folder")
@click.option(
    "-r", "--project", default="Relecov", help="Project to which the samples belong"
)
@click.pass_context
def upload_results(ctx, user, password, batch_id, template_path, project):
    """Upload batch results to sftp server."""
    debug = ctx.obj.get("debug", False)
    upload_sftp = relecov_tools.upload_results.UploadSftp(
        user, password, batch_id, template_path, project
    )
    try:
        upload_sftp.execute_process()
    except Exception as e:
        if debug:
            log.error(f"EXCEPTION FOUND: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


@relecov_tools_cli.command(help_priority=1)
@click.option(
    "-n",
    "--config_name",
    default=None,
    help="Name of the config key that will be added",
)
@click.option(
    "-f",
    "--config_file",
    default=None,
    help="Path to the input file: Json or Yaml format",
)
@click.option(
    "--force",
    is_flag=True,
    default=False,
    help="Force replacement of existing configuration if needed",
)
@click.option(
    "--clear_config",
    is_flag=True,
    default=False,
    help="Remove given config_name from extra config: Use with empty --config_name to remove all",
)
@click.pass_context
def add_extra_config(ctx, config_name, config_file, force, clear_config):
    """Save given file content as additional configuration"""
    debug = ctx.obj.get("debug", False)
    try:
        config_json = relecov_tools.config_json.ConfigJson(extra_config=True)
        if clear_config:
            config_json.remove_extra_config(config_name)
        else:
            config_json.include_extra_config(
                config_file, config_name=config_name, force=force
            )
    except Exception as e:
        if debug:
            log.error(f"EXCEPTION FOUND: {e}")
            raise
        else:
            sys.exit(f"EXCEPTION FOUND: {e}")


if __name__ == "__main__":
    run_relecov_tools()
