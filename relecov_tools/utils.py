#!/usr/bin/env python
"""
Common utility function for relecov_tools package.
"""
import os
import glob
import hashlib
from rich.console import Console
import questionary


def file_exists(file_to_check):
    """
    Input:
        file_to_check   # file name to check if exists
    Return:
        True if exists
    """
    if os.path.isfile(file_to_check):
        return True
    return False


def get_md5_from_local_folder(local_folder):
    """Fetch the md5 values for each file in the file list"""
    md5_results = {}
    reg_for_md5 = os.path.join(local_folder, "*.md5")
    reg_for_non_md5 = os.path.join(local_folder, "*[!.md5]")
    md5_files = glob.glob(reg_for_md5)
    if len(md5_files) > 0:
        # find the md5 files and link them to the original file
        non_md5_files = glob.glob(reg_for_non_md5)
        for file_name in md5_files:
            f_name, f_ext = os.path.splitext(file_name)
            f_related_md5 = [v for v in non_md5_files if f_name in v]
            if len(f_related_md5) == 1:
                file_md5_path = os.path.join(local_folder, file_name)
                fh = open(file_md5_path, "r")
                md5_results[f_related_md5[0]] = fh.read()
                fh.close()
    return md5_results


def calculate_md5(file_name):
    """Calculate the md5 value for the file name"""
    block_size = 2**20
    return hashlib.md5(open(file_name, "rb").read(block_size)).hexdigest()


def write_md5_file(file_name, md5_value):
    """Write md5 to file"""
    with open(file_name, "wb") as fh:
        fh.write(md5_value)
    return


def create_md5_files(file_list):
    """Create the md5 files and return their value"""
    md5_results = {}
    for file_name in file_list:
        f_name, f_ext = os.path.splitext(file_name)
        md5_results[os.path.base_name(f_name)] = calculate_md5(file_name)
        md5_file_name = f_name + ".md5"
        write_md5_file(md5_file_name, md5_results[os.path.base_name(f_name)])
    return md5_results


def save_local_md5(file_name, md5_value):
    """Save the MD5 value"""
    with open(file_name, "w") as fh:
        fh.write(md5_value)
    return True


def rich_force_colors():
    """
    Check if any environment variables are set to force Rich to use coloured output
    """
    if (
        os.getenv("GITHUB_ACTIONS")
        or os.getenv("FORCE_COLOR")
        or os.getenv("PY_COLORS")
    ):
        return True
    return None


stderr = Console(
    stderr=True, style="dim", highlight=False, force_terminal=rich_force_colors()
)


def prompt_text(msg):
    source = questionary.text(msg).ask()
    return source


def prompt_password(msg):
    source = questionary.password(msg).ask()
    return source


def prompt_tmp_dir_path():
    stderr.print("Temporal directory destination to execute service")
    source = questionary.path("Source path").unsafe_ask()
    return source


def prompt_selection(msg, choices):
    selection = questionary.select(msg, choices=choices).unsafe_ask()
    return selection


def prompt_path(msg):
    source = questionary.path(msg).unsafe_ask()
    return source


def prompt_yn_question(msg):
    confirmation = questionary.confirm(msg).unsafe_ask()
    return confirmation


def prompt_skip_folder_creation():
    stderr.print("Do you want to skip folder creation? (Y/N)")
    confirmation = questionary.confirm("Skip?", default=False).unsafe_ask()
    return confirmation
