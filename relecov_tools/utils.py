#!/usr/bin/env python
"""
Common utility function for relecov_tools package.
"""
import os
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


def get_md5_from_local_folder(local_folder, file_list):
    """Fetch the md5 values for each file in the file list"""
    md5_results = {}
    for file_name in file_list:
        f_name, f_ext = os.path.splitext(file_name)
        if f_ext.lowercase() == ".md5":
            file_md5_path = os.path.join(local_folder, file_name)
            # file_origin_path = os
            fh = open(file_path, "r")
            md5_results[file_name]
        if file_exists(file_path):
            pass


def calculate_md5(files_list):
    """Calculate the md5 value for the list of files"""
    block_size = 2**20
    hash_md5 = hashlib.md5()
    for file_list in files_list:
        f_name, f_ext = os.path.splitext(file_list)
        if not f_ext.lowercase() == ".md5":
            hash_md5.update(open(file_list, "rb").read(block_size))
    return hash_md5.hexdigest()


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
