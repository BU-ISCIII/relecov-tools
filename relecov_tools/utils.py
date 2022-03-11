#!/usr/bin/env python
"""
Common utility function for relecov_tools package.
"""
import os
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
    stderr.print("Temporal directory destination to execute sercive")
    source = questionary.path("Source path").unsafe_ask()
    return source


def prompt_source_path():
    stderr.print("Directory containing files cd to transfer")
    source = questionary.path("Source path").unsafe_ask()
    return source


def prompt_destination_path():
    stderr.print("Directory to which the files will be transfered")
    destination = questionary.path("Destination path").unsafe_ask()
    return destination


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
