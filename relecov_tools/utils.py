import os

def file_exists(file_to_check):
    '''
    Input:
        file_to_check   # file name to check if exists
    Return:
        True if exists
    '''
    if os.path.isfile(file_to_check):
        return True
    return False


def rich_force_colors():
    """
    Check if any environment variables are set to force Rich to use coloured output
    """
    if os.getenv("GITHUB_ACTIONS") or os.getenv("FORCE_COLOR") or os.getenv("PY_COLORS"):
        return True
    return None
