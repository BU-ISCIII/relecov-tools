import logging
import os
import shutil

import relecov_tools.utils
from relecov_tools.log_summary import LogSum
from relecov_tools.config_json import ConfigJson


class BaseModule:
    """Base module Class to handle logs for all modules in the package"""

    # These variables should only be activated via CLI using --hex-code
    _global_hex_code = None
    _cli_log_file = None
    _cli_log_path_param = None
    # This indicates that there is already a process functioning,
    # therefore you should create new log handlers instead of redirecting
    _active_process = False
    _current_version = None
    _cli_command = None

    def __init__(self, output_directory: str = None, called_module: str = None):
        """Set logs output path based on the module being executed

        Args:
            output_directory (str, optional): Output folder to save called module logs. Defaults to None.
            called_module (str, optional): Name of the module being executed. Defaults to None.
        """
        if BaseModule._active_process:
            self.log = logging.getLogger(f"{__class__.__module__}.{called_module}")
        else:
            self.log = logging.getLogger()
        self.log.propagate = True
        self.log.info(f"RELECOV-tools version {BaseModule._current_version}")
        self.log.info(f"CLI command executed: {BaseModule._cli_command}")
        if called_module is None:
            called_module = self.log.name
        self.called_module = called_module.replace("relecov_tools.", "").replace(
            "-", "_"
        )
        self.batch_id = "temp_id"

        config = ConfigJson(extra_config=True)
        logs_config = config.get_configuration("logs_config")
        if self.called_module in logs_config.get("modules_outpath", {}):
            output_directory = logs_config["modules_outpath"][self.called_module]
        else:
            if output_directory is None:
                output_directory = logs_config.get(
                    "default_outpath", "/tmp/relecov_tools"
                )
        output_directory = os.path.realpath(output_directory)
        if BaseModule._global_hex_code is None:
            if BaseModule._cli_log_file:
                hex_folder = os.path.dirname(BaseModule._cli_log_file)
            else:
                hex_folder = output_directory
            self.hex = relecov_tools.utils.get_safe_hex(hex_folder)
            BaseModule._global_hex_code = self.hex
        else:
            self.hex = BaseModule._global_hex_code
        new_logname = self.tag_filename(self.called_module + ".log")
        self.final_log_path = os.path.join(output_directory, new_logname)
        if BaseModule._active_process is True:
            log_level = self.log.getEffectiveLevel()
            handler = BaseModule.set_log_handler(self.final_log_path, level=log_level)
            self.log.addHandler(handler)
        else:
            # First time this class is initialized, set root handler and move cli-log-file
            if not self.log.handlers:
                handler = BaseModule.set_log_handler(
                    self.final_log_path, level=logging.DEBUG
                )
                self.log.addHandler(handler)
            self.redirect_logs(
                self.final_log_path, old_log_path=BaseModule._cli_log_file
            )

        self.base_logsum = None
        self.basemod_outdir = output_directory
        # Set this after the first module starts in case it calls other modules
        BaseModule._active_process = True

    @staticmethod
    def set_log_handler(log_filepath, level=logging.DEBUG, only_stream=False):
        """Create a custom log handler for logging"""
        if only_stream is True:
            log_fh = logging.StreamHandler()
        else:
            os.makedirs(os.path.dirname(log_filepath), exist_ok=True)
            log_fh = logging.FileHandler(log_filepath, encoding="utf-8")
        log_fh.setLevel(level)
        log_fh.setFormatter(
            logging.Formatter(
                "[%(asctime)s] %(name)-20s [%(levelname)-7s]  %(message)s"
            )
        )
        return log_fh

    def redirect_logs(
        self,
        new_log_path: str,
        old_log_path: str = None,
    ):
        """
        Move output log file to destination while keeping track of past logs.
        Keep folder destination if self.cli_log_path is activated.
        """
        if old_log_path is None:
            log_file = self.get_log_file()
        else:
            log_file = old_log_path
        if new_log_path is None:
            if self.base_logsum is not None:
                outdir = self.base_logsum.output_location
                new_log_path = os.path.join(
                    outdir, self.tag_filename(self.called_module + ".log")
                )
            else:
                # Cannot redirect log file
                self.log.error(
                    "Could not redirect log file, no new destination nor active logsum"
                )
                return
        if log_file == new_log_path:
            self.log.debug(
                "log_file and new_log_path are the same, no redirection needed"
            )
            return
        if BaseModule._cli_log_path_param:
            self.log.warning(
                "--log-path activated via CLI. Logs output folder wont change"
            )
            new_log_path = os.path.join(
                os.path.dirname(BaseModule._cli_log_path_param),
                os.path.basename(new_log_path),
            )
        try:
            # Only copy file content, not permissions nor metadata
            shutil.copyfile(log_file, new_log_path)
            os.remove(log_file)
            self.log.debug(f"Successful copy and delete of old log-file {log_file}")
        except OSError as e:
            self.log.error(f"Could not redirect {log_file} to {new_log_path}: {e}")
            return
        for handler in self.log.handlers:
            if isinstance(handler, logging.FileHandler):
                if self.called_module in handler.baseFilename:
                    self.log.debug(f"Removing handler for {handler.baseFilename}...")
                    self.log.removeHandler(handler)
                    handler.close()

        new_handler = BaseModule.set_log_handler(
            new_log_path, level=self.log.getEffectiveLevel()
        )
        self.log.addHandler(new_handler)
        self.log.info(f"Redirected logs from {log_file} to {new_log_path}")
        return

    def get_log_file(self):
        """Retrieve the output path for the active logger"""
        module_handlers = []
        file_handlers = [
            x for x in self.log.handlers if isinstance(x, logging.FileHandler)
        ]
        for handler in file_handlers:
            if self.called_module in handler.baseFilename:
                module_handlers.append(handler)
        if len(module_handlers) == 1:
            return module_handlers[0].baseFilename
        else:
            err_txt = "Could not retrieve log-file: "
            if len(module_handlers) == 0:
                self.log.error(
                    err_txt
                    + f"No output log-file defined for module {self.called_module}"
                )
            else:
                self.log.error(
                    err_txt
                    + f"Too many handlers defined for module {self.called_module}: {module_handlers}"
                )
            return ""

    def parent_log_summary(self, *args, **kwargs):
        """Initiate relecov_tools.LogSum class with given parameters"""
        if "output_location" not in kwargs:
            kwargs["output_location"] = self.basemod_outdir
        self.base_logsum = LogSum(**kwargs)
        return self.base_logsum

    def parent_create_error_summary(self, *args, **kwargs):
        """Output log summary in the same folder as log file if filepath is not given"""
        sum_filepath = self.final_log_path.replace(".log", "_log_summary.json")
        if "filepath" not in kwargs:
            kwargs["filepath"] = sum_filepath
        self.base_logsum.create_error_summary(**kwargs)
        return

    def set_batch_id(self, batch_id):
        """Set batch_id variable and rename log file to include this tag"""
        if self.batch_id == "temp_id":
            self.log.debug(f"Setting batch_id as {batch_id}...")
            self.batch_id = str(batch_id)
        self.final_log_path = self.tag_filename(self.final_log_path).replace(
            "_temp_id", ""
        )
        self.redirect_logs(self.final_log_path)
        return

    def tag_filename(self, filename: str):
        """Include hexadecimal code and batch_id in filename to avoid duplication"""
        tag = self.batch_id + "_" + self.hex
        base_name, extension = os.path.splitext(filename)
        for name, mark in {"batch_id": self.batch_id, "hex code": self.hex}.items():
            if mark in base_name:
                base_name = base_name.replace("_" + mark, "")
                self.log.debug(f"{filename} already includes {name}: {mark}.")
        return str(base_name) + "_" + tag + str(extension)
