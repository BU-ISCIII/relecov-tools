#!/usr/bin/env python
import json
import os
import yaml
import logging

import relecov_tools.utils

log = logging.getLogger(__name__)


# pass test
class ConfigJson:
    # TODO: Make this path configurable too
    _extra_config_path = os.path.expanduser("~/.relecov_tools/extra_config.json")

    def __init__(
        self,
        json_file=os.path.join(os.path.dirname(__file__), "conf", "configuration.json"),
        extra_config=False,
    ):
        """Load config content in configuration.json and additional config if required

        Args:
            json_file (str, optional): config filepath.
            extra_config (bool, optional): Include content from ~/.relecov_tools/extra_config.json.
        """
        with open(json_file, "r", encoding="utf-8") as fh:
            self.json_data = json.load(fh)

        active_extra_conf = False
        if extra_config:
            if os.path.isfile(ConfigJson._extra_config_path):
                try:
                    with open(
                        ConfigJson._extra_config_path, "r", encoding="utf-8"
                    ) as add_fh:
                        additional_conf = json.load(add_fh)
                    self.json_data.update(additional_conf)
                    active_extra_conf = True
                except (OSError, json.JSONDecodeError) as e:
                    log.warning(
                        f"Could not load extra config: {e}. Using default instead"
                    )
            else:
                log.warning(
                    f"Could not load extra config: {ConfigJson._extra_config_path} does not exist. Using default instead"
                )
                log.warning(
                    "Run ``relecov-tools add-extra-config`` to include additional configuration"
                )
        if not active_extra_conf:
            log.debug("Running with default configuration.")
        else:
            log.debug("Loaded additional configuration.")
        self.topic_config = list(self.json_data.keys())

    def get_configuration(self, topic):
        """Obtain the topic configuration from json data"""
        if topic in self.topic_config:
            return self.json_data[topic]
        return None

    def get_topic_data(self, topic, found):
        """Obtain from topic any forward items from json data"""

        def get_recursive(subtopic, found):
            if isinstance(subtopic, dict):
                for key, val in subtopic.items():
                    if key == found:
                        return val
                    elif isinstance(val, dict):
                        result = get_recursive(val, found)
                        if result is not None:
                            return result
            else:
                return None

        if topic not in self.json_data.keys():
            return None
        if found in self.json_data[topic]:
            return self.json_data[topic][found]
        else:
            return get_recursive(self.json_data[topic], found)

    def include_extra_config(self, config_file, config_name=None, force=False):
        """Include given file content as additional configuration for later usage.

        Args:
            config_name (str): Name of the config key that will be added.
            config_file (str): Path to the input file, either Json or Yaml format.
            force (bool, optional): Force replacement of existing configuration if needed.

        Raises:
            ValueError: If provided config_file does not have a supported extension.
        """

        def validate_new_config(config_name, current_conf, force=False):
            """Check if config_name is already in configuration"""
            if config_name in current_conf.keys():
                if force:
                    log.info(
                        f"`{config_name}` already in config. Replacing its content..."
                    )
                    return True
                else:
                    err_txt = f"Cannot add `{config_name}`: already in config. Set `force` to force replacement"
                    log.error(err_txt)
                    return False
            else:
                return True

        def rec_merge_config(current_conf, new_conf, force=False):
            """Recursively add new configuration without deleting the existing keys"""
            for k, v in new_conf.items():
                if isinstance(v, dict) and k in current_conf:
                    rec_merge_config(current_conf[k], v, force=force)
                else:
                    change_msg = f"{k}: {current_conf.get(k, '')} -> {v}"
                    if not validate_new_config(k, current_conf, force=force):
                        summary["Canceled"].append(change_msg)
                        continue
                    summary["Included"].append(change_msg)
                    current_conf[k] = v

        if not os.path.isfile(str(config_file)):
            raise FileNotFoundError(f"Extra config file {config_file} does not exist")
        if os.path.isfile(ConfigJson._extra_config_path):
            with open(ConfigJson._extra_config_path, "r", encoding="utf-8") as f:
                additional_config = json.load(f)
        else:
            additional_config = None

        summary = {"Included": [], "Canceled": []}
        valid_exts = (".json", ".yaml", ".yml")
        os.makedirs(os.path.dirname(ConfigJson._extra_config_path), exist_ok=True)
        with open(config_file, "r") as fh:
            if config_file.endswith(".json"):
                file_content = json.load(fh)
            elif config_file.endswith((".yaml", ".yml")):
                file_content = yaml.load(fh, Loader=yaml.FullLoader)
            else:
                raise ValueError(
                    f"config_file {config_file} extension is not supported. Use: {valid_exts}"
                )
        if additional_config is not None:
            if config_name is None:
                rec_merge_config(additional_config, file_content, force=force)
            else:
                if validate_new_config(config_name, self.json_data, force=force):
                    msg = f"{config_name}: {file_content}"
                    summary["Included"].append(msg)
                    additional_config.update({config_name: file_content})
        else:
            if config_name is None:
                additional_config = file_content
            else:
                additional_config = {config_name: file_content}
            summary["Included"].extend([k for k in additional_config.keys()])
        relecov_tools.utils.write_json_to_file(
            additional_config, ConfigJson._extra_config_path
        )
        log.info("Finished including extra configuration")
        print("Update summary:")
        for state, changes in summary.items():
            print(state, ":\n", "\n".join([str(msg) for msg in changes]))
        return

    def remove_extra_config(self, config_name):
        """Remove key from extra_config configuration file

        Args:
            config_name (str): _description_
        """
        if config_name is None:
            try:
                os.remove(ConfigJson._extra_config_path)
                log.info("Removed extra config file.")
            except OSError as e:
                log.error(f"Could not remove extra config file: {e}")
                return
        else:
            with open(ConfigJson._extra_config_path, "r") as fh:
                additional_config = json.load(fh)
            try:
                additional_config.pop(config_name)
            except KeyError:
                log.error(f"{config_name} not found in extra_config for removal")
                return
            except OSError as e:
                log.error(f"Could not remove {config_name} key: {e}")
                return
            relecov_tools.utils.write_json_to_file(
                additional_config, ConfigJson._extra_config_path
            )
            log.info(f"Successfully removed {config_name} from extra config")
        print(f"Finished clearing extra config: {config_name}")
        return

    def get_lab_code(self, submitting_institution):
        """Get the corresponding code for the given submitting institution"""
        if submitting_institution is None:
            log.warning("No submitting institution could be found to update lab_code")
            return
        institutions_config = self.get_configuration("institutions_config")
        if not institutions_config:
            log.warning("No institutions_config found. Could not extract lab_code")
            return
        for code, conf in institutions_config.items():
            if submitting_institution in conf.get("institution_name"):
                return code
        else:
            log.warning(f"{submitting_institution} not found in institutions_config")
        return
