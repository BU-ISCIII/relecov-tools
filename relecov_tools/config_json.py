#!/usr/bin/env python
import json
import os
import yaml
import logging
import copy

import relecov_tools.utils

log = logging.getLogger(__name__)


# pass test
class ConfigJson:
    """
    ----------------------------------------------------------------------------
    Purpose
    -------
    Load *configuration.json* (defaults) and, optionally, the user
    *extra_config.json* (overrides). Internally we normalise everything to the
    dual-level layout **params / commands**:

        {
          "download": {
              "params": {                    # ← defaults (configuration.json)
                  "threads": 4,
                  "output_dir": "/default/path"
              },
              "commands": {                  # ← overrides (extra_config.json)
                  "threads": 8
              }
          },
          "generic": {...},
          ...
        }

    Helper methods
    --------------
    • ``get_configuration(topic, raw=False)``
        * **raw=False**  → flat dict where *commands overwrite params*
          (ideal to feed modules directly).
        * **raw=True**   → the nested block shown above.

    • ``get_topic_data(topic, key)``
        Returns a *single* value following the priority
        **commands > params > recursive search** >>> legacy flat section.

    ----------------------------------------------------------------------------
    """

    # TODO: Make this path configurable too
    _extra_config_path = os.path.expanduser("~/.relecov_tools/extra_config.json")

    def __init__(
        self,
        json_file=os.path.join(os.path.dirname(__file__), "conf", "configuration.json"),
        extra_config=False,
    ):
        """Load config content in configuration.json and additional config if required

        Parameters
        ----------
        json_file : str
            Path to the *defaults* file (``configuration.json``).
        extra_config : bool
            If *True* merge in the user's overrides located at
            ``~/.relecov_tools/extra_config.json``.
        """
        # ── 1. Load defaults ------------------------------------------------
        with open(json_file, "r", encoding="utf-8") as fh:
            self.base_conf = json.load(fh)
        # ── 2. Optionally load user overrides -------------------------------
        extra_conf, active_extra = {}, False
        if extra_config and os.path.isfile(ConfigJson._extra_config_path):
            try:
                with open(ConfigJson._extra_config_path, "r", encoding="utf-8") as fh:
                    extra_conf = json.load(fh)
                active_extra = True
            except (OSError, json.JSONDecodeError) as e:
                log.warning(f"Could not load extra config: {e}. Using default instead")
        elif extra_config:
            log.warning(
                f"Could not load extra config: {ConfigJson._extra_config_path} does not exist. "
                "Using default instead"
            )
            log.warning(
                "Run `relecov-tools add-extra-config` to include additional configuration"
            )

        # ── 3. Merge defaults + overrides into params/args ---------------
        self.json_data = self._nested_merge_with_args(self.base_conf, extra_conf)
        missing_required = self.validate_configuration(self.json_data)
        if missing_required:
            log.error(f"Could not validate current configuration. Missing required config: {missing_required}")
            raise ValueError(f"Required configuration missing in current config: {missing_required}")
        log.debug(
            "Loaded additional configuration."
            if active_extra
            else "Running with default configuration."
        )
        self.topic_config = list(self.json_data.keys())

    def get_configuration(self, topic: str, *, raw: bool = False):
        """
        Return the configuration block for *topic*.

        Examples
        --------
        >>> cfg = ConfigJson(extra_config=True)
        >>> cfg.get_configuration("download")
        {'threads': 8, 'output_dir': '/default/path'}          # flat view

        >>> cfg.get_configuration("download", raw=True)
        {'params': {'threads': 4, 'output_dir': '/default/path'},
         'commands': {'threads': 8}}                          # nested view
        """
        # ── 1. Topic not present ───────────────────────────────────────────
        if topic not in self.json_data:
            return None

        block = self.json_data[topic]

        # ── 2. Caller wants the nested structure as is ─────────────────────
        if raw:
            return block

        # ── 3. Layout: merge params + commands  (commands win) ─────────
        if isinstance(block, dict) and ("params" in block or "commands" in block):
            flattened = dict(block.get("params", {}))  # defaults
            flattened.update(block.get("commands", {}))  # overrides
            return flattened

        return block

    def get_topic_data(self, topic: str, found: str):
        """
        Fetch a single value *found* from *topic*.

        Search priority
        ----------------
        1. **commands** – overrides
        2. **params**   – defaults
        3. Recursive search inside both dicts
        4. Legacy flat section (back-compat)

        Example
        -------
        >>> cfg.get_topic_data("download", "threads")   # → 8
        >>> cfg.get_topic_data("download", "output_dir")  # → '/default/path'
        """

        # ── Helper: depth-first search in nested dicts ──────────────────────
        def _recursive_lookup(node, key):
            """Searches for `key` at any level of nested dictionaries."""
            if not isinstance(node, dict):
                return None
            for k, v in node.items():
                if k == key:
                    return v
                if isinstance(v, dict):
                    res = _recursive_lookup(v, key)
                    if res is not None:
                        return res
            return None

        # ── 1. Find the topic block ─────────────────────────────────────────
        topic_block = self.json_data.get(topic)
        if topic_block is None:
            return None

        # ── 2. New layout (params / commands) ───────────────────────────────
        if isinstance(topic_block, dict) and (
            "params" in topic_block or "commands" in topic_block
        ):
            # 2.1  direct hit in commands  (highest priority)
            if found in topic_block.get("commands", {}):
                return topic_block["commands"][found]
            # 2.2  direct hit in params   (defaults)
            if found in topic_block.get("params", {}):
                return topic_block["params"][found]
            # 2.3  recursive search (first commands, then params)
            return _recursive_lookup(
                topic_block.get("commands", {}), found
            ) or _recursive_lookup(topic_block.get("params", {}), found)

        # ── 3. Legacy flat section ──────────────────────────────────────────
        if found in topic_block:
            return topic_block[found]

        # ── 4. Legacy with deeper nesting ───────────────────────────────────
        return _recursive_lookup(topic_block, found)
    
    def validate_configuration(self, config_dict: dict):
        """Validate the given configuration dictionary, preferably after merge.
        
        Args:
            config_name (dict): Dictionary containing all the configuration from
            the JSON or YAML file.

        Raises:
            ValueError: If any there is any required field missing.
        
        Returns:
            missing_required (list): List of the missing configuration fields.
        """
        def recursive_validation(deep_conf: dict, parent: str):
            """Recursively check if required keys are present in given config"""
            missing = []
            required_list = deep_conf.get(req_key, [])
            for req in required_list:
                if deep_conf.get(req, "") == "":
                    # No he usado `not` porque 0 y False son valores válidos
                    new_parent = ".".join([parent, req]) if parent else req
                    missing.append(new_parent)
            for key, val in deep_conf.items():
                if isinstance(val, dict):
                    new_parent = ".".join([parent, key]) if parent else key
                    missing.extend(recursive_validation(val, new_parent))
            return missing

        req_key = "required_conf"
        glob_required = config_dict.pop(req_key) if req_key in config_dict else []
        log.debug(f"Starting config validation for given keys: {config_dict.keys()}")
        missing_required = [x for x in glob_required if not config_dict.get(x)]
        for key in config_dict.keys():
            conf_data = config_dict[key]
            if "params" in conf_data and "commands" in conf_data:
                # Its parsed from _nested_merge_with_args() so config is inside params
                missing_required.extend(
                    recursive_validation(config_dict[key]["params"], parent=key)
                )
            else:
                missing_required.extend(
                    recursive_validation(config_dict[key], parent=key)
                )
        return missing_required

    def insert_new_config(self, config_name, current_conf, force=False):
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
    
    def merge_config(self, config_dict, new_config, force=True):
        """
        Recursively merge a new configuration dictionary into an existing one.

        This method updates ``config_dict`` with values from ``new_config`` while:
            - Preserving existing keys unless specified with **force**
            - Recursively merging nested dictionaries
            - Tracking which changes were applied or canceled
            - Delegating overwrite decisions to ``self.insert_new_config``

        Args:
            config_dict (dict):
                The original configuration dictionary to update.

            new_config (dict):
                The new configuration values to merge into the original dictionary.

            force (bool, optional):
                If True, allows overwriting existing values. Defaults to True.

        Returns:
            tuple:
                - merged_dict (dict): The updated configuration dictionary.
                - summary (dict): A dictionary summarizing merge actions with keys:
                    * "Included": list of applied changes
                    * "Canceled": list of rejected changes
        """        
        def _rec_merge(current_conf, new_conf, force=False, parent=""):
            """Recursively add new configuration without deleting the existing keys"""
            for k, v in new_conf.items():
                new_parent = ".".join([parent, k]) if parent else k
                if k in current_conf and current_conf[k] == v:
                    continue
                if isinstance(v, dict) and k in current_conf and isinstance(current_conf[k], dict):
                    _rec_merge(current_conf[k], v, force=force, parent=new_parent)
                else:
                    change_msg = f"{new_parent}: {current_conf.get(k, '')} -> {v}"
                    if not self.insert_new_config(k, current_conf, force=force):
                        summary["Canceled"].append(change_msg)
                        continue
                    if k == "required_conf":
                        try:
                            updated_list = list(set(current_conf.get(k, []) + v))
                            change_msg = f"{new_parent}: {current_conf.get(k, '')} -> {updated_list}"
                            current_conf[k] = updated_list
                        except TypeError:
                            log.error(
                                f"Skipped {new_parent}. It should be a list instead of {type(v)}"
                            )
                    else:
                        current_conf[k] = v
                    summary["Included"].append(change_msg)
            return current_conf

        summary = {"Canceled": [], "Included": []}
        merged_dict = _rec_merge(config_dict, new_config, force=force)
        return merged_dict, summary
        

    def include_extra_config(self, config_file, config_name=None, force=False):
        """Include given file content as additional configuration for later usage.

        Args:
            config_name (str): Name of the config key that will be added.
            config_file (str): Path to the input file, either Json or Yaml format.
            force (bool, optional): Force replacement of existing configuration if needed.

        Raises:
            ValueError: If provided config_file does not have a supported extension.
        """

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
                additional_config, summary = self.merge_config(additional_config, file_content, force=force)
            else:
                if self.insert_new_config(config_name, self.json_data, force=force):
                    msg = f"{config_name}: {file_content}"
                    summary["Included"].append(msg)
                    additional_config.update({config_name: file_content})
        else:
            if config_name is None:
                additional_config = file_content
            else:
                additional_config = {config_name: file_content}
            summary["Included"].extend(list(additional_config.keys()))
        full_test_conf, _ = self.merge_config(
            copy.deepcopy(self.base_conf), additional_config, force=force
        )
        missing_required = self.validate_configuration(full_test_conf)
        if missing_required:
            log.error(f"Could not validate incoming extra config. Missing required config: {missing_required}")
            raise ValueError(f"Required configuration missing from incoming config: {missing_required}")
        relecov_tools.utils.write_json_to_file(
            additional_config, ConfigJson._extra_config_path
        )
        log.info("Finished including extra configuration")
        print("Update summary:")
        for state, changes in summary.items():
            print(state, ":\n", "\n".join([str(msg) for msg in changes]))
        return

    def remove_extra_config(self, topic=None, deep=None, force=False):
        """Remove key from extra_config configuration file.

        Args:
            topic (str | None): top-level key
            deep (str | None): nested key to remove
            force (bool): if False, ask for confirmation
        """

        if topic is None and deep is None:
            # Remove extra_config.json file
            if not force:
                confirm = input("Remove entire extra_config file? [y/N]: ")
                if confirm.lower() != "y":
                    print("Aborted.")
                    return

            try:
                os.remove(ConfigJson._extra_config_path)
                log.info("Removed extra config file.")
            except OSError as e:
                log.error(f"Could not remove extra config file: {e}")
            return

        with open(ConfigJson._extra_config_path, "r") as fh:
            additional_config = json.load(fh)

        removed_paths = []

        def recursive_remove(d, key_to_remove, current_path=""):
            """Recursively search and remove configuration"""
            removed = False

            if isinstance(d, dict):
                keys_to_delete = []
                for k, v in d.items():
                    path = f"{current_path}.{k}" if current_path else k

                    if k == key_to_remove:
                        keys_to_delete.append((path, k))
                        removed = True
                    else:
                        if recursive_remove(v, key_to_remove, path):
                            removed = True

                for tup in keys_to_delete:
                    path = tup[0]
                    k = tup[1]
                    if not force:
                        confirm = input(f"Remove '{path}'? [y/N]: ")
                        if confirm.lower() != "y":
                            print(f"Skipped {path}.")
                            continue
                    d.pop(k)
                    removed_paths.append(path)

            elif isinstance(d, list):
                for idx, item in enumerate(d):
                    path = f"{current_path}[{idx}]"
                    if recursive_remove(item, key_to_remove, path):
                        removed = True

            return removed

        if topic and deep is None:
            # Only remove topic
            if topic not in additional_config:
                log.error(f"Main key `{topic}` not found in extra_config")
                return

            if not force:
                confirm = input(f"Remove entire config '{topic}'? [y/N]: ")
                if confirm.lower() != "y":
                    print("Aborted.")
                    return

            additional_config.pop(topic)
            removed_paths.append(topic)

        elif topic and deep:
            if topic not in additional_config:
                log.error(f"Main key `{topic}` not found in extra_config")
                return
            # Try to find config deep inside topic
            recursive_remove(additional_config[topic], deep, topic)

        elif topic is None and deep:
            # Try to find deep anywhere, even for multiple matches
            recursive_remove(additional_config, deep)

        if not removed_paths:
            log.warning(f"No matches found for topic={topic}, deep={deep}")
            return

        # ---- write updated file ----
        relecov_tools.utils.write_json_to_file(
            additional_config,
            ConfigJson._extra_config_path
        )

        log.info(f"Removed {len(removed_paths)} key(s)")
        print(f"Finished clearing extra config. Removed: {removed_paths}")

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

    def _nested_merge_with_args(self, base_conf: dict, extra_conf: dict) -> dict:
        """
        Produce the *params / args* structure described in the class docstring.

        • Everything from 'args' in *extra_config.json* → **commands**
        • Everything else *configuration.json* or *extra_config.json* → **params**
          (relocating the key to its first-level parent if necessary).
        • Note: Data from *extra_config.json* will override config in *configuration.json*
        """
        merged = {}

        base_reqs = base_conf.pop("required_conf") if "required_conf" in base_conf else []
        extra_reqs = extra_conf.pop("required_conf") if "required_conf" in extra_conf else []
        merged_reqs = list(set(base_reqs + extra_reqs))
        merged["required_conf"] = merged_reqs

        for key, val in base_conf.items():
            merged[key] = {"params": val, "commands": {}}

        for key, val in extra_conf.items():
            val_args = val.get("args", [])
            if key in merged:
                merged[key]["commands"] = val_args
                current_params = copy.deepcopy(merged[key]["params"])
                merged[key]["params"], _ = self.merge_config(current_params, val)
            else:
                merged[key] = {"params": val, "commands": val_args}
        return merged
