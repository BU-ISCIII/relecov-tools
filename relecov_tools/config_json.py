#!/usr/bin/env python
import json
import os
import yaml
import logging

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
            base_conf = json.load(fh)

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

        # ── 3. Build an index  <leaf-key → first-level-parent> ---------------
        #    Needed to relocate overrides that appear outside their branch
        self._leaf_parent = {}

        def _index_parents(node: dict, top_key: str):
            if not isinstance(node, dict):
                return
            for k, v in node.items():
                self._leaf_parent.setdefault(k, top_key)
                _index_parents(v, top_key)

        for first_level_key, subtree in base_conf.items():
            _index_parents(subtree, first_level_key)

        # ── 4. Merge defaults + overrides into params/commands ---------------
        self.json_data = self._nested_merge_with_commands(base_conf, extra_conf)

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

    def _nested_merge_with_commands(self, base_conf: dict, extra_conf: dict) -> dict:
        """
        Produce the *params / commands* structure described in the class docstring.

        • Everything from *configuration.json* → **params**
        • Everything from *extra_config.json* → **commands**
          (relocating the key to its first-level parent if necessary).
        """
        merged = {}

        for key, val in base_conf.items():
            merged[key] = {"params": val, "commands": {}}

        for key, val in extra_conf.items():
            if key in merged:
                merged[key]["commands"] = val
            elif key in self._leaf_parent:
                parent = self._leaf_parent[key]
                merged.setdefault(parent, {"params": {}, "commands": {}})
                merged[parent]["commands"][key] = val
            else:
                merged[key] = {"params": {}, "commands": val}

        return merged
