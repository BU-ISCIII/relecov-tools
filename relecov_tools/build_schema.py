#!/usr/bin/env python
import rich.console
import pandas as pd
import os
import re
import openpyxl
import sys
import json
import difflib
import inspect

import relecov_tools.utils
import relecov_tools.assets.schema_utils.jsonschema_draft
import relecov_tools.assets.schema_utils.metadatalab_template
from relecov_tools.config_json import ConfigJson
from relecov_tools.base_module import BaseModule
from datetime import datetime
from openpyxl.worksheet.datavalidation import DataValidation

pd.set_option("future.no_silent_downcasting", True)

stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class BuildSchema(BaseModule):
    def __init__(
        self,
        input_file=None,
        schema_base=None,
        excel_template=None,
        draft_version=None,
        diff=False,
        output_dir=None,
        version=None,
        project=None,
        non_interactive=False,
    ):
        """
        Initialize the SchemaBuilder class. This class generates a JSON Schema file based on the provided draft version.
        It reads the database definition from an Excel file and allows customization of the schema generation process.
        """
        super().__init__(output_dir=output_dir, called_module=__name__)
        self.excel_file_path = input_file
        self.non_interactive = non_interactive
        # Validate params
        if not self.excel_file_path or not os.path.isfile(self.excel_file_path):
            self.log.error("A valid Excel file path must be provided.")
            raise ValueError("A valid Excel file path must be provided.")
        if not self.excel_file_path.endswith(".xlsx"):
            self.log.error("The Excel file must have a .xlsx extension.")
            raise ValueError("The Excel file must have a .xlsx extension.")

        # No metadata is being processed so batch_id will be execution date
        self.set_batch_id(self.basemod_date)

        # Validate output folder creation
        if not output_dir:
            self.output_dir = relecov_tools.utils.prompt_create_outdir()
        else:
            self.output_dir = os.path.abspath(output_dir)
            if not os.path.exists(self.output_dir):
                self.output_dir = relecov_tools.utils.prompt_create_outdir()

        # Get version option
        if not version:
            # If not defined, then ask via prompt
            self.version = relecov_tools.utils.prompt_text(
                "Write the desired version using semantic versioning:"
            )
        else:
            self.version = version
        if not relecov_tools.utils.validate_semantic_version(self.version):
            raise ValueError("[red]Error: Invalid version format")

        # Validate show diff option
        if diff is False:
            self.show_diff = None
        else:
            self.show_diff = True

        # Validate json schema draft version
        if not draft_version and self.non_interactive:
            self.draft_version = "2020-12"
        else:
            self.draft_version = (
                relecov_tools.assets.schema_utils.jsonschema_draft.check_valid_version(
                    draft_version
                )
            )

        # Get version option
        # Parse build-schema configuration
        self.build_schema_json_file = os.path.join(
            os.path.dirname(__file__), "conf", "build_schema_config.json"
        )

        if project is None:
            project = relecov_tools.utils.prompt_text("Write the desired project:")

        self.project = project

        available_projects = self.get_available_projects(self.build_schema_json_file)

        # Get collecting institutions and dropdown list
        self._lab_dropdowns, self._lab_uniques = self._load_laboratory_addresses()

        # Config params
        config_build_schema = ConfigJson(self.build_schema_json_file)
        config_data = config_build_schema.get_configuration("projects") or {}
        self.configurables = (
            config_build_schema.get_configuration("configurables") or {}
        )
        config_json = ConfigJson()

        if self.project in available_projects:
            self.project_config = config_data.get(self.project, {})
        else:
            self.log.error(
                f"No configuration available for '{self.project}'. Available projects: {', '.join(available_projects)}"
            )
            stderr.print(
                f"[red]No configuration available for '{self.project}'. Available projects: {', '.join(available_projects)}"
            )
            raise ValueError(
                f"No configuration available for '{self.project}'. Available projects: {', '.join(available_projects)}"
            )

        # Validate base schema
        if schema_base is not None:
            if relecov_tools.utils.file_exists(schema_base):
                self.base_schema_path = schema_base
            else:
                self.log.error(
                    f"[Error]Defined base schema file not found: {schema_base}."
                )
                stderr.print(
                    f"[Error]Defined base schema file not found: {schema_base}. Exiting..."
                )
                raise FileNotFoundError(
                    f"Defined base schema file not found: {schema_base}."
                )
        else:
            try:
                relecov_schema = config_json.get_topic_data("generic", "relecov_schema")
            except KeyError as key_error:
                self.log.error(f"Configuration key error: {key_error}")
                stderr.print(f"[orange]Configuration key error: {key_error}")
                raise

            try:
                self.base_schema_path = os.path.join(
                    os.path.dirname(os.path.realpath(__file__)),
                    "schema",
                    relecov_schema,
                )
            except FileNotFoundError as fnf_error:
                self.log.error(f"Configuration file not found: {fnf_error}")
                stderr.print(f"[red]Configuration file not found: {fnf_error}")
                raise

            if not relecov_tools.utils.file_exists(self.base_schema_path):
                self.log.error(
                    "[Error]Fatal error. Relecov schema were not found in current relecov-tools installation. Make sure relecov-tools command is functioning."
                )
                stderr.print(
                    "[Error]Fatal error. Relecov schema were not found in current relecov-tools installation. Make sure relecov-tools command is functioning. Exiting..."
                )
                raise FileNotFoundError(
                    "Fatal error. Relecov schema were not found in current relecov-tools installation. Make sure relecov-tools command is functioning."
                )

            self.log.info("RELECOV schema successfully found in the configuration.")
            stderr.print(
                "[green]RELECOV schema successfully found in the configuration."
            )

            # TODO: What if no previous template exist?
            if excel_template:
                self.excel_template = excel_template
            else:
                try:
                    excel_template_path = os.path.join(
                        os.path.dirname(os.path.realpath(__file__)), "assets"
                    )
                    # FIXME: filenames should inherit project name.
                    excel_template = [
                        f
                        for f in os.listdir(excel_template_path)
                        if f.startswith("Relecov_metadata_template")
                    ]
                    if len(excel_template) > 1:
                        self.log.error(
                            "[Error]Fatal error. More than one excel template was found in current relecov-tools installation (assets)"
                        )
                        stderr.print(
                            "[Error]Fatal error.More than one excel template was found in current relecov-tools installation (assets)..Exiting"
                        )
                        raise FileExistsError(
                            "Fatal error. More than one excel template was found in current relecov-tools installation (assets)"
                        )

                    self.excel_template = os.path.join(
                        excel_template_path, excel_template[0]
                    )

                except (FileNotFoundError, IndexError):
                    self.log.error(
                        "[Error]Fatal error. Excel template was not found in current relecov-tools installation (assets)"
                    )
                    stderr.print(
                        "[Error]Fatal error. Excel template not found in current relecov-tools installation (assets). Exiting..."
                    )
                    raise

    def _load_laboratory_addresses(self):
        """
        Returns two dictionaries with key in the three special fields:
        - dropdowns[field] ........ list ‘<name> [<city>] [<ccn>]’
        - uniques[field] .......... unique names for schema enum
        """
        json_path = os.path.join(
            os.path.dirname(__file__),
            "conf",
            "laboratory_address.json",
        )
        with open(json_path, encoding="utf-8") as fh:
            lab_data = json.load(fh)

        fields = [
            "collecting_institution",
            "submitting_institution",
            "sequencing_institution",
        ]
        dropdowns = {f: [] for f in fields}
        uniques = {f: set() for f in fields}

        for ccn, info in lab_data.items():
            city = info.get("geo_loc_city", "").strip()

            for f in fields:
                name = info.get(f, "").strip()
                if name:
                    dropdowns[f].append(f"{name} [{city}] [{ccn}]")
                    uniques[f].add(name)

        dropdowns = {k: sorted(v) for k, v in dropdowns.items()}
        uniques = {k: sorted(v) for k, v in uniques.items()}
        return dropdowns, uniques

    def validate_database_definition(self, json_data):
        """Validate the mandatory features and ensure:
        - No duplicate enum values in the JSON schema.
        - All fields have an example.
        - All examples should have the same type as JSON schema.
        - Date formats follow 'YYYY-MM-DD'.

        Args:
            json_data (dict): The JSON data representing the database definition.

        Returns:
            dict: A dictionary containing errors found, categorized by:
                - Missing features
                - Duplicate enums
                - Missing examples
                - Invalid example types
                - Incorrect date formats
        """
        log_errors = {
            "missing_features": {},
            "missing_examples": {},
            "duplicate_enums": {},
            "invalid_example_types": {},
            "invalid_date_formats": {},
        }

        mandatory_features = [
            "enum",
            "examples",
            "ontology_id",
            "type",
            "description",
            "classification",
            "label_name",
            "fill_mode",
            "required (Y/N)",
            "complex_field (Y/N)",
        ]

        # Iterate over properties in json_data
        for prop_name, prop_features in json_data.items():
            missing_features = [
                feature
                for feature in mandatory_features
                if feature not in prop_features
            ]
            if missing_features:
                log_errors["missing_features"][prop_name] = missing_features

            # Check for duplicate enum values
            if prop_features.get("enum"):
                if not pd.isna(prop_features["enum"]):
                    enum_values = prop_features["enum"].split("; ")
                    # Verify that enum has no duplicates
                    if len(enum_values) != len(set(enum_values)):
                        duplicates = [
                            value
                            for value in set(enum_values)
                            if enum_values.count(value) > 1
                        ]
                        log_errors["duplicate_enums"][prop_name] = duplicates

            # Check for missing examples
            example = prop_features.get("examples")
            if example is None:
                log_errors["missing_examples"][prop_name] = ["Missing example."]

            feature_type = prop_features["type"]
            match feature_type:
                # Check date format for properties with type=string and format=date
                case "string":
                    if prop_features.get("format") == "date":
                        if isinstance(example, datetime):
                            example = example.strftime("%Y-%m-%d")
                        if isinstance(example, str):
                            try:
                                datetime.strptime(example, "%Y-%m-%d")
                            except ValueError:
                                if prop_name not in log_errors["invalid_date_formats"]:
                                    log_errors["invalid_date_formats"][prop_name] = []
                                log_errors["invalid_date_formats"][prop_name].append(
                                    f"Invalid date format '{example}', expected 'YYYY-MM-DD'"
                                )

                case "integer" | "number":
                    function_to_convert = float if feature_type == "number" else int
                    try:
                        example = function_to_convert(example)
                    except ValueError:
                        log_errors["invalid_example_types"][prop_name] = [
                            f"Value {example} is not a valid {feature_type}"
                        ]

        # return log errors if any
        if any(log_errors.values()):
            stderr.print("[red]\t- Database Validation Failed")
            # Convert log_errors dictionary to DataFrame
            df_errors = pd.DataFrame(
                [
                    {
                        "Error Category": category,
                        "Field": field,
                        "Details": (
                            ", ".join(details) if isinstance(details, list) else details
                        ),
                    }
                    for category, errors in log_errors.items()
                    for field, details in errors.items()
                ]
            )

            # Save errors to file
            error_file_path = f"{self.output_dir}/schema_validation_errors.csv"
            df_errors.to_csv(error_file_path, index=False, encoding="utf-8")

            # Provide errors to user in rich table format:
            relecov_tools.utils.display_dataframe_to_user(
                name="Schema Validation Errors", dataframe=df_errors
            )
            stderr.print(f"\t- Log errors saved to:\n\t{error_file_path}")

            # Ask user whether to continue or stop execution
            if self.non_interactive or relecov_tools.utils.prompt_yn_question(
                "Errors found in database values. Do you want to continue? (Y/N)"
            ):
                pass
            else:
                return log_errors
        else:
            stderr.print("[green]\t- Database validation passed")

        # If no errors found
        return None

    def get_available_projects(self, json):
        """Get list of available software in configuration

        Args:
            json (str): Path to bioinfo configuration json file.

        Returns:
            available_software: List containing available software defined in json.
        """
        config = relecov_tools.utils.read_json_file(json)
        # available_software = list(config.keys())
        available_projects = list(config.get("projects", {}).keys())
        return available_projects

    def read_database_definition(self, sheet_id="main"):
        """Reads the database definition from an Excel sheet and converts it into JSON format.

        Args:
            sheet_id (str): The sheet name or ID in the Excel file to read from. Defaults to "main".

        Returns:
            json_data (dict): The JSON data representing the database definition.
        """
        caller_method = inspect.stack()[1][3]
        # Read excel file
        df = pd.read_excel(
            self.excel_file_path,
            sheet_name=sheet_id,
            na_values=["nan", "N/A", "NA", ""],
        )
        # Convert database to json format
        json_data = {}
        for row in df.itertuples(index=False):
            property_name = row[0]
            values = row[1:]
            json_data[property_name] = dict(zip(df.columns[1:], values))

        # Check json is not empty
        if len(json_data) == 0:
            self.log.error(f"{caller_method}{sheet_id}) No data found in xlsx database")
            stderr.print(
                f"{caller_method}{sheet_id}) [red]No data found in xlsx database"
            )
            sys.exit(1)

        # Perform validation of database content
        validation_out = self.validate_database_definition(json_data)
        if validation_out:
            sys.exit()
        else:
            return json_data

    def create_schema_draft_template(self):
        """
        Create a JSON Schema draft template based on the draft version.
        Available drafts: [2020-12]

        Returns:
            draft_template(dict): The JSON Schema draft template.
        """
        draft_template = (
            relecov_tools.assets.schema_utils.jsonschema_draft.create_draft(
                draft_version=self.draft_version, required_items=True
            )
        )
        return draft_template
    
    def standard_jsonschema_object(self, property_id, property_feature_key: str, value: any, clean_ontologies=False):
        """
        Process a property from the resulting JSON from

        Args:
            property_id (_type_): _description_
            value (_type_): _description_
            clean_ontologies (bool, optional): _description_. Defaults to False.

        Returns:
            _type_: _description_
        """
        
        # Function to handle NaN values
        def handle_nan(value):
            if pd.isna(value) or value in ["nan", "NaN", "None", "none"]:
                return ""
            return str(value)

        jsonschema_value = {}
        #value = handle_nan(value)
        match property_feature_key, value:
            case "options", str(value):
                options_list = [option.split(":") for option in value.split(",")]
                # Handling minLengh, minimum, maximum etc
                for key, value in options_list:
                    key = key.strip()
                    value = value.strip()
                    try:
                        value = float(value)
                        value = int(value) if value.is_integer() else value
                    except ValueError:
                        pass
                    jsonschema_value[key] = value
            case "examples", str(value):
                jsonschema_value = {property_feature_key: value.split("; ")}
            # KNOWN ISSUE: MORE THAN 1 EXAMPLE WILL BE TREATED AS STRING
            # Known not a solution: if parse string, some str examples are parsed as int/number
            case "examples", int(value) | float(value):
                value = float(value)
                value = [int(value) if value.is_integer() else value]
                jsonschema_value = {property_feature_key: value}
            case "enum", str(enums):
                jsonschema_value = {"$ref": f"#/$defs/enums/{property_id}"}
            case _, value if not pd.isna(value):
                # Non-serializable JSON values (e.g. datetimes)
                try:
                    json.dumps(value)
                except (TypeError, OverflowError):
                    value = str(value)
                jsonschema_value = {property_feature_key: value}
            case _, _:
                pass
        
        return jsonschema_value

    def handle_properties(self, json_data: dict) -> list[dict, list[str]]:
        schema_property = {}
        required_property = []
        definitions = {"$defs": {"enums": {}}}

        # List of properties to check in the features dictionary (it maps values between database features and json schema features):
        #       key[db_feature_key]: value[schema_feature_key]
        # TODO mapping_features should be part of a config file, and this way it could be specific to each project (Maybe part of configJson?)
        mapping_features = {
            "enum": "enum",
            "examples": "examples",
            "ontology_id": "ontology",
            "type": "type",
            "options": "options",
            "description": "description",
            "classification": "classification",
            "label_name": "label",
            "fill_mode": "fill_mode",
            "required (Y/N)": "required",
            "submitting_lab_form": "header",
        }
        exclude_fields = ["required (Y/N)", "submitting_lab_form"]
        # Flag property values that belong outside the property:
        # - is_required: if required, goes to root 'required' keyword
        # - has_enum: if there is an enum, store it for '$defs'
        for property_id, db_features_dic in json_data.items():
            is_required = True if db_features_dic.get("required (Y/N)", "") == "Y" else False
            has_enum = db_features_dic.get("enum", False)

            # Create empty placeholder
            schema_property[property_id] = {}
            # If property is complex, call build schema again; else, continue function
            is_complex = True if db_features_dic.get("complex_field (Y/N)", "") == "Y" else False
            # FIXME CHECK HOW COMPLEX JSON_DATA IS HANDLED IN MEPRAM SCHEMA, but recursion should suffice
            if is_complex:
                # TODO check with team if we want more basic info in subschema (e.g. title)
                schema_draft = {
                    "type": "object",
                    "properties": {},
                    "required": []
                    }
                subschema = self.read_database_definition(property_id)
                complex_json_feature = self.build_new_schema(subschema, schema_draft, root_schema=False)
                if complex_json_feature:
                    # I don't fully like this: enums this way wouldn't be fully unique (defs are identified by non-unique key)
                    if complex_json_feature.get("$defs"):
                        definitions.update(complex_json_feature["$defs"])
                        complex_json_feature.pop("$defs")
                    schema_property[property_id]["type"] = "array"
                    schema_property[property_id]["items"] = complex_json_feature
            else:
                for db_feature_key, db_feature_value in db_features_dic.items():
                    if db_feature_key in exclude_fields:
                        continue
                    # Extra check to avoid non-mapping properties. Aun queda por evitar que se metan propiedades (e.g. required (Y/N))
                    if db_feature_key in mapping_features:
                        std_json_feature = self.standard_jsonschema_object(property_id, db_feature_key, db_feature_value)
                        if std_json_feature:
                            schema_property[property_id].update(std_json_feature)
            
            # If flag was set, update values accordingly per property
            if is_required:
                required_property.append(property_id)
            
            # TODO: FIX THIS CHECK, FOR SOME REASON NAN does this
            if isinstance(has_enum, str):
                enum = [value.strip() for value in has_enum.split("; ")]
                definitions["$defs"]["enums"][property_id] = {}
                definitions["$defs"]["enums"][property_id]["enum"] = enum

        # Just to be completely sure, but it should be unique
        required_property = list(set(required_property))
            

        return schema_property, required_property, definitions

    def schema_build_all_of(self, json_data):

        all_of_base = []

        # Generate all the anyOf within
        all_any_of = []
        conditional_required = {key: value.get("conditional_required_group").strip()
                                for key, value in json_data.items()
                                if not pd.isna(value.get("conditional_required_group"))}
        groups = list(set(conditional_required.values()))
        conditional_required_by_group = {group: [key for key in conditional_required.keys() 
                                                if conditional_required[key] == group]
                                                for group in groups}
        for group, keys in conditional_required_by_group.items():
            any_of = [{"required": [key]} for key in keys]
            all_any_of.append({"anyOf": any_of})

        all_of_base.extend(all_any_of)

        # For future: generate if_then within (for required props when specific value)
        # FUTURE: all_of_base.extend(all_if_then)

        return all_of_base


    def build_new_schema(self, json_data, schema_draft, root_schema=True):
        """
        Build a new JSON Schema based on the provided JSON data and draft template, in three stages:
        - Pre-properties: all the operations needed prior to handling the properties (e.g. creation of root properties)
        - properties: handling both simple and complex properties on a separate function
        - Post-properties: All the operations needed after handling properties (e.g. defining which properties are required)

        Parameters:
        json_data (dict): Dictionary containing the properties and values of the database definition.
        schema_draft (dict): The JSON Schema draft template.
        root_schema(bool): True if is root of schema, False if not (e.g. complex property generation)

        Returns:
            schema_draft (dict): The newly created JSON Schema.
        """
        # Pre-properties
        new_schema = schema_draft
        if root_schema:
            # Fill schema header
            # FIXME: it gets 'relecov-tools' instead of RELECOV
            project_name = relecov_tools.utils.get_package_name()
            new_schema["$id"] = relecov_tools.utils.get_schema_url()
            new_schema["title"] = f"{project_name} Schema."
            new_schema["description"] = (
                f"Json Schema that specifies the structure, content, and validation rules for {project_name}"
            )
            new_schema["version"] = self.version

        # Fill schema properties
        # Properties
        try:
            properties, required, defs = self.handle_properties(json_data)
        except Exception as e:
            self.log.error(f"Error building properties: {str(e)}")
            stderr.print(f"[red]Error building properties: {str(e)}")
            raise e

        # Post-properties
        # Finally, send schema_property object to the new json schema draft.
        new_schema["properties"] = properties
        if required:
            new_schema["required"] = required
        if defs:
            new_schema.update(defs)

        # Build the allOf keyword
        all_of = self.schema_build_all_of(json_data)
        if all_of:
            new_schema["allOf"] = all_of
        # Future: Here it can be extended to build other keywords at the end following the example above


        return new_schema

    def verify_schema(self, schema):
        """
        Verify that the given schema adheres to the JSON Schema specification for the specified draft version.

        Args:
            schema (dict): The JSON Schema to be verified.

        Raises:
            ValueError: If the schema does not conform to the JSON Schema specification.
        """
        relecov_tools.assets.schema_utils.jsonschema_draft.check_schema_draft(
            schema, self.draft_version
        )

    def get_schema_diff(self, base_schema, new_schema):
        """
        Print the differences between the base schema and the newly generated schema.

        Args:
            base_schema (dict): The base JSON Schema to compare against.
            new_schema (dict): The newly generated JSON Schema to compare.

        Returns:
            bool: True if differences are found, False otherwise.
        """
        # Set diff input
        base_schema_lines = json.dumps(base_schema, indent=4).splitlines()
        new_schema_lines = json.dumps(new_schema, indent=4).splitlines()

        # Get diff lines
        diff_lines = list(
            difflib.unified_diff(
                base_schema_lines,
                new_schema_lines,
                fromfile="base_schema.json",
                tofile="new_schema.json",
            )
        )

        if not diff_lines:
            self.log.info(
                "No differences were found between already installed and new generated schema. Exiting. No changes made"
            )
            stderr.print(
                "[yellow]No differences were found between already installed and new generated schema. Exiting. No changes made"
            )
            return None
        else:
            self.log.info(
                "Differences found between the existing schema and the newly generated schema."
            )
            stderr.print(
                "[yellow]Differences found between the existing schema and the newly generated schema."
            )
            if self.show_diff:
                return self.print_save_schema_diff(diff_lines)
            else:
                return None

    def print_save_schema_diff(self, diff_lines=None):
        # Set user's choices
        choices = ["Print to standard output (stdout)", "Save to file", "Both"]
        diff_output_choice = (
            "Save to file"
            if self.non_interactive
            else relecov_tools.utils.prompt_selection(
                "How would you like to print the diff between schemes?:", choices
            )
        )
        if diff_output_choice in ["Print to standard output (stdout)", "Both"]:
            for line in diff_lines:
                print(line)
            return True
        if diff_output_choice in ["Save to file", "Both"]:
            diff_filepath = os.path.join(
                os.path.realpath(self.output_dir) + "/build_schema_diff.txt"
            )
            with open(diff_filepath, "w") as diff_file:
                diff_file.write("\n".join(diff_lines))
            self.log.info(f"[green]Schema diff file saved to {diff_filepath}")
            stderr.print(f"[green]Schema diff file saved to {diff_filepath}")
            return True

    # FIXME: Add version tag to file name
    def save_new_schema(self, json_data):
        """
        Save the generated JSON Schema to the output folder.

        Args:
            json_data (dict): The JSON Schema to be saved.

        Returns:
            bool: True if the schema was successfully saved, False otherwise.
        """
        try:
            path_to_save = f"{self.output_dir}/relecov_schema.json"
            with open(path_to_save, "w") as schema_file:
                json.dump(json_data, schema_file, ensure_ascii=False, indent=4)
            self.log.info(f"New JSON schema saved to: {path_to_save}")
            stderr.print(f"[green]New JSON schema saved to: {path_to_save} ")
            return True
        except PermissionError as perm_error:
            self.log.error(f"Permission error: {perm_error}")
            stderr.print(f"[red]Permission error: {perm_error}")
        except IOError as io_error:
            self.log.error(f"I/O error: {io_error}")
            stderr.print(f"[red]I/O error: {io_error}")
        except Exception as e:
            self.log.error(f"An unexpected error occurred: {str(e)}")
            stderr.print(f"[red]An unexpected error occurred: {str(e)}")
        return False

    # FIXME: overview-tab - FIX first column values
    # FIXME: overview-tab - Still need to add the column that maps to tab metadatalab
    def create_metadatalab_excel(self, json_schema):
        """
        Generates an Excel template file for Metadata LAB with four sheets:
        Overview, Metadata LAB, Data Validation, and Version History.

        Args:
            json_schema (dict): The JSON schema used to generate the template.
                                It should include properties and required fields.

        Returns:
            None: If an error occurs during the process.
        """
        try:
            notes_control_input = (
                "Auto-generated update"
                if self.non_interactive
                else input(
                    "\033[93mEnter a note about changes made to the schema: \033[0m"
                )
            )

            # ------------------------------------------------------------------ #
            # 1.  Versioning & paths
            # ------------------------------------------------------------------ #
            version_history = pd.DataFrame(
                columns=["FILE_VERSION", "CODE", "NOTES CONTROL", "DATE"]
            )

            try:
                wb = openpyxl.load_workbook(self.excel_template)
                ws_version = wb["VERSION"]
                data = ws_version.values
                columns = next(data)
                version_history = pd.DataFrame(data, columns=columns)
            except Exception as e:
                self.log.warning(
                    f"Error reading previous VERSION sheet: {e}. Setting 1.0.0 as default."
                )
                version_history = pd.DataFrame(
                    [
                        {
                            "FILE_VERSION": "Relecov_metadata_template_v1.0.0",
                            "CODE": "1.0.0",
                            "NOTES CONTROL": "Initial version",
                            "DATE": datetime.now().strftime("%Y-%m-%d"),
                        }
                    ]
                )

            next_version = self.version
            version_info = {
                "FILE_VERSION": f"Relecov_metadata_template_v{next_version}",
                "CODE": next_version,
                "NOTES CONTROL": notes_control_input,
                "DATE": datetime.now().strftime("%Y-%m-%d"),
            }
            version_history = pd.concat(
                [version_history, pd.DataFrame([version_info])], ignore_index=True
            )
            out_file = os.path.join(
                self.output_dir, f"Relecov_metadata_template_v{next_version}.xlsx"
            )

            # ------------------------------------------------------------------ #
            # 2.  Schema filtering and dataframe preparation
            # ------------------------------------------------------------------ #
            default_classification_filter = [
                "Database Identifiers",
                "Sample collection and processing",
                "Host information",
                "Sequencing",
                "Pathogen diagnostic testing",
                "Contributor Acknowledgement",
                "Public databases",
                "Bioinformatics and QC metrics fields",
            ]
            required_properties = set(json_schema.get("required", []))
            schema_properties = json_schema.get("properties")
            enum_defs = json_schema.get("$defs", {}).get("enums", {})

            try:
                schema_properties_flatten = relecov_tools.assets.schema_utils.metadatalab_template.schema_to_flatten_json(
                    schema_properties, required_properties
                )
                df = relecov_tools.assets.schema_utils.metadatalab_template.schema_properties_to_df(
                    schema_properties_flatten
                )

                classification_overrides = self.configurables.get(
                    "classification_filters", {}
                )
                classification_filter = classification_overrides.get(
                    self.project, default_classification_filter
                )
                if classification_filter and "classification" in df.columns:
                    df = df[df["classification"].isin(classification_filter)]
                if "is_required" in df.columns:
                    df["required"] = df["is_required"].apply(
                        lambda value: "Y" if bool(value) else "N"
                    )
                else:
                    df["required"] = df["property_id"].apply(
                        lambda x: "Y" if x in required_properties else "N"
                    )

                def clean_ontologies(enums):
                    return [re.sub(r"\s*\[.*?\]", "", item).strip() for item in enums]

                def resolve_enum_ref(ref: str, enum_defs: dict) -> list[str]:
                    property_id = ref.split("/")[-1]
                    try:
                        values = enum_defs[property_id]["enum"]
                    except KeyError:
                        self.log.error(
                            f"Error finding enum for property '{property_id}'; not found in $defs"
                        )
                        stderr.print(
                            f"[red]Error finding enum for property '{property_id}'; not found in $defs"
                        )
                        return []
                    return (
                        clean_ontologies(values) if isinstance(values, list) else values
                    )

                df["enum"] = df["$ref"].apply(
                    lambda row: (
                        resolve_enum_ref(row, enum_defs=enum_defs)
                        if not pd.isna(row)
                        else row
                    )
                )
                common_dropdown = self._lab_dropdowns["collecting_institution"]

                lab_fields = [
                    "collecting_institution",
                    "submitting_institution",
                    "sequencing_institution",
                ]

                mask = df["property_id"].isin(lab_fields)

                df.loc[mask, "enum"] = pd.Series(
                    [common_dropdown] * mask.sum(), index=df.loc[mask].index
                )

            except Exception as e:
                self.log.error(f"Error processing schema properties: {e}")
                stderr.print(f"Error processing schema properties: {e}")
                return None

            # ------------------------------------------------------------------ #
            # 3.  Headers / filtering
            # ------------------------------------------------------------------ #
            if "header" in df.columns:
                df["header"] = df["header"].astype(str).str.strip()
                df_filtered = df[df["header"].str.upper() == "Y"]
            else:
                self.log.warning(
                    "No se encontró la columna 'header', usando df sin filtrar."
                )
                df_filtered = df

            # ------------------------------------------------------------------ #
            # 4.  Construction of OVERVIEW, METADATA_LAB and DATA_VALIDATION
            # ------------------------------------------------------------------ #
            # -- OVERVIEW
            try:
                overview_header = [
                    "Label name",
                    "Description",
                    "Group",
                    "Mandatory (Y/N)",
                    "Example",
                ]
                df_overview = pd.DataFrame(columns=overview_header)
                df_overview["Label name"] = df_filtered["label"]
                df_overview["Description"] = df_filtered["description"]
                df_overview["Group"] = df_filtered["classification"]
                df_overview["Mandatory (Y/N)"] = df_filtered["required"]
                df_overview["Example"] = df_filtered["examples"].apply(
                    lambda x: x[0] if isinstance(x, list) else x
                )
            except Exception as e:
                self.log.error(f"Error creating overview sheet: {e}")
                stderr.print(f"Error creating overview sheet: {e}")
                return None

            # -- METADATA_LAB
            try:
                metadatalab_header = ["REQUERIDO", "EJEMPLOS", "DESCRIPCIÓN", "CAMPO"]
                df_metadata = pd.DataFrame(columns=metadatalab_header)
                df_metadata["REQUERIDO"] = df_filtered["required"].apply(
                    lambda x: "YES" if str(x).upper() in ["Y", "YES"] else ""
                )
                df_metadata["EJEMPLOS"] = df_filtered["examples"].apply(
                    lambda x: x[0] if isinstance(x, list) else x
                )
                df_metadata["DESCRIPCIÓN"] = df_filtered["description"]
                df_metadata["CAMPO"] = df_filtered["label"]
                df_metadata = df_metadata.transpose()
            except Exception as e:
                self.log.error(f"Error creating MetadataLab sheet: {e}")
                stderr.print(f"[red]Error creating MetadataLab sheet: {e}")
                return None

            # -- DATA_VALIDATION
            try:
                datavalidation_header = ["EJEMPLOS", "DESCRIPCIÓN", "CAMPO"]
                df_hasenum = df[pd.notnull(df.enum)]
                df_validation = pd.DataFrame(columns=datavalidation_header)
                df_validation["tmp_property"] = df_hasenum["property_id"]
                df_validation["EJEMPLOS"] = df_hasenum["examples"].apply(
                    lambda x: x[0] if isinstance(x, list) else x
                )
                df_validation["DESCRIPCIÓN"] = df_hasenum["description"]
                df_validation["CAMPO"] = df_hasenum["label"]
            except Exception as e:
                self.log.error(f"Error creating DataValidation sheet: {e}")
                stderr.print(f"[red]Error creating DataValidation sheet: {e}")
                return None

            # ------------------------------------------------------------------ #
            # 5.  Prepare DATA_VALIDATION
            # ------------------------------------------------------------------ #
            try:
                enum_dict = {prop: [] for prop in df_hasenum["property_id"]}
                enum_maxitems = 0
                for key in enum_dict.keys():
                    enum_values = df_hasenum[df_hasenum["property_id"] == key][
                        "enum"
                    ].values
                    if enum_values.size > 0:
                        enum_list = enum_values[0]
                        enum_dict[key] = enum_list
                        enum_maxitems = max(enum_maxitems, len(enum_list))
                    else:
                        enum_dict[key] = []

                for key in enum_dict.keys():
                    enum_dict[key].extend([""] * (enum_maxitems - len(enum_dict[key])))

                new_df = pd.DataFrame(enum_dict)
                valid_index = df_validation["tmp_property"].values
                valid_transposed = df_validation.transpose()
                valid_transposed.columns = valid_index
                df_validation = pd.concat([valid_transposed, new_df]).drop(
                    index=["tmp_property"]
                )
            except Exception as e:
                self.log.error(f"Error processing enums and combining data: {e}")
                stderr.print(f"[red]Error processing enums and combining data: {e}")
                return None

            # ------------------------------------------------------------------ #
            # 6.  Cleaning of NaN's
            # ------------------------------------------------------------------ #
            df_overview = (
                df_overview.replace([float("inf"), float("-inf")], "")
                .fillna("")
                .infer_objects()
            )
            df_metadata = (
                df_metadata.replace([float("inf"), float("-inf")], "")
                .fillna("")
                .infer_objects()
            )
            df_validation = (
                df_validation.replace([float("inf"), float("-inf")], "")
                .fillna("")
                .infer_objects()
            )

            # ------------------------------------------------------------------ #
            # 7.  Initial writing of the XLSX
            # ------------------------------------------------------------------ #
            try:
                writer = pd.ExcelWriter(out_file, engine="xlsxwriter")
                mt = relecov_tools.assets.schema_utils.metadatalab_template

                mt.excel_formater(
                    df_overview,
                    writer,
                    "OVERVIEW",
                    out_file,
                    have_index=False,
                    have_header=False,
                )
                mt.excel_formater(
                    df_metadata,
                    writer,
                    "METADATA_LAB",
                    out_file,
                    have_index=True,
                    have_header=False,
                )
                mt.excel_formater(
                    df_validation,
                    writer,
                    "DATA_VALIDATION",
                    out_file,
                    have_index=True,
                    have_header=False,
                )
                version_history.to_excel(writer, sheet_name="VERSION", index=False)
                writer.close()

                self.log.info(
                    f"Metadata lab template successfuly created in: {out_file}"
                )
                stderr.print(
                    f"[green]Metadata lab template successfuly created in: {out_file}"
                )
            except Exception as e:
                self.log.error(f"Error writing to Excel: {e}")
                stderr.print(f"[red]Error writing to Excel: {e}")
                return None

            # ------------------------------------------------------------------ #
            # 8.  OpenPyXL post-processing (conditions, dropdowns, etc.)
            # ------------------------------------------------------------------ #
            try:
                wb = openpyxl.load_workbook(out_file)
                ws_metadata = wb["METADATA_LAB"]
                ws_metadata.freeze_panes = self.configurables.get("freeze_panel", "D1")
                ws_metadata.delete_rows(5)
                mt.create_condition(ws_metadata, self.project_config, df_filtered)
                mt.add_conditional_format_age_check(ws_metadata, df_filtered)

                # Hidden sheet for dropdowns
                ws_dropdowns = (
                    wb.create_sheet("DROPDOWNS")
                    if "DROPDOWNS" not in wb.sheetnames
                    else wb["DROPDOWNS"]
                )
                for row in ws_dropdowns.iter_rows():
                    for cell in row:
                        cell.value = None

                # Dynamic lists for the three special fieldss
                common_dropdown = self._lab_dropdowns["collecting_institution"]
                special_dropdowns = {
                    "collecting_institution": common_dropdown,
                    "submitting_institution": common_dropdown,
                    "sequencing_institution": common_dropdown,
                }
                # ------------------------------------------------------------------------------

                # We scroll through the columns of METADATA_LAB (original order of df)
                for col_idx, property_id in enumerate(df["property_id"], start=1):
                    # Select list of values
                    if property_id in special_dropdowns:
                        enum_values = special_dropdowns[property_id]
                    else:
                        enum_values = df.loc[
                            df["property_id"] == property_id, "enum"
                        ].values[0]

                    if not isinstance(enum_values, list) or len(enum_values) == 0:
                        continue

                    # Write on sheet DROPDOWNS
                    col_letter = openpyxl.utils.get_column_letter(col_idx)
                    for i, val in enumerate(enum_values, start=1):
                        ws_dropdowns[f"{col_letter}{i}"].value = val

                    # Create validation in METADATA_LAB
                    dv_range = (
                        f"DROPDOWNS!${col_letter}$1:${col_letter}${len(enum_values)}"
                    )
                    meta_col = ws_metadata.cell(row=4, column=col_idx + 1).column_letter
                    meta_rng = f"{meta_col}5:{meta_col}1000"

                    dv = DataValidation(
                        type="list",
                        formula1=dv_range,
                        allow_blank=False,
                        showErrorMessage=True,
                    )
                    dv.error = "Valor no permitido. Elija un elemento de la lista."
                    dv.errorTitle = "Error de validación"
                    dv.prompt = f"Seleccione un valor para {property_id}"
                    dv.promptTitle = "Selección de valor"

                    ws_metadata.add_data_validation(dv)
                    dv.add(meta_rng)

                # ------------------------------------------------------------------ #
                # 9.  Visual adjustments (widths, text-wrap, hide sheet)
                # ------------------------------------------------------------------ #
                if "OVERVIEW" in wb.sheetnames:
                    ws_overview = wb["OVERVIEW"]
                    column_width = 35
                    for col in ws_overview.columns:
                        max_length = max(
                            len(str(cell.value)) for cell in col if cell.value
                        )
                        ws_overview.column_dimensions[col[0].column_letter].width = min(
                            max_length + 2, column_width
                        )
                    for row in ws_overview.iter_rows():
                        for cell in row:
                            cell.alignment = openpyxl.styles.Alignment(wrap_text=True)

                    ws_version = wb["VERSION"]
                    for i, col in enumerate(ws_version.columns, start=1):
                        max_len = max(len(str(c.value)) for c in col if c.value)
                        ws_version.column_dimensions[
                            openpyxl.utils.get_column_letter(i)
                        ].width = (max_len + 2)

                ws_dropdowns.sheet_state = "hidden"
                wb.save(out_file)
            except Exception as e:
                self.log.error(f"Error adding dropdowns: {e}")
                stderr.print(f"[red]Error adding dropdowns: {e}")
                return None

        except Exception as e:
            self.log.error(f"Error in create_metadatalab_excel: {e}")
            stderr.print(f"[red]Error in create_metadatalab_excel: {e}")
            return None

    def summarize_schema(self, json_schema):
        """
        Generate summary statistics for a JSON Schema and display it in tabular format.

        Args:
            json_schema (dict): The JSON Schema to analyze.

        Returns:
            None: Displays the table directly to stdout.
        """
        properties = json_schema.get("properties", {})
        # Initialize counters
        total_properties = len(properties)
        type_counts = {}
        enum_count = 0
        free_text_count = 0

        # Iterate over properties
        for _, prop_details in properties.items():
            prop_type = prop_details.get("type", "unknown")

            # Count types
            type_counts[prop_type] = type_counts.get(prop_type, 0) + 1

            # Count enum vs free text
            if "enum" in prop_details:
                enum_count += 1
            else:
                free_text_count += 1

        # Prepare summary data
        summary_data = {
            "Total Properties": [total_properties],
            "String Properties": [type_counts.get("string", 0)],
            "Integer Properties": [type_counts.get("integer", 0)],
            "Number Properties": [type_counts.get("number", 0)],
            "Boolean Properties": [type_counts.get("boolean", 0)],
            "Object Properties": [type_counts.get("object", 0)],
            "Array Properties": [type_counts.get("array", 0)],
            "Enum Properties": [enum_count],
            "Free Text Properties": [free_text_count],
        }
        summary_df = pd.DataFrame(summary_data)

        # Display summary using rich table (if available) or print raw
        try:
            relecov_tools.utils.display_dataframe_to_user(
                name="JSON Schema Summary", dataframe=summary_df
            )
        except AttributeError:
            print(summary_df.to_string(index=False))

        def shorten_path(path, max_length=50):
            """Shortens long paths by keeping first and last segments while adding ellipsis in the middle."""
            if len(path) <= max_length:
                return path  # No need to shorten

            parts = path.split(os.sep)  # Split into parts
            if len(parts) > 3:
                return os.sep.join(
                    [parts[0], "..."] + parts[-2:]
                )  # Keep first, last two, and replace middle
            return path  # If it's already short, return as is

        # Folder containing results
        outdir_data = {
            "Description": [
                "Output Folder",
                "New JSON Schema",
                "Old JSON Schema",
                "Schema Diff File",
                "Metadata Template File",
            ],
            "Path": [
                shorten_path(self.output_dir),
                shorten_path(f"{self.output_dir}/relecov_schema.json"),
                shorten_path(self.base_schema_path),
                shorten_path(f"{self.output_dir}/build_schema_diff.txt"),
                shorten_path(f"{self.output_dir}/Relecov_metadata_template_v*.xlsx"),
            ],
        }

        # Convert to DataFrame
        outdir_df = pd.DataFrame(outdir_data)

        # Display summary using rich table or print raw if unavailable
        try:
            relecov_tools.utils.display_dataframe_to_user(
                name="JSON Results Overview", dataframe=outdir_df
            )
        except AttributeError:
            print(outdir_df.to_string(index=False))

    def handle_build_schema(self):
        # Load xlsx database and convert into json format
        self.log.info("Start reading xlsx database")
        stderr.print("[white]Start reading xlsx database")
        database_dic = self.read_database_definition()

        # Verify current schema used by relecov-tools:
        base_schema_json = relecov_tools.utils.read_json_file(self.base_schema_path)
        if not base_schema_json:
            self.log.error("Couldn't find relecov base schema.)")
            stderr.print("[red]Couldn't find relecov base schema. Exiting...)")
            sys.exit(1)

        # Create schema draft template (leave empty to be prompted to list of available schema versions)
        schema_draft_template = self.create_schema_draft_template()

        # build new schema draft based on database definition.
        new_schema_json = self.build_new_schema(database_dic, schema_draft_template)

        # Verify new schema follows json schema specification rules.
        self.verify_schema(new_schema_json)

        # Compare base vs new schema and print/saves differences (--diff = True)
        self.get_schema_diff(base_schema_json, new_schema_json)

        # Saves new JSON schema
        self.save_new_schema(new_schema_json)

        # Create metadata lab template
        if self.non_interactive or relecov_tools.utils.prompt_yn_question(
            "Do you want to create a metadata lab file?:"
        ):
            self.create_metadatalab_excel(new_schema_json)

        # Return new schema
        return new_schema_json
