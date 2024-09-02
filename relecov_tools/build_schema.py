#!/usr/bin/env python
import logging
import relecov_tools.json_validation
import rich.console
import pandas as pd
import os
import sys
import json
import difflib
import inspect

import relecov_tools.utils
import relecov_tools.assets.schema_utils.jsonschema_draft
import relecov_tools.assets.schema_utils.metadatalab_template
from relecov_tools.config_json import ConfigJson

log = logging.getLogger(__name__)
stderr = rich.console.Console(
    stderr=True,
    style="dim",
    highlight=False,
    force_terminal=relecov_tools.utils.rich_force_colors(),
)


class SchemaBuilder:
    def __init__(
        self,
        excel_file_path=None,
        base_schema_path=None,
        draft_version=None,
        show_diff=None,
        out_dir=None,
    ):
        """
        Initialize the SchemaBuilder class. This class generates a JSON Schema file based on the provided draft version.
        It reads the database definition from an Excel file and allows customization of the schema generation process.
        """
        self.excel_file_path = excel_file_path
        # Validate input variables
        if not self.excel_file_path or not os.path.isfile(self.excel_file_path):
            raise ValueError("A valid Excel file path must be provided.")
        if not self.excel_file_path.endswith(".xlsx"):
            raise ValueError("The Excel file must have a .xlsx extension.")

        # Validate output folder creation
        if not out_dir or not os.path.isfile(out_dir):
            self.output_folder = relecov_tools.utils.prompt_create_outdir(None, out_dir)
        else:
            self.output_folder = out_dir

        # Validate show diff option
        if not show_diff:
            self.show_diff = None
        else:
            self.show_diff = True

        # Validate json schema draft version
        self.draft_version = (
            relecov_tools.assets.schema_utils.jsonschema_draft.check_valid_version(
                draft_version
            )
        )

        # Validate base schema
        if base_schema_path is not None:
            if relecov_tools.utils.file_exists(base_schema_path):
                self.base_schema_path = base_schema_path
            else:
                stderr.print(
                    f"[Error]Defined base schema file not found: {base_schema_path}. Exiting..."
                )
                sys.exit(1)
        else:
            try:
                config_json = ConfigJson()
                relecov_schema = config_json.get_topic_data(
                    "json_schemas", "relecov_schema"
                )
                try:
                    self.base_schema_path = os.path.join(
                        os.path.dirname(os.path.realpath(__file__)),
                        "schema",
                        relecov_schema,
                    )
                    if not relecov_tools.utils.file_exists(self.base_schema_path):
                        stderr.print(
                            "[Error]Fatal error. Relecov schema were not found in current relecov-tools installation. Make sure relecov-tools command is functioning. Exiting..."
                        )
                        sys.exit(1)
                    stderr.print(
                        "[green]RELECOV schema successfully found in the configuration."
                    )
                except FileNotFoundError as fnf_error:
                    stderr.print(f"[red]Configuration file not found: {fnf_error}")
                    sys.exit(1)
            except KeyError as key_error:
                stderr.print(f"[orange]Configuration key error: {key_error}")
                sys.exit(1)

    def validate_database_definition(self, json_data):
        """Validate the mandatory features of each property in json_data.
        Validate the mandatory features of each property in json_data.

        Args:
        json_data (dict): The JSON data representing the database definition.

        Returns:
            dict or None: A dictionary with properties that are missing mandatory/invalid features,
            or None if all mandatory features are present
        """
        # Check mandatory key features to build a json schema
        notvalid_properties = {}
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
        # Iterate over each property in json_data
        for j_key, j_value in json_data.items():
            missing_features = []
            for feature in mandatory_features:
                if feature not in j_value:
                    missing_features.append(feature)

            if missing_features:
                notvalid_properties[j_key] = missing_features

        # Summarize validation
        if notvalid_properties:
            return notvalid_properties
        else:
            return None

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
            stderr.print(
                f"{caller_method}{sheet_id}) [red]No data found in xlsx database"
            )
            sys.exit(1)

        # Perform validation of database content
        validation_out = self.validate_database_definition(json_data)

        if validation_out:
            stderr.print(
                f"({caller_method}:{sheet_id}) [red]Validation of database content falied. Missing mandatory features in: {validation_out}"
            )
            sys.exit(1)
        else:
            stderr.print(
                f"({caller_method}:{sheet_id}) [green]Validation of database content passed."
            )
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
                self.draft_version, True
            )
        )
        return draft_template

    def standard_jsonschema_object(seschemalf, data_dict, target_key):
        """
        Create a standard JSON Schema object for a given key in the data dictionary.

        Args:
            data_dict (dict): The data dictionary containing the properties.
            target_key (str): The key for which to create the JSON Schema object.

        Returns:
            json_dict (dict): The JSON Schema object for the target key.
        """
        # For enum and examples, wrap the value in a list
        json_dict = {}

        # Function to handle NaN values
        def handle_nan(value):
            if pd.isna(value) or value in ["nan", "NaN", "None", "none"]:
                return ""
            return str(value)

        if target_key in ["enum", "examples"]:
            value = handle_nan(data_dict.get(target_key, ""))
            # if no value, json key won't be necessary, then avoid adding it
            if len(value) > 0:
                if target_key == "enum":
                    json_dict[target_key] = value.split(", ")
                elif target_key == "examples":
                    json_dict[target_key] = [value]
        elif target_key == "description":
            json_dict[target_key] = handle_nan(data_dict.get(target_key, ""))
        else:
            json_dict[target_key] = handle_nan(data_dict.get(target_key, ""))
        return json_dict

    def complex_jsonschema_object(self, property_id, features_dict):
        """
        Create a complex (nested) JSON Schema object for a given property ID.

        Args:
            property_id (str): The ID of the property for which to create the JSON Schema object.
            features_dict (dict): A dictionary mapping database features to JSON Schema features.

        Returns:
            json_dict (dict): The complex JSON Schema object.
        """
        json_dict = {"type": "object", "properties": {}}

        # Read tab-dedicated sheet in excell database
        try:
            complex_json_data = self.read_database_definition(sheet_id=property_id)
        except ValueError as e:
            stderr.print(f"[yellow]{e}")
            return None

        # Add sub property items
        for sub_property_id, _ in complex_json_data.items():
            json_dict["properties"][sub_property_id] = {}
            complex_json_feature = {}
            for db_feature_key, json_key in features_dict.items():
                if json_key == "required":
                    continue
                feature_schema = self.standard_jsonschema_object(
                    complex_json_data[sub_property_id], db_feature_key
                )
                if feature_schema:
                    complex_json_feature[json_key] = feature_schema[db_feature_key]
            json_dict["properties"][sub_property_id] = complex_json_feature

        return json_dict

    def build_new_schema(self, json_data, schema_draft):
        """
        Build a new JSON Schema based on the provided JSON data and draft template.

        Parameters:
        json_data (dict): The JSON data representing the database definition.
        schema_template (dict): The JSON Schema draft template.

        Returns:
            schema_draft (dict): The newly created JSON Schema.
        """
        try:
            # List of properties to check in the features dictionary (it maps values between database features and json schema features):
            #       key[db_feature_key]: value[schema_feature_key]
            features_to_check = {
                "type": "type",
                "enum": "enum",
                "examples": "examples",
                "ontology_id": "ontology",
                "description": "description",
                "classification": "classification",
                "label_name": "label",
                "fill_mode": "fill_mode",
                "required (Y/N)": "required",
            }
            required_property_unique = []

            # Read property_ids in the database.
            #   Perform checks and create (for each property) feature object like:
            #       {'example':'A', 'ontology': 'B'...}.
            #   Finally this objet will be written to the draft schema.
            for property_id, db_features_dic in json_data.items():
                schema_property = {}
                required_property = {}

                # Parse property_ids that needs to be incorporated as complex fields in json_schema
                if json_data[property_id].get("complex_field (Y/N)") == "Y":
                    complex_json_feature = self.complex_jsonschema_object(
                        property_id, features_to_check
                    )
                    if complex_json_feature:
                        schema_property["type"] = "array"
                        schema_property["items"] = complex_json_feature
                        schema_property["additionalProperties"] = False
                        schema_property["required"] = [
                            key for key in complex_json_feature["properties"].keys()
                        ]
                # For those that follows standard format, add them to json schema as well.
                else:
                    for db_feature_key, schema_feature_key in features_to_check.items():
                        # Verifiy that db_feature_key is present in the database (processed excel (aka 'json_data'))
                        if db_feature_key not in db_features_dic:
                            stderr.print(
                                f"[INFO] Feature {db_feature_key} is not present in database ({self.excel_file_path})"
                            )
                            continue
                        # Record the required value for each property
                        if (
                            "required" in db_feature_key
                            or "required" == schema_feature_key
                        ):
                            is_required = str(db_features_dic[db_feature_key])
                            if is_required != "nan":
                                required_property[property_id] = is_required
                        else:
                            std_json_feature = self.standard_jsonschema_object(
                                db_features_dic, db_feature_key
                            )
                            if std_json_feature:
                                schema_property[schema_feature_key] = std_json_feature[
                                    db_feature_key
                                ]
                            else:
                                continue
                # Finally, send schema_property object to the new json schema draft.
                schema_draft["properties"][property_id] = schema_property

                # Add to schema draft the recorded porperty_ids.
                for key, values in required_property.items():
                    if values == "Y":
                        required_property_unique.append(key)
            schema_draft["required"] = required_property_unique

            # Return new schema
            return schema_draft

        except Exception as e:
            stderr.print(f"[red]Error building schema: {str(e)}")
            raise

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
            stderr.print(
                "[yellow]No differencess were found between already installed and new generated schema. Exiting. No changes made"
            )
            return None
        else:
            stderr.print(
                "[yellow]Differences found between the existing schema and the newly generated schema."
            )
            return self.print_save_schema_diff(diff_lines)

    def print_save_schema_diff(self, diff_lines=None):
        # Set user's choices
        choices = ["Print to sandard output (stdout)", "Save to file", "Both"]
        diff_output_choice = relecov_tools.utils.prompt_selection(
            "How would you like to print the diff between schemes?:", choices
        )
        if diff_output_choice in ["Print to sandard output (stdout)", "Both"]:
            for line in diff_lines:
                print(line)
            return True
        if diff_output_choice in ["Save to file", "Both"]:
            diff_filepath = os.path.join(
                os.path.realpath(self.output_folder) + "/build_schema_diff.txt"
            )
            with open(diff_filepath, "w") as diff_file:
                diff_file.write("\n".join(diff_lines))
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
            path_to_save = self.output_folder + "/relecov_schema.json"
            with open(path_to_save, "w") as schema_file:
                json.dump(json_data, schema_file, ensure_ascii=False, indent=4)
            stderr.print(f"[green]New JSON schema saved to: {path_to_save} ")
            return True
        except PermissionError as perm_error:
            stderr.print(f"[red]Permission error: {perm_error}")
        except IOError as io_error:
            stderr.print(f"[red]I/O error: {io_error}")
        except Exception as e:
            stderr.print(f"[red]An unexpected error occurred: {str(e)}")
        return False

    # FIXME: overview-tab - FIX first column values
    # FIXME: overview-tab - Still need to add the column that maps to tab metadatalab
    def create_metadatalab_excel(self, json_schema):
        """
        Generate an Excel template file for Metadata LAB with three tabs: Overview, Metadata LAB, and Data Validation.

        Args:
            json_schema (dict): The JSON Schema from which the Excel template is generated. It should include properties and required fields.

        Returns:
            None: if any error occurs during the process.
        """
        try:
            # Set up metadatalab configuration
            out_file = os.path.join(
                self.output_folder, "metadatalab_template" + ".xlsx"
            )
            required_classification = [
                "Database Identifiers",
                "Sample collection and processing",
                "Host information",
                "Sequencing",
                "Pathogen Diagnostic testing",
                "Contributor Acknowledgement",
            ]
            required_properties = json_schema.get("required")
            schema_properties = json_schema.get("properties")

            # Read json schema properties and convert it into pandas df
            try:
                schema_properties_flatten = relecov_tools.assets.schema_utils.metadatalab_template.schema_to_flatten_json(
                    schema_properties
                )
                df = relecov_tools.assets.schema_utils.metadatalab_template.schema_properties_to_df(
                    schema_properties_flatten
                )
                df = df[df["classification"].isin(required_classification)]
                df["required"] = df["property_id"].apply(
                    lambda x: "Y" if x in required_properties else "N"
                )
            except Exception as e:
                stderr.print(f"Error processing schema properties: {e}")
                return None

            # Overview sheet
            try:
                overview_header = [
                    "Label name",
                    "Description",
                    "Group",
                    "Mandatory (Y/N)",
                    "Example",
                    "METADATA_LAB COLUMN",
                ]
                df_overview = pd.DataFrame(
                    columns=[col_name for col_name in overview_header]
                )
                df_overview["Label name"] = df["label"]
                df_overview["Description"] = df["description"]
                df_overview["Group"] = df["classification"]
                df_overview["Mandatory (Y/N)"] = df["required"]
                df_overview["Example"] = df["examples"].apply(
                    lambda x: x[0] if isinstance(x, list) else x
                )
            except Exception as e:
                stderr.print(f"Error creating overview sheet: {e}")
                return None

            # MetadataLab sheet
            try:
                metadatalab_header = ["EJEMPLOS", "DESCRIPCIÓN", "CAMPO"]
                df_metadata = pd.DataFrame(
                    columns=[col_name for col_name in metadatalab_header]
                )
                df_metadata["EJEMPLOS"] = df["examples"].apply(
                    lambda x: x[0] if isinstance(x, list) else x
                )
                df_metadata["DESCRIPCIÓN"] = df["description"]
                df_metadata["CAMPO"] = df["label"]
                df_metadata = df_metadata.transpose()
            except Exception as e:
                stderr.print(f"[red]Error creating MetadataLab sheet: {e}")
                return None

            # DataValidation sheet
            try:
                datavalidation_header = ["EJEMPLOS", "DESCRIPCIÓN", "CAMPO"]
                df_hasenum = df[(pd.notnull(df.enum))]
                df_validation = pd.DataFrame(
                    columns=[col_name for col_name in datavalidation_header]
                )
                df_validation["tmp_property"] = df_hasenum["property_id"]
                df_validation["EJEMPLOS"] = df_hasenum["examples"].apply(
                    lambda x: x[0] if isinstance(x, list) else x
                )
                df_validation["DESCRIPCIÓN"] = df_hasenum["description"]
                df_validation["CAMPO"] = df_hasenum["label"]
            except Exception as e:
                stderr.print(f"[red]Error creating DataValidation sheet: {e}")
                return None

            try:
                # Since enums have different lengths we need further processing.
                # Convert df into dict to perform data manipulation.
                enum_dict = {property: [] for property in df_hasenum["property_id"]}
                enum_maxitems = 0
                # Populate the dictionary with flattened lists
                for key in enum_dict.keys():
                    enum_values = df_hasenum[df_hasenum["property_id"] == key][
                        "enum"
                    ].values
                    if enum_values.size > 0:
                        enum_list = enum_values[0]  # Extract the list
                        enum_dict[key] = enum_list  # Assign the list to the dictionary
                        if enum_maxitems < len(enum_list):
                            enum_maxitems = len(enum_list)
                    else:
                        enum_dict[key] = []

                # Reshape list dimensions based on enum length.
                for key in enum_dict.keys():
                    if len(enum_dict[key]) < enum_maxitems:
                        num_nas = enum_maxitems - len(enum_dict[key])
                        for _ in range(num_nas):
                            enum_dict[key].append("")

                new_df = pd.DataFrame(enum_dict)
                new_index = range(len(new_df.columns))
                new_df.reindex(columns=new_index)

                valid_index = df_validation["tmp_property"].values
                valid_transposed = df_validation.transpose()
                valid_transposed.columns = valid_index

                frames = [valid_transposed, new_df]
                df_validation = pd.concat(frames)
                df_validation = df_validation.drop(index=["tmp_property"])
            except Exception as e:
                stderr.print(f"[red]Error processing enums and combining data: {e}")
                return None

            # WRITE EXCEL
            try:
                writer = pd.ExcelWriter(out_file, engine="xlsxwriter")
                relecov_tools.assets.schema_utils.metadatalab_template.excel_formater(
                    df_overview,
                    writer,
                    "OVERVIEW",
                    out_file,
                    have_index=False,
                    have_header=False,
                )
                relecov_tools.assets.schema_utils.metadatalab_template.excel_formater(
                    df_metadata,
                    writer,
                    "METADATA_LAB",
                    out_file,
                    have_index=True,
                    have_header=False,
                )
                relecov_tools.assets.schema_utils.metadatalab_template.excel_formater(
                    df_validation,
                    writer,
                    "DATA_VALIDATION",
                    out_file,
                    have_index=True,
                    have_header=False,
                )
                writer.close()
                stderr.print(
                    f"[green]Metadata lab template successfuly created in: {out_file}"
                )
            except Exception as e:
                stderr.print(f"[red]Error writing to Excel: {e}")
                return None
        except Exception as e:
            stderr.print(f"[red]Error in create_metadatalab_excel: {e}")
            return None

    def handle_build_schema(self):
        # Load xlsx database and convert into json format
        stderr.print("[white]Start reading xlsx database")
        database_dic = self.read_database_definition()

        # Verify current schema used by relecov-tools:
        base_schema_json = relecov_tools.utils.read_json_file(self.base_schema_path)
        if not base_schema_json:
            stderr.print("[red]Couldn't find relecov base schema. Exiting...)")
            sys.exit(1)

        # Create schema draft template (leave empty to be prompted to list of available schema versions)
        schema_draft_template = self.create_schema_draft_template()

        # build new schema draft based on database definition.
        new_schema_json = self.build_new_schema(database_dic, schema_draft_template)

        # Verify new schema follows json schema specification rules.
        self.verify_schema(new_schema_json)

        # Compare base vs new schema and saves new JSON schema
        stderr.print(self.show_diff)
        if self.show_diff:
            schema_diff = self.get_schema_diff(base_schema_json, new_schema_json)
        else:
            schema_diff = None

        if schema_diff:
            self.save_new_schema(new_schema_json)
        else:
            stderr.print(
                f"[green]No changes found against base schema ({self.base_schema_path})."
            )

        # Create metadata lab template
        promp_answ = relecov_tools.utils.prompt_yn_question(
            "Do you want to create a metadata lab file?:"
        )
        if promp_answ:
            self.create_metadatalab_excel(new_schema_json)
