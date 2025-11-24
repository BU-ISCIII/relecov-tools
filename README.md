# relecov-tools

[![python_lint](https://github.com/BU-ISCIII/relecov-tools/actions/workflows/python_lint.yml/badge.svg)](https://github.com/BU-ISCIII/relecov-tools/actions/workflows/python_lint.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

relecov-tools is a set of helper tools for the assembly of the different elements in the RELECOV platform (Spanish Network for genomic surveillance of SARS-Cov-2) as data download, processing, validation and upload to public databases, as well as analysis runs and database storage.

## Table of contents

- [relecov-tools](#relecov-tools)
  - [Table of contents](#table-of-contents)
  - [Installation](#installation)
    - [Requirements prior install](#requirements-prior-install)
      - [Install 7-Zip on Linux](#install-7-zip-on-linux)
    - [Bioconda](#bioconda)
    - [Pip](#pip)
    - [Development version](#development-version)
  - [Usage](#usage)
    - [Command-line](#command-line)
  - [Modules](#modules)
      - [download](#download)
      - [read-lab-metadata](#read-lab-metadata)
      - [send-mail](#send-mail)
      - [read-bioinfo-metadata](#read-bioinfo-metadata)
        - [Configuration of module `read-bioinfo-metadata`](#configuration-of-module-read-bioinfo-metadata)
      - [validate](#validate)
      - [map](#map)
      - [upload-to-ena](#upload-to-ena)
      - [upload-to-gisaid](#upload-to-gisaid)
      - [update-db](#update-db)
      - [pipeline-manager](#pipeline-manager)
      - [wrapper](#wrapper)
      - [logs-to-excel](#logs-to-excel)
      - [add-extra-config](#add-extra-config)
    - [build-schema](#build-schema)
      - [Mandatory Fields](#mandatory-fields)
      - [Logging\_functionality](#logging_functionality)
      - [Custom logs](#custom-logs)
    - [Python package mode](#python-package-mode)
  - [Acknowledgements](#acknowledgements)

## Installation

### Requirements prior install

These tools require

- Python > 3.8
- [7-Zip](https://www.7-zip.org/) (via the `p7zip` package) to handle compressed files. Please make sure it is installed before running the code.

#### Install 7-Zip on Linux

On **Debian/Ubuntu-based systems**:

```bash
sudo apt update
sudo apt install p7zip-full
```

On **Red Hat/Fedora-based systems**:

```bash
sudo dnf install p7zip p7zip-plugins
```

> **Note:** For other distributions, please use your distribution's package manager to install `p7zip`.

### Bioconda

relecov-tools is available in Bioconda and can be installed via conda.

If you already have conda installed. Do the following:

```Bash
conda config --add channels bioconda
conda create --name relecov-tools
conda activate relecov-tools
conda install -c bioconda relecov-tools
```

### Pip

relecov-tools is available in Pypi and can be installed via pip:

```Bash
pip install relecov-tools
```

### Development version

If you want to install the latest code in the repository:

```
conda create -n relecov_dev pip
pip install --force-reinstall --upgrade git+https://github.com/bu-isciii/relecov-tools.git@develop
```

## Usage

### Command-line

relecov-tools provides a command-line version with help descriptions and params prompt if needed.

```
$ relecov-tools --help
             ___   ___       ___  ___  ___
\    |--|   |   \ |    |    |    |    |   | \      /
\    \  /   |__ / |__  |    |___ |    |   |  \    /
/    /  \   |  \  |    |    |    |    |   |   \  /
/    |--|   |   \ |___ |___ |___ |___ |___|    \/
RELECOV-tools version 1.7.3
Usage: relecov-tools [OPTIONS] COMMAND [ARGS]...

Options:
  --version            Show the version and exit.
  -v, --verbose        Print verbose output to the console.
  -l, --log-path TEXT  Creates log file in given folder. Uses default path in
                       config or tmp if empty.
  -d, --debug          Show the full traceback on error for debugging
                       purposes.
  -h, --hex-code TEXT  Define hexadecimal code. This might overwrite existing
                       files with the same hex-code
  --help               Show this message and exit.

Commands:
  download               Download files located in sftp server.
  read-lab-metadata      Create the json compliant to the relecov schema...
  send-mail              Send a sample validation report by mail.
  validate               Validate json file against schema.
  map                    Convert data between phage plus schema to ENA,...
  upload-to-ena          parse data to create xml files to upload to ena
  upload-to-gisaid       parsed data to create files to upload to gisaid
  update-db              upload the information included in json file to...
  read-bioinfo-metadata  Create the json compliant from the Bioinfo...
  metadata-homogeneizer  Parse institution metadata lab to the one used...
  pipeline-manager       Create the symbolic links for the samples which...
  build-schema           Generates and updates JSON Schema files from...
  logs-to-excel          Creates a merged xlsx and Json report from all...
  wrapper                Executes the modules in config file sequentially
  upload-results         Upload batch results to sftp server.
  add-extra-config       Save given file content as additional configuration
```

Further explanation for each argument:

- `--verbose`: Prints all logs as standard output, showing them to the user.
- `--log-path`: Use it to indicate a custom path for all logs to be saved. See [Logging functionality](#logging_functionality) for more information.
- `--debug`: Activate DEBUG logs. When not provided, logs will only show the most relevant information.
- `--hex-code`: By default all files generated will include a date and an unique hexadecimal code which is randomly generated upon execution. Using this argument you can pre-define the resulting hexadecimal code. NOTE: Keep in mind that this could overwrite existing files.

## Modules

#### download

The command `download` connects to a transfer protocol (currently sftp) and downloads all files in the different available folders in the passed credentials. In addition, it checks if the files in the current folder match the files in the metadata file and also checks if there are md5sum for each file. Else, it creates one before storing in the final repository.

```
$ relecov-tools download --help
Usage: relecov-tools download [OPTIONS]

  Download files located in sftp server.

Options:
  -u, --user TEXT             User name for login to sftp server
  -p, --password TEXT         password for the user to login
  -f, --conf_file TEXT        Configuration file (not params file)
  -d, --download_option TEXT  Select the download option: [download_only,download_clean, delete_only].
                              download_only will only download the files.
                              download_clean will remove files from sftp after download.
                              delete_only will only delete the files.
  -o, --output_dir, TEXT      Directory where the generated output will be saved
  -t, --target_folders TEXT   Flag: Select which folders will be targeted
                              giving [paths] or via prompt. For multiple
                              folders use ["folder1", "folder2"]
  -s, --subfolder TEXT        Flag: Specify which subfolder to process
                              (default: RELECOV)
  --help                      Show this message and exit.
```

Configuration can be passed in several formats:

- if no config_file is passed, default values are fetched from conf/configuration.json, and user and password are asked in prompt.
- Default values can be overwritten using a yml config file, so you can input user, password, sftp_server, etc.

Config file example with all available options:

```
sftp_server: "sftprelecov.isciii.es"
sftp_port: "22"
sftp_user : "user"
sftp_passwd : "pass"
storage_local_folder: "/tmp/relecov"
tmp_folder_for_metadata: "/tmp/relecov/tmp"
allowed_sample_extensions:
    - .fastq.gz
    - .fasta
```

#### read-lab-metadata

`read-lab-metadata` command reads the excel file with laboratory metadata and processes it adding additional needed fields.

```
$ relecov-tools read-lab-metadata --help
Usage: relecov-tools read-lab-metadata [OPTIONS]

  Create the json compliant to the relecov schema from the Metadata file.

Options:
  -m, --metadata_file PATH     file containing metadata
  -s, --sample_list_file PATH  Json with the additional metadata to add to the received user metadata
  -o, --output_dir, TEXT       Directory where the generated output will be saved
  -f, --files_folder PATH      Path to folder where samples files are located
  --help                       Show this message and exit.
```

An example for the metadata excel file can be found [here](./relecov_tools/example_data/METADATA_LAB_TEST.xlsx)

#### send-mail

`send-mail` sends a validation summary report by email using predefined Jinja templates. It supports attachments, multiple recipients, and includes the option to add additional notes manually or via a .txt file.

```
$ relecov-tools send-email --help
Usage: relecov-tools send-email [OPTIONS]

  Send a sample validation report by mail.

Options:
  -v, --validate_file PATH     Path to the validation summary json file
                               (validate_log_summary.json)  [required]
  -r, --receiver_email TEXT    Recipient's e-mail address (optional). If not
                               provided, it will be extracted from the
                               institutions guide.
  -a, --attachments PATH       Path to file
  -t, --template_path PATH     Path to relecov-tools templates folder
                               (optional)
  -p, --email_psswd TEXT       Password for bioinformatica@isciii.es
  -n, --additional_notes PATH  Path to a .txt file with additional notes to
                               include in the email (optional).
  --help                       Show this message and exit.
```

#### read-bioinfo-metadata

`read-bioinfo-metadata` Include the results from the Bioinformatics analysis into the Json previously created with read-lab-metadata module.

```bash
$ relecov-tools read-bioinfo-metadata --help
Usage: relecov-tools read-bioinfo-metadata [OPTIONS]

  Create the json compliant  from the Bioinfo Metadata.

Options:
  -j, --json_file PATH            json file containing lab metadata
  -s, --json_schema_file TEXT     Path to the JSON Schema file used for
                                  validation
  -i, --input_folder PATH         Path to input files
  -o, --output_dir, --output-dir, --output_folder, --out-folder, --output_location, --output_path, --out_dir, --output DIRECTORY
                                  Directory where the generated output will be
                                  saved
  -p, --software_name TEXT        Name of the software/pipeline used.
  --update                        If the output file already exists, ask if
                                  you want to update it.
  --soft_validation               If the module should continue even if any
                                  sample does not validate.
  --help                          Show this message and exit.
```

- Note: Software-specific configurations are available in [bioinfo_config.json](./relecov_tools/conf/bioinfo_config.json).

##### Configuration of module `read-bioinfo-metadata`

The [`bioinfo_config.json`](relecov_tools/conf/bioinfo_config.json) file is a configuration file used by the `read-bioinfo-metadata` module. Its purpose is to specify **which files to search for** and **how to extract relevant information** from a folder containing bioinformatics results. With this configuration, the module identifies parameters and results for each sample and returns them in a standardized JSON format.

Structure:

> 1. **Top Level**: Bioinformatics Software Name  
  The top-level keys represent the name of the bioinformatics software used in the analysis (e.g., `"viralrecon"`). Each block contains the configuration for extracting data generated by that specific pipeline.

> 2. **Second Level:** Analysis Stages or Result Files  
Within each software block, the keys correspond to different analysis stages or result files. Each section defines how to locate and process specific output files.

> 3. **Key Fields Explained**  

| **Key**           | **Description**                                                                                             | **Value Type**     |
|-------------------|-------------------------------------------------------------------------------------------------------------|--------------------|
| `fn`              | Regular expression pattern to locate the corresponding files.                                               | String (regex)     |
| `sample_col_idx`  | Index of the column containing the sample identifier (0-based).                                              | Integer            |
| `header_row_idx`  | Index of the row containing the file header.                                                                 | Integer            |
| `required`        | Indicates if the file is mandatory (`true`) or optional (`false`).                                           | Boolean            |
| `function`        | Name of a custom processing function (if applicable). Functions should be located in `assets/pipeline_utils`. | String or `null`   |
| `multiple_samples`| Specifies if the file contains data for multiple samples.                                                    | Boolean            |
| `split_by_batch`  | Indicates whether data should be separated by batches.                                                       | Boolean            |
| `map`  | Indicates whether the file's values should be mapped into the final metadata (`j_data`). If set to `false`, the file is still processed (e.g., to generate the `long_table`), but its content will not be included in `j_data`.                                                       | Boolean            |
| `extract`         | If `true`, instructs the module to extract data from the file’s content.                                     | Boolean            |
| `content`         | Dictionary mapping standardized parameter names to the corresponding columns in the source file.             | Object             |

#### validate

`validate` commands validate the data in json format outputted by `read-metadata` command against a json schema, in this case the relecov [schema specification](./relecov_tools/schema/relecov_schema.json). It also creates a summary of the errors and warnings found in excel format as a report to the users.

```
$ relecov-tools validate --help
Usage: relecov-tools validate [OPTIONS]

  Validate json file against schema.

  Options:
    -j, --json_file TEXT            Json file to validate
    -s, --json_schema_file          TEXT Path to the JSON Schema file used for validation
    -m, --metadata PATH             Origin file containing metadata
    -o, --output_dir,               Directory where the generated output will be saved.
    -e, --excel_sheet TEXT          Optional: Name of the sheet in excel file to validate.
    --upload_files                  Wether to upload the resulting files from validation process or not.
    -l, --logsum_file TEXT          Required if --upload_files. Path to the log_summary.json file merged from all
                                    previous processes, used to check for invalid samples.  
    --help                          Show this message and exit.

```

#### map

The command `map` converts a data in json format from relecov data model to ena or gisaid data model using their own schemas acording to their annotated ontology terms.

```
$ relecov-tools map --help
Usage: relecov-tools map [OPTIONS]

  Convert data between phage plus schema to ENA, GISAID, or any other schema

Options:
  -p, --origin_schema TEXT          File with the origin (relecov) schema
  -j, --json_file TEXT              File with the json data to convert
  -d, --destination_schema [ENA|GISAID|other]
                                    schema to be mapped
  -f, --schema_file TEXT            file with the custom schema
  -o, --output_dir,                 Directory where the generated output will be saved.
  --help                            Show this message and exit.
```

#### upload-to-ena

`upload-to-ena` command uses json data mapped to ena schema to use the [ena_upload_cli](https://github.com/usegalaxy-eu/ena-upload-cli) package to upload raw data and metadata to ENA db.

```
Usage: relecov-tools upload-to-ena [OPTIONS]

  parsed data to create xml files to upload to ena

  Options:
    -u, --user                               user name for login to ena
    -p, --password                           password for the user to login
    -c, --center                             center name
    -e, --ena_json                           where the validated json is
    -t, --template_path                      path to folder containing ENA xml templates
    -a, --action                             select one of the available options: [add|modify|cancel|release]
    --dev                                    Flag: Test submission
    --upload_fastq                           Flag: Upload fastq files. Mandatory for "add" action
    -m", --metadata_types                    List of metadata xml types to submit [study,experiment,run,sample]
    -o, --output_dir                         Directory where the generated output will be saved.
    --help                                   Show this message and exit.

```

#### upload-to-gisaid

`upload-to-gisaid` uses the json mapped to gisaid schema to upload raw data and metadata to GISAID db

```
Usage: relecov-tools upload-to-gisaid [OPTIONS]

  parsed data to create xml files to upload to ena

Options:
  -u, --user TEXT                 user name for login
  -p, --password TEXT             password for the user to login
  -c, --client_id TEXT            client-ID provided by clisupport@gisaid.org
  -t, --token TEXT                path to athentication token
  -e, --gisaid_json TEXT          path to validated json mapped to GISAID
  -i, --input_path TEXT           path to fastas folder or multifasta file
  -o, --output_dir,               Directory where the generated output will be saved.
  -f, --frameshift [catch_all|catch_none|catch_novel]
                                  frameshift notification
  -x, --proxy_config TEXT         introduce your proxy credentials as:
                                  username:password@proxy:port
  --single                        input is a folder with several fasta files.
                                  Default: False
  --gzip                          input fasta is gziped. Default: False
  --help                          Show this message and exit.
```

#### update-db

Upload the information included in json file to the database

```
Usage: relecov-tools update-db [OPTIONS]

  upload the information included in json file to the database

Options:
  -j, --json TEXT                 data in json format
  -t, --type TEXT                 Without --full_update choose one of
                                  [sample|bioinfodata|variantdata]. With --full_update
                                  this flag is ignored.
  -plat, --platform [iskylims|relecov]
                                  name of the platform where data is uploaded
  -u, --user TEXT                 user name for login
  -p, --password TEXT             password for the user to login
  -s, --server_url TEXT           url of the platform server
  -f, --full_update [TEXT]        Run the full update. Use without value to run all
                                  steps. Optionally pass a comma-separated list of
                                  step numbers (e.g. -f 2,3,4) to run only those:
                                  1=sample->iskylims, 2=sample->relecov,
                                  3=bioinfodata->relecov, 4=variantdata->relecov.
  -l, --long_table TEXT           Long_table.json file from read-bioinfo-
                                  metadata + viralrecon
  --help                          Show this message and exit.
```
When running with `--full_update`:
- `-f` (sin argumentos) ejecuta los cuatro pasos en orden.
- `-f 2,3,4` ejecuta sólo sample->relecov, bioinfodata->relecov y
  variantdata->relecov en ese orden.


#### pipeline-manager

Create the folder structure to execute the given pipeline for the latest sample batches after executing download, read-lab-metadata and validate modules. This module will create symbolic links for each sample and generate the necessary files for pipeline execution using the information from validated_BATCH-NAME_DATE.json.

```
Usage: relecov-tools pipeline-manager [OPTIONS]

  Create the symbolic links for the samples which are validated to prepare for
  bioinformatics pipeline execution.

Options:
  -i, --input PATH           select input folder where are located the sample
                             files
  -t, --templates_root PATH  Path to folder containing the pipeline templates
                             from buisciii-tools
  -c, --config PATH          select the template config file
  -o, --output_dir,          Directory where the generated output will be saved.
  -f, --folder_names TEXT    Folder basenames to process. Target folders names
                             should match the given dates. E.g. ... -f folder1
                             -f folder2 -f folder3
  --help                     Show this message and exit.
```

#### wrapper

Execute download, read-lab-metadata and validate sequentially using a config file to fill the arguments for each one. It also creates a global report with all the logs for the three processes in a user-friendly .xlsx format. The config file should include the name of each module that is executed, along with the necessary parameters in YAML format.

```
Usage: relecov-tools wrapper [OPTIONS]

  Executes the modules in config file sequentially

Options:
  -c, --config_file PATH    Path to config file in yaml format  [required]
  -o, --output_dir,         Directory where the generated output will be saved [required].
  --help                    Show this message and exit.
```

#### logs-to-excel

Creates an xlsx file with all the entries found for a specified laboratory in a given set of log_summary.json files (from log-summary module). The laboratory name must match the name of one of the keys in the provided logs to work.

```
Usage: relecov-tools logs-to-excel [OPTIONS]

  Creates a merged xlsx report from all the log summary jsons given as input

Options:
    -l, --lab_name          Name for target laboratory in log-summary.json files
    -o, --output_dir,       Directory where the generated output will be saved [required]. 
    -f, --files             Paths to log_summary.json files to merge into xlsx file, called once per file
```

#### add-extra-config

This command is used to create an additional config file that will override the configuration in `conf/configuration.json`. You may pass this configuration in a YAML or JSON file. If you want the keys in your additional configuration to be grouped under a certain keyname, use param `-n, --config_name`. Otherwise, the file content will be parsed with no additional processing.

```
Usage: relecov-tools add-extra-config [OPTIONS]

  Save given file content as additional configuration

Options:
  -n, --config_name TEXT  Name of the config key that will be added
  -f, --config_file TEXT  Path to the input file: Json or Yaml format
  --force                 Force replacement of existing configuration if
                          needed
  --clear_config          Remove given config_name from extra config: Use with
                          empty --config_name to remove all
  --help                  Show this message and exit.
```

### build-schema

The `build-schema` module provides functionality to generate and manage JSON Schema files based on database definitions from Excel spreadsheets. It automates the creation of JSON Schemas, including validation, drafting, and comparison with existing schemas. Uses the generated JSON schema to create a structured Excel template.

```
Usage: relecov-tools build-schema [OPTIONS]

  Generates and updates JSON Schema files from Excel-based database
  definitions.

Options:
  -i, --input_file PATH           Path to the Excel document containing the
                                  database definition. This file must have a
                                  .xlsx extension.  [required]
  -s, --schema_base PATH          Path to the base schema file. This file is
                                  used as a reference to compare it with the
                                  schema generated using this module.
                                  (Default: installed schema in 'relecov-tools
                                  /relecov_tools/schema/relecov_schema.json')
  -e, --excel_template PATH       Path to the excel template file. This file
                                  is used to get version history of the excel
                                  template (stored in assets/Relecov_metadata_*.xlsx)
  -v, --draft_version TEXT        Version of the JSON schema specification to
                                  be used. Example: '2020-12'. See:
                                  https://json-schema.org/specification-links
  -d, --diff                      Prints a changelog/diff between the base and
                                  incoming versions of the schema.
  --version TEXT                  Specify the schema version.
  -p, --project TEXT              Specficy the project to build the metadata
                                  template.
  --non-interactive               Run the script without user interaction,
                                  using default values.
  -o, --output_dir, --output-dir, --output_folder, --out-folder, --output_location, --output_path, --out_dir, --output DIRECTORY
                                  Directory where the generated output will be saved
  --help                          Show this message and exit.
```

#### Mandatory Fields

Ensure that the fields below are properly defined as headers in your Excel sheet (database definition):

```Bash
enum: List of possible values for enumeration.
examples: Example values for the property.
ontology_id: Identifier for ontology.
type: Data type of the property (e.g., string, integer).
description: Description of the property.
classification: Classification or category of the property.
label_name: Label or name for the property.
fill_mode: Mode for filling in the property (e.g., required, optional).
required (Y/N): Indicates if the property is required (Y) or optional (N).
complex_field (Y/N): Indicates if the property is a complex (nested) field (Y) or a standard field (N).
```

#### Logging_functionality

relecov-tools generate logs by default for all processes using a standard name: `<module>_<batch-date>_<hexcode>.log`

The *--log-path* given via command-line interface (CLI) is used to specify the destination directory where logs will be saved during execution
When provided, it overrides the default logging behavior and directs all log files to the specified folder.

How default Logs are generated (when --log-path is not used):
Use a predefined default location found in configuration.json under `logs_config` key. If the module executed is found in
`modules_outpath` subkey, the log will be generated in the specified folder, otherwise it will be generated in `default_outpath/module`

If you want your logs to be sent to custom locations depending on the module executed you can do so by using add-extra-config, providing

#### Custom logs

After executing each of these modules, you may find a custom log report in json format named `EXECUTED-MODULE_<date>_hex_log_summary.json`. These custom log summaries can be useful to detect errors in metadata in order to fix them and/or notify the users.

### Python package mode

relecov-tools is designed in a way that you can use import the different modules and use them in your own scripts, for example:

```Python
import relecov_tools.sftp_handle
user="admin"
passwd="1234"
conf_file="/path/to/conf"

sftp_connection = relecov_tools.sftp_handle.SftpHandle(
    user, password, conf_file
)
sftp_connection.download()
```

DOCs soon!!

## Acknowledgements

Python package idea and design is really inspired in [nf-core/tools](https://github.com/nf-core/tools).
