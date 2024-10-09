# relecov-tools
[![python_lint](https://github.com/BU-ISCIII/relecov-tools/actions/workflows/python_lint.yml/badge.svg)](https://github.com/BU-ISCIII/relecov-tools/actions/workflows/python_lint.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

> THIS REPO IS UNDER ACTIVE DEVELOPMENT.

relecov-tools is a set of helper tools for the assembly of the different elements in the RELECOV platform (Spanish Network for genomic surveillance of SARS-Cov-2) as data download, processing, validation and upload to public databases, as well as analysis runs and database storage.

## Table of contents

- [relecov-tools](#relecov-tools)
  - [Table of contents](#table-of-contents)
  - [Installation](#installation)
    - [Bioconda](#bioconda)
    - [Pip](#pip)
    - [Development version](#development-version)
  - [Usage](#usage)
    - [Command-line](#command-line)
      - [download](#download)
      - [read-lab-metadata](#read-lab-metadata)
      - [read-bioinfo-metadata](#read-bioinfo-metadata)
      - [validate](#validate)
      - [map](#map)
      - [upload-to-ena](#upload-to-ena)
      - [upload-to-gisaid](#upload-to-gisaid)
      - [update-db](#update-db)
      - [pipeline-manager](#pipeline-manager)
      - [wrapper](#wrapper)
      - [logs-to-excel](#logs-to-excel)
    - [build-schema](#build-schema)
      - [Mandatory Fields](#mandatory-fields)
      - [custom logs](#custom-logs)
    - [Python package mode](#python-package-mode)
  - [Acknowledgements](#acknowledgements)

## Installation

### Bioconda
Soon

### Pip
relecov-tools is available in Pypi and can be installed via pip:
```
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
RELECOV-tools version 1.2.0
Usage: relecov-tools [OPTIONS] COMMAND [ARGS]...

Options:
--version                  Show the version and exit.
-v, --verbose              Print verbose output to the console.
-l, --log-file <filename>  Save a verbose log to a file.
--help                     Show this message and exit.

Commands:
  download               Download files located in sftp server.
  read-lab-metadata      Create the json compliant to the relecov schema...
  validate               Validate json file against schema.
  map                    Convert data between phage plus schema to ENA,...
  upload-to-ena          parse data to create xml files to upload to ena
  upload-to-gisaid       parsed data to create files to upload to gisaid
  update-db              upload the information included in json file to...
  read-bioinfo-metadata  Create the json compliant from the Bioinfo...
  metadata-homogeneizer  Parse institution metadata lab to the one used...
  pipeline-manager       Create the symbolic links for the samples which...
  wrapper                Execute download, read-lab-metadata and validate...
  build-schema           Generates and updates JSON Schema files from...
  logs-to-excel          Creates a merged xlsx report from all the log...
```
#### download
The command `download` connects to a transfer protocol (currently sftp) and downloads all files in the different available folders in the passed credentials. In addition, it checks if the files in the current folder match the files in the metadata file and also checks if there are md5sum for each file. Else, it creates one before storing in the final repository.

```
$ relecov-tools download --help
Usage: relecov-tools download [OPTIONS]

  Download files located in sftp server.

  Options:
    -u, --user            User name for login to sftp server
    -p, --password        Password for the user to login
    -d, --download_option Select the download option: [download_only, download_clean, delete_only].
        download_only will only download the files
        download_clean will remove files from sftp after download
        delete_only will only delete the files
    -o, --output_location Flag: Select location for downloaded files, overrides config file location
    -t, --target_folders  Flag: Select which sftp folders will be targeted giving [paths] or via prompt
    -f, --conf_file       Configuration file in yaml format (no params file)
    --help                Show this message and exit.
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
Usage: relecov-tools read-metadata [OPTIONS]

  Create the json compliant to the relecov schema from the Metadata file.

  Options:
    -m, --metadata_file PATH     file containing metadata in xlsx format.
    -s, --sample_list_file PATH  Json with the additional metadata to add to the
    received user metadata.
    -o, --metadata-out PATH      Path to save output  metadata file in json format.
    --help                       Show this message and exit.
```


An example for the metadata excel file can be found [here](./relecov_tools/example_data/METADATA_LAB_TEST.xlsx)

#### read-bioinfo-metadata
`read-bioinfo-metadata` Include the results from the Bioinformatics analysis into the Json previously created with read-lab-metadata module.

```
$ relecov-tools read-bioinfo-metadata --help
Usage: relecov-tools read-bioinfo-metadata [OPTIONS]

   Create the json compliant to the relecov schema with Bioinfo Metadata.

   Options:
      -j, --json_file       Json file containing lab metadata
      -i, --input_folder    Path to folder containing analysis results
      -s, --software_name   Name of the software employed in the bioinformatics analysis (default: viralrecon).
      -o, --out_dir         Path to save output file"
```
- Note: Software-specific configurations are available in [bioinfo_config.json](./relecov_tools/conf/bioinfo_config.json).

#### validate
`validate` commands validate the data in json format outputted by `read-metadata` command against a json schema, in this case the relecov [schema specification](./relecov_tools/schema/relecov_schema.json). It also creates a summary of the errors and warnings found in excel format as a report to the users.

```
$ relecov-tools validate --help
Usage: relecov-tools validate [OPTIONS]

  Validate json file against schema.

  Options:
    -j, --json_file TEXT    Json file to validate
    -s, --json_schema TEXT  Json schema (default: relecov-schema)
    -m, --metadata PATH     Origin file containing metadata
    -o, --out_folder TEXT   Path to save validate json file
    --help                  Show this message and exit.

```

#### map
The command `map` converts a data in json format from relecov data model to ena or gisaid data model using their own schemas acording to their annotated ontology terms.

```
$ relecov-tools map --help
Usage: relecov-tools map [OPTIONS]

  Convert data between phage plus schema to ENA, GISAID, or any other schema

  Options:
    -p, --origin_schema TEXT        File with the origin (relecov) schema
    -j, --json_data TEXT            File with the json data to convert
    -d, --destination_schema [ENA|GSAID|other]
    schema to be mapped
    -f, --schema_file TEXT          file with the custom schema
    -o, --output TEXT               File name and path to store the mapped json
    --help                          Show this message and exit.
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
    -o, --output_path TEXT                   output folder for the xml generated files
    --help                                   Show this message and exit.

```

#### upload-to-gisaid
`upload-to-gisaid` uses the json mapped to gisaid schema to upload raw data and metadata to GISAID db

```
Usage: relecov-tools upload-to-gisaid [OPTIONS]

  parsed data to create xml files to upload to ena

  Options:
    -u, --user            user name for login
    -p, --password        password for the user to login
    -c, --client_id       client-ID provided by clisupport@gisaid.org
    -t, --token           path to athentication token
    -e, --gisaid_json     path to validated json mapped to GISAID
    -i, --input_path      path to fastas folder or multifasta file
    -f, --frameshift      frameshift notification: ["catch_all", "catch_none", "catch_novel"]
    -x, --proxy_config    introduce your proxy credentials as: username:password@proxy:port
    --single              Flag: input is a folder with several fasta files.
    --gzip                Flag: input fasta is gziped.
```

#### update-db
    -u, --user                         user name for login
    -p, --password                     password for the user to login
    -t, --type                         Select the type of information to upload to database [sample,bioinfodata,variantdata]
    -d, --databaseServer               Name of the database server receiving the data [iskylims,relecov]

#### pipeline-manager
Create the folder structure to execute the given pipeline for the latest sample batches after executing download, read-lab-metadata and validate modules. This module will create symbolic links for each sample and generate the necessary files for pipeline execution using the information from validated_BATCH-NAME_DATE.json.
```
Usage: relecov-tools pipeline-manager [OPTIONS]

  Create the symbolic links for the samples which are validated to prepare for
  bioinformatics pipeline execution.

Options:
  -i, --input PATH          Path to the input folder where sample files are located
  -t, --template PATH       Path to the pipeline template folder to be copied in the                       output folder
  -c, --config PATH         Path to the the template config file
  -o, --out_dir PATH        Path to output folder
  --help                    Show this message and exit.
```

#### wrapper
Execute download, read-lab-metadata and validate sequentially using a config file to fill the arguments for each one. It also creates a global report with all the logs for the three processes in a user-friendly .xlsx format. The config file should include the name of each module that is executed, along with the necessary parameters in YAML format.
```
Usage: relecov-tools wrapper [OPTIONS]

  Executes the modules in config file sequentially

Options:
  -c, --config_file PATH    Path to config file in yaml format  [required]
  -o, --output_folder PATH  Path to folder where global results are saved [required]
  --help                    Show this message and exit.
```

#### logs-to-excel
Creates an xlsx file with all the entries found for a specified laboratory in a given set of log_summary.json files (from log-summary module). The laboratory name must match the name of one of the keys in the provided logs to work.
```
Usage: relecov-tools logs-to-excel [OPTIONS]

  Creates a merged xlsx report from all the log summary jsons given as input

Options:
    -l, --lab_name                         Name for target laboratory in log-summary.json files
    -o, --output_folder                    Path to output folder where xlsx file is saved
    -f, --files                            Paths to log_summary.json files to merge into xlsx file, called once per file
```

### build-schema
The `build-schema` module provides functionality to generate and manage JSON Schema files based on database definitions from Excel spreadsheets. It automates the creation of JSON Schemas, including validation, drafting, and comparison with existing schemas.

```
Usage: relecov-tools build-schema [OPTIONS]

  Generates and updates JSON Schema files from Excel-based database
  definitions.

Options:
  -i, --input_file PATH     Path to the Excel document containing the database
                            definition. This file must have a .xlsx extension.
                            [required]
  -s, --schema_base PATH    Path to the base schema file. This file is used as
                            a reference to compare it with the schema
                            generated using this module. (Default: installed
                            schema in 'relecov-
                            tools/relecov_tools/schema/relecov_schema.json')
  -v, --draft_version TEXT  Version of the JSON schema specification to be
                            used. Example: '2020-12'. See: https://json-
                            schema.org/specification-links
  -d, --diff BOOLEAN        Prints a changelog/diff between the base and
                            incoming versions of the schema.
  -o, --out_dir PATH        Path to save output file/s
  --help                    Show this message and exit.
```

#### Mandatory Fields
Ensure that the fields below are properly defined as headers in your Excel sheet (database definition):

```
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

#### custom logs
After executing each of these modules, you may find a custom log report in json format named "DATE_EXECUTED-MODULE_log_summary.json. These custom log summaries can be useful to detect errors in metadata in order to fix them and/or notify the users.

### Python package mode
relecov-tools is designed in a way that you can use import the different modules and use them in your own scripts, for example:

```
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
