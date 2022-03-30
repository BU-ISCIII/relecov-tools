# relecov-tools
[![python_lint](https://github.com/BU-ISCIII/relecov-tools/actions/workflows/python_lint.yml/badge.svg)](https://github.com/BU-ISCIII/relecov-tools/actions/workflows/python_lint.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

relecov-tools is a set of helper tools for the assembly of the different elements in the RELECOV platform (Spanish Network for genomic surveillance of SARS-Cov-2) as data download, processing, validation and upload to publica databases, as well as analysis runs and database storage.

## Table of contents

* [Installation](#installation)
* [Usage](#usage)
* [download](#download)
* [read-metadata](#read-metadata)
* [validate](#validate)
* [map](#map)
* [upload-to-ena](#upload-to-ena)
* [upload-to-gisaid](#upload-to-gisaid)
* [launch](#launch)
* [update-db](#update-db)

## Installation

### Bioconda
soon

### Pip
soon

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
RELECOV-tools version 0.0.1
Usage: relecov-tools [OPTIONS] COMMAND [ARGS]...

Options:
--version                  Show the version and exit.
-v, --verbose              Print verbose output to the console.
-l, --log-file <filename>  Save a verbose log to a file.
--help                     Show this message and exit.

Commands:
    download          Download files located in sftp server.
    read-metadata     Create the json compliant to the relecov schema from...
    validate          Validate json file against schema.
    map               Convert data between phage plus schema to ENA,...
    upload-to-ena     parsed data to create xml files to upload to ena
    upload-to-gisaid  parsed data to create files to upload to gisaid
    launch            launch viralrecon in hpc
    update-db         feed database with metadata jsons
```


# Python package mode
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


