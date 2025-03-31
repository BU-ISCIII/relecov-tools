# relecov-tools Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.0dev] - 2025-0X-XX : https://github.com/BU-ISCIII/relecov-tools/releases/tag/XXX

### Credits

Code contributions to the release:

- [Pablo Mata](https://github.com/shettland)
- [Alejandro Bernabeu](https://github.com/aberdur)
- [Daniel Valle-Millares](https://github.com/Daniel-VM)
- [Victor Lopez](https://github.com/victor5lm)
- [Jaime Ozáez](https://github.com/jaimeozaez)

### Modules

#### Added enhancements

- Added git CI test for json schema exception scenarios [#434](https://github.com/BU-ISCIII/relecov-tools/pull/434)
- Added custom validator to extend validation exceptions [#429](https://github.com/BU-ISCIII/relecov-tools/pull/429)
- Refactor GitHub Actions & Add build-schema Step [#417](https://github.com/BU-ISCIII/relecov-tools/pull/417)
- Add documentation for `bioinfo_config.json` in README [#415](https://github.com/BU-ISCIII/relecov-tools/pull/415)
- Added a more robust datatype handling in utils.py read_csv_file_return_dict() method [#379](https://github.com/BU-ISCIII/relecov-tools/pull/379)
- Improved relecov template generator and version control [#382](https://github.com/BU-ISCIII/relecov-tools/pull/382)
- Improve "options" interpretation in build-schema and update read-lab-metadata field type [#388](https://github.com/BU-ISCIII/relecov-tools/pull/388)
- Enhance validation of database definitinon values [#389](https://github.com/BU-ISCIII/relecov-tools/pull/389)
- Add schema summary method to build-schema module [#391](https://github.com/BU-ISCIII/relecov-tools/pull/391)
- Implement conditioning on Host_Age and Host_Age_Months [#392](https://github.com/BU-ISCIII/relecov-tools/pull/392)
- Fix schema generation dependence on --diff parameter [#394](https://github.com/BU-ISCIII/relecov-tools/pull/392)
- Update template conditions project dependent [#404](https://github.com/BU-ISCIII/relecov-tools/pull/404)
- Add and fix logs for multiple modules [#406](https://github.com/BU-ISCIII/relecov-tools/pull/406)
- Auto-fill tax_id and host_disease based on organism fields [#407](https://github.com/BU-ISCIII/relecov-tools/pull/407)
- Implement Pull Request Template [#410](https://github.com/BU-ISCIII/relecov-tools/pull/410)
- Now pipeline-manager splits by organism-template first [#412](https://github.com/BU-ISCIII/relecov-tools/pull/412)
- Implement non-interactive execution of build-schema module [#416](https://github.com/BU-ISCIII/relecov-tools/pull/416)
- Add repeated samples to test data [#413](https://github.com/BU-ISCIII/relecov-tools/pull/413)
- Add create_summary_tables.py to assets to process data from a given epidemiological week [#418](https://github.com/BU-ISCIII/relecov-tools/pull/418)
- Add wrapper to github actions (test_sftp_modules) [#409](https://github.com/BU-ISCIII/relecov-tools/pull/409)
- Remove wrapper from github actions (test_sftp_modules) [#421](https://github.com/BU-ISCIII/relecov-tools/pull/421)
- Add Validation for Dropdown Columns: Notify Users of Invalid Entries in build-schema module [#423](https://github.com/BU-ISCIII/relecov-tools/pull/423)
- Test SFTP Login by Updating Port Assignment in wrapper_manager [#426](https://github.com/BU-ISCIII/relecov-tools/pull/426)
- Update Test Data for new Schema & Modify JSON Filepaths in read-bioinfo-metadata [#427](https://github.com/BU-ISCIII/relecov-tools/pull/427)
- Update download Module to Process Data by Laboratory COD and Project Subfolder [#431](https://github.com/BU-ISCIII/relecov-tools/pull/431)
- Created new `upload-results` for uploading analysis_results folder back to every COD folder in sftp. [#433](https://github.com/BU-ISCIII/relecov-tools/pull/433)
- Update relecov_schema.json to 3.0.0dev version [#435](https://github.com/BU-ISCIII/relecov-tools/pull/435)
- Fix viralrecon_filepaths Path in read-bioinfo-metadata Module [#438](https://github.com/BU-ISCIII/relecov-tools/pull/438)
- Fix adding_ontology_to_enum when enum has no ontology [#439](https://github.com/BU-ISCIII/relecov-tools/pull/439)
- Update relecov_schema.json and read-bioinfo-metadata to v3.0.0 [#442](https://github.com/BU-ISCIII/relecov-tools/pull/442)

#### Fixes

- Fixed configuration to include mandatory values needed when interacting with Relecov API [#436](https://github.com/BU-ISCIII/relecov-tools/pull/436)
- Fix linting when Pull Request is closed [#404](https://github.com/BU-ISCIII/relecov-tools/pull/404)
- Fix removal of samples with repeated sampleID and .fastq files [#413](https://github.com/BU-ISCIII/relecov-tools/pull/413)
- Fix renaming of folders withou any valid sample [#413](https://github.com/BU-ISCIII/relecov-tools/pull/413)
- Fix download module when deleting corrupted pair-end data [#419](https://github.com/BU-ISCIII/relecov-tools/pull/419)
- Fixed the upload_results module to handle exceptions properly [#441](https://github.com/BU-ISCIII/relecov-tools/pull/441)

#### Changed

- Temporarily changed bioinfo_config 'quality_control' requirement to false [#379](https://github.com/BU-ISCIII/relecov-tools/pull/379)
- Improve and fix minor issues in build_schema.py [#404](https://github.com/BU-ISCIII/relecov-tools/pull/404)
- Changed utils.write_json_fo_file() name to write_json_to_file() [#412](https://github.com/BU-ISCIII/relecov-tools/pull/412)
- Changed pipeline-manager --template param to --templates_root, now points to root folder [#412](https://github.com/BU-ISCIII/relecov-tools/pull/412)

#### Removed

### Requirements

## [1.4.0] - 2025-01-27 : https://github.com/BU-ISCIII/relecov-tools/releases/tag/v1.4.0

### Credits

Code contributions to the release:

- [Sarai Varona](https://github.com/svarona)
- [Alejandro Bernabeu](https://github.com/aberdur)
- [Victor Lopez](https://github.com/victor5lm)

### Modules

#### Added enhancements

- Added a IonTorrent flow cell for validation [#363](https://github.com/BU-ISCIII/relecov-tools/pull/363)
- Added solution to timeout in upload-to-ena module [#368](https://github.com/BU-ISCIII/relecov-tools/pull/368)
- Added log functionality to build-schema module [#340](https://github.com/BU-ISCIII/relecov-tools/pull/340)
- Updated the metadata_processing field in configuration.json and added the other_preparation_kit, quality_control_metrics and consensus_criteria fields in the json schema [#372](https://github.com/BU-ISCIII/relecov-tools/pull/372)
- Added quality control functionality to read-bioinfo-metadata [#373](https://github.com/BU-ISCIII/relecov-tools/pull/373)
- Added dropdown functionality to build-schema enums [#374](https://github.com/BU-ISCIII/relecov-tools/pull/374)

#### Fixes

- Fixed read-bioinfo-metadata module [#367](https://github.com/BU-ISCIII/relecov-tools/pull/367)

#### Changed

#### Removed

### Requirements

## [1.3.0] - 2024-12-23 : https://github.com/BU-ISCIII/relecov-tools/releases/tag/v1.3.0

### Credits

Code contributions to the release:

- [Pablo Mata](https://github.com/Shettland)
- [Sergio Olmos](https://github.com/OPSergio)
- [Sarai Varona](https://github.com/svarona)

### Modules

- Included files-folder option for read-lab-metadata when no samples_data.json is provided [#330](https://github.com/BU-ISCIII/relecov-tools/pull/330)
- Included folder_names multiple arg for pipeline_manager to specify names of folders to process [#331](https://github.com/BU-ISCIII/relecov-tools/pull/331)
- Include send-mail. Automated email notification module to generate and send validation reports. [#328](https://github.com/BU-ISCIII/relecov-tools/pull/328)

#### Added enhancements

- Now logs-to-excel can handle logs with multiple keys and includes folder logs [#329](https://github.com/BU-ISCIII/relecov-tools/pull/329)
- Improved logging messages for duplicated sample IDs in read-lab and download modules [#330](https://github.com/BU-ISCIII/relecov-tools/pull/330)
- Included a new method string_to_date() in utils to search for a date pattern within a string [#331](https://github.com/BU-ISCIII/relecov-tools/pull/331)
- Integrated jinja2 for template rendering in the mail module. [#328](https://github.com/BU-ISCIII/relecov-tools/pull/328)
- Configurations for the mail module added to configuration.json [#328](https://github.com/BU-ISCIII/relecov-tools/pull/328)
- Added static method get_invalid_count in log_summary.py [#328](https://github.com/BU-ISCIII/relecov-tools/pull/328)
- Included a try-except for every module to catch unexpected errors in __main__.py [#339](https://github.com/BU-ISCIII/relecov-tools/pull/339)
- Added, removed and renamed collecting institutions and their cities [#340](https://github.com/BU-ISCIII/relecov-tools/pull/340)
- Updated contact directory to integrate additional institution data. [#349](https://github.com/BU-ISCIII/relecov-tools/pull/349)
- Added support for multiple recipients in the email_receiver field. [#349](https://github.com/BU-ISCIII/relecov-tools/pull/349)
- Introduced a new Jinja template for successful and error validation reports. [#349](https://github.com/BU-ISCIII/relecov-tools/pull/349)
- Modified the module logic to dynamically select and render email templates based on user input. [#349](https://github.com/BU-ISCIII/relecov-tools/pull/349)
- Enhanced email formatting and added a default CC to bioinformatica@isciii.es. [#349](https://github.com/BU-ISCIII/relecov-tools/pull/349)
- Validate module now takes an optional argument to select the name of the sheet to check in excel file [#357](https://github.com/BU-ISCIII/relecov-tools/pull/357)
- Fixed email module [#361](https://github.com/BU-ISCIII/relecov-tools/pull/361)

#### Fixes

- Fixed python linting workflow was still waiting for .py files[#335](https://github.com/BU-ISCIII/relecov-tools/pull/335)
- Now files-folder arg works with relative paths in read-lab-metadata [#339](https://github.com/BU-ISCIII/relecov-tools/pull/339)
- Now check-gzip-integrity() catches any exception in utils.py as it only needs to return True when file can be decompressed [#339](https://github.com/BU-ISCIII/relecov-tools/pull/339)
- Now validate modules does not crash when no METADATA_LAB sheet is found. [#357](https://github.com/BU-ISCIII/relecov-tools/pull/357)

#### Changed

- Pipeline-manager fields_to_split is now in configuration.json to group samples by those fields [#331](https://github.com/BU-ISCIII/relecov-tools/pull/331)
- Homogeneized style of report global report sheet in logs-excel [#339](https://github.com/BU-ISCIII/relecov-tools/pull/339)

#### Removed

### Requirements

## [1.2.0] - 2024-10-11 : https://github.com/BU-ISCIII/relecov-tools/releases/tag/v1.2.0

### Credits

Code contributions to the release:

- [Juan Ledesma](https://github.com/juanledesma78)
- [Pablo Mata](https://github.com/Shettland)
- [Sergio Olmos](https://github.com/OPSergio)

### Modules

- Included wrapper module to launch download, read-lab-metadata and validate processes sequentially [#322](https://github.com/BU-ISCIII/relecov-tools/pull/322)
- Changed launch-pipeline name for pipeline-manager when tools are used via CLI [#324](https://github.com/BU-ISCIII/relecov-tools/pull/324)

#### Added enhancements

- Now also check for gzip file integrity after download. Moved cleaning process to end of workflow [#313](https://github.com/BU-ISCIII/relecov-tools/pull/313)
- Introduced a decorator in sftp_client.py to reconnect when conection is lost [#313](https://github.com/BU-ISCIII/relecov-tools/pull/313)
- Add Hospital Universitari Doctor Josep Trueta to laboratory_address.json [#316] (https://github.com/BU-ISCIII/relecov-tools/pull/316)
- samples_data json file is no longer mandatory as input in read-lab-metadata [#314](https://github.com/BU-ISCIII/relecov-tools/pull/314)
- Included handling of alternative column names to support two distinct headers using the same schema in read-lab-metadata [#314](https://github.com/BU-ISCIII/relecov-tools/pull/314)
- Included a new hospital (Hospital Universitario Araba) to laboratory_address.json [#315](https://github.com/BU-ISCIII/relecov-tools/pull/315) 
- More accurate cleaning process, skipping only sequencing files instead of whole folder [#321](https://github.com/BU-ISCIII/relecov-tools/pull/321)
- Now single logs summaries are also created for each folder during download [#321](https://github.com/BU-ISCIII/relecov-tools/pull/321)
- Introduced handling for missing/dup files and more accurate information in prompt for pipeline_manager [#321](https://github.com/BU-ISCIII/relecov-tools/pull/321)
- Included excel resize, brackets removal in messages and handled exceptions in log_summary.py [#322](https://github.com/BU-ISCIII/relecov-tools/pull/322)
- Included processed batchs and samples in read-bioinfo-metadata log summary [#324](https://github.com/BU-ISCIII/relecov-tools/pull/324)
- When no samples_data.json is given, read-lab-metadata now creates a new one [#324](https://github.com/BU-ISCIII/relecov-tools/pull/324)
- Handling for missing sample ids in read-lab-metadata [#324](https://github.com/BU-ISCIII/relecov-tools/pull/324)
- Better logging for download, read-lab-metadata and wrapper [#324](https://github.com/BU-ISCIII/relecov-tools/pull/324)
- Add SQK-RBK114-96 to library_preparation_kit schema [#333](https://github.com/BU-ISCIII/relecov-tools/pull/333)
- Corrected the Submitting_institution for Hospital de Valdepeñas, Centro de Salud Altagracia and Hospital General de Santa Bárbara.[#334](https://github.com/BU-ISCIII/relecov-tools/pull/334)
- Added Hospital General de Tomelloso. [#334](https://github.com/BU-ISCIII/relecov-tools/pull/334)
#### Fixes

- Fixed wrong city name in relecov_tools/conf/laboratory_address.json [#320](https://github.com/BU-ISCIII/relecov-tools/pull/320)
- Fixed wrong single-paired layout detection in metadata due to Capital letters [#321](https://github.com/BU-ISCIII/relecov-tools/pull/321)
- Error handling in merge_logs() and create_logs_excel() methods for log_summary.py [#322](https://github.com/BU-ISCIII/relecov-tools/pull/322)
- Included handling of multiple empty rows in metadata xlsx file [#322](https://github.com/BU-ISCIII/relecov-tools/pull/322)

#### Changed

- Renamed and refactored "bioinfo_lab_heading" for "alt_header_equivalences" in configuration.json [#314](https://github.com/BU-ISCIII/relecov-tools/pull/314)
- Included a few schema fields that were missing or outdated, related to bioinformatics results [#314](https://github.com/BU-ISCIII/relecov-tools/pull/314)
- Updated metadata excel template, moved to relecov_tools/assets [#320](https://github.com/BU-ISCIII/relecov-tools/pull/320)
- Now python lint only triggers when PR includes python files [#320](https://github.com/BU-ISCIII/relecov-tools/pull/320)
- Moved concurrency to whole workflow instead of each step in test_sftp-handle.yml [#320](https://github.com/BU-ISCIII/relecov-tools/pull/320)
- Updated test_sftp-handle.yml testing datasets [#320](https://github.com/BU-ISCIII/relecov-tools/pull/320)
- Now download skips folders containing "invalid_samples" in its name [#321](https://github.com/BU-ISCIII/relecov-tools/pull/321)
- read-lab-metadata: Some warnings now include label. Also removed trailing spaces [#322](https://github.com/BU-ISCIII/relecov-tools/pull/322)
- Renamed launch-pipeline for pipeline-manager and updated keys in configuration.json [#324](https://github.com/BU-ISCIII/relecov-tools/pull/324)
- Pipeline manager now splits data based on enrichment_panel and version. One folder for each group [#324](https://github.com/BU-ISCIII/relecov-tools/pull/324) 

#### Removed

- Removed duplicated tests with pushes after PR was merged in test_sftp-handle [#312](https://github.com/BU-ISCIII/relecov-tools/pull/312)
- Deleted deprecated auto-release in pypi_publish as it does not work with tag pushes anymore [#312](https://github.com/BU-ISCIII/relecov-tools/pull/312)
- Removed first sleep time for reconnection decorator in sftp_client.py, sleep time now increases in the second attempt [#321](https://github.com/BU-ISCIII/relecov-tools/pull/321)

### Requirements

## [1.1.0] - 2024-09-13 : https://github.com/BU-ISCIII/relecov-tools/releases/tag/v1.1.0

### Credits

Code contributions to the release:

- [Pablo Mata](https://github.com/Shettland)
- [Sara Monzón](https://github.com/saramonzon)

### Modules

- New logs-to-excel function to create an excel file given a list of log-summary.json files [#300](https://github.com/BU-ISCIII/relecov-tools/pull/300)

#### Added enhancements

- Included a way to extract pango-designation version in read-bioinfo-metadata [#299](https://github.com/BU-ISCIII/relecov-tools/pull/299)
- Now log_summary.py also creates an excel file with the process logs [#300](https://github.com/BU-ISCIII/relecov-tools/pull/300)
- Read-bioinfo-metadata splits files and data by batch of samples [#306](https://github.com/BU-ISCIII/relecov-tools/pull/306)
- Included a sleep time in test_sftp-handle to avoid concurrency check failure [#308](https://github.com/BU-ISCIII/relecov-tools/pull/308)

#### Fixes

- Fixes in launch_pipeline including creation of samples_id.txt and joined validated json [#303](https://github.com/BU-ISCIII/relecov-tools/pull/303)
- Fixed failing module_tests.yml workflow due to deprecated upload-artifact version [#308](https://github.com/BU-ISCIII/relecov-tools/pull/308)

#### Changed

- Changed pypi_publish action to publish on every release, no need to push tags [#308](https://github.com/BU-ISCIII/relecov-tools/pull/308)

#### Removed

- Removed only_samples argument in log_summary.py as it was not used in any module. [#300](https://github.com/BU-ISCIII/relecov-tools/pull/300)

### Requirements

## [1.0.0] - 2024-09-02 : https://github.com/BU-ISCIII/relecov-tools/releases/tag/1.0.0

### Credits

Code contributions to the inital release:

- [Sara Monzón](https://github.com/saramonzon)
- [Sarai Varona](https://github.com/svarona)
- [Guillermo Gorines](https://github.com/GuilleGorines)
- [Pablo Mata](https://github.com/Shettland)
- [Luis Chapado](https://github.com/luissian)
- [Erika Kvalem](https://github.com/ErikaKvalem)
- [Alberto Lema](https://github.com/Alema91)
- [Daniel Valle](https://github.com/Daniel-VM)
