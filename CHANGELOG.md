# relecov-tools Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.X.X] - 2025-XX-XX : https://github.com/BU-ISCIII/relecov-tools/releases/tag/v1.X.X

### Credits

#### Added enhancements

#### Fixes

#### Changed

#### Removed

### Requirements

## [1.6.1] - 2025-07-28 : https://github.com/BU-ISCIII/relecov-tools/releases/tag/v1.6.1

### Credits

- [Pablo Mata](https://github.com/shettland)
- [Victor Lopez](https://github.com/victor5lm)
- [Alejandro Bernabeu](https://github.com/aberdur)

#### Added enhancements

- Expanded rest_api.get_request() method to also accept credentials and params [#721](https://github.com/BU-ISCIII/relecov-tools/pull/721)
- Included a new validation workflow to discard samples that are already in the platform database [#721](https://github.com/BU-ISCIII/relecov-tools/pull/721)
- New flag 'check_db' was introduced in validate to activate checking of samples in platform [#721](https://github.com/BU-ISCIII/relecov-tools/pull/721)
- Included docstrings for all methods in rest_api.py [#721](https://github.com/BU-ISCIII/relecov-tools/pull/721)
- Updated create_summary_tables.py to organize data jointly [#722](https://github.com/BU-ISCIII/relecov-tools/pull/722).
- Updated schema and configuration for Ion Torrent [#724](https://github.com/BU-ISCIII/relecov-tools/pull/724)
- Refactor configuration.json structure by grouping fields by module usage[#725](https://github.com/BU-ISCIII/relecov-tools/pull/725)
- Standardized rest_api responses for all methods with a new static method called standardize_response [#741](https://github.com/BU-ISCIII/relecov-tools/pull/741)


#### Fixes

- Fixed target_folders selection problems in download module [#736](https://github.com/BU-ISCIII/relecov-tools/pull/736)
- Removed override of init input params using config in download [#736](https://github.com/BU-ISCIII/relecov-tools/pull/736)
- Fix detection of missing label in JSON-Schema properties [#743](https://github.com/BU-ISCIII/relecov-tools/pull/743)
- Fix wrapper file-name lookup and prevent upload errors of invalid sample uploaded by download module [#748](https://github.com/BU-ISCIII/relecov-tools/pull/748)

#### Changed

- Moved sample_id_ontology previously hard-coded in validate.py to configuration.json [#733](https://github.com/BU-ISCIII/relecov-tools/pull/733)
- Removed RELECOV as default subfolder for upload-results and download from __main__.py [#736](https://github.com/BU-ISCIII/relecov-tools/pull/736)
- Adapted upload_database to new standard RestApi responses [#741](https://github.com/BU-ISCIII/relecov-tools/pull/741)
- RestApi methods now return Response.json.data if possible in data key [#744](https://github.com/BU-ISCIII/relecov-tools/pull/744)

#### Removed

### Requirements

## [1.6.0] - 2025-07-04 : https://github.com/BU-ISCIII/relecov-tools/releases/tag/v1.6.0

### Credits

- [Sara Monzon](https://github.com/saramonzon)
- [Jaime Ozáez](https://github.com/jaimeozaez)
- [Pablo Mata](https://github.com/shettland)
- [Sarai Varona](https://github.com/svarona)
- [Victor Lopez](https://github.com/victor5lm)
- [Alejandro Bernabeu](https://github.com/aberdur)

#### Added enhancements

- Included sftp files missing in metadata as samples in log_summary so they appear in final report [#685](https://github.com/BU-ISCIII/relecov-tools/pull/685)
- Added a custom FormatChecker in custom_validators.py to check that date is in the desired range [#687](https://github.com/BU-ISCIII/relecov-tools/pull/687)
- Included a new field in configuration.json called starting_date to be used by validate custom formatchecker [#687](https://github.com/BU-ISCIII/relecov-tools/pull/687)
- Now validate_instances() method can also be given a custom validator as input [#687](https://github.com/BU-ISCIII/relecov-tools/pull/687)
- Added validation for read-bioinfo-metadata [#673](https://github.com/BU-ISCIII/relecov-tools/pull/673)
- Remove ontology from metadata excel generated in build-schema [#698](https://github.com/BU-ISCIII/relecov-tools/pull/698)
- Implemented new workflow in validate module to upload invalid samples and report to sftp [#708](https://github.com/BU-ISCIII/relecov-tools/pull/708)
- Created tests/test_validate.py to test the new validate workflow [#708](https://github.com/BU-ISCIII/relecov-tools/pull/708)
- Improved error message in upload_file method from sftp_client [#708](https://github.com/BU-ISCIII/relecov-tools/pull/708)
- Read-bioinfo-metadata now also includes valid samples in its log_summary, not only invalid [#708](https://github.com/BU-ISCIII/relecov-tools/pull/708)
- Included software_versions.yml required file for read-bioinfo-metadata tests [#713](https://github.com/BU-ISCIII/relecov-tools/pull/713)

#### Fixes

- Fixed output_folder to output_dif in logs_to_excel params [#658](https://github.com/BU-ISCIII/relecov-tools/pull/658)
- Fixed an issue where no message was printed to stdout/stderr when an exception was raised, making debugging harder [#672](https://github.com/BU-ISCIII/relecov-tools/pull/672)
- Fixed logic to ensure that example entries are not silently skipped when empty — now they are properly validated or reported [#672](https://github.com/BU-ISCIII/relecov-tools/pull/672)
- Corrected some geo_loc_region names  [#674](https://github.com/BU-ISCIII/relecov-tools/pull/674)
- Fixed error when subfolder was a part of the main_folder name e.g. FOLDER_RELECOV. Fixes #646 [#679](https://github.com/BU-ISCIII/relecov-tools/pull/679)
- Fixed wrapper crash when there was no invalid samples for a given folder. Fixes #678 [#679](https://github.com/BU-ISCIII/relecov-tools/pull/679)
- Added a clear error message when library_layout is missing in the metadata. [#680](https://github.com/BU-ISCIII/relecov-tools/pull/680)
- AnyOf error messages are now correctly managed in validation process [#684](https://github.com/BU-ISCIII/relecov-tools/pull/684)
- Fixed incorrect sample count in validate summary message for date fields [#684](https://github.com/BU-ISCIII/relecov-tools/pull/684)
- Included a small hotfix in formatchecker to work with non-string fields [#689](https://github.com/BU-ISCIII/relecov-tools/pull/689)
- Fixed filepaths names and split table names [#673](https://github.com/BU-ISCIII/relecov-tools/pull/673)
- Replaced duplicate flag for read-bioinfo-metadata module [#695](https://github.com/BU-ISCIII/relecov-tools/pull/695).
- fix invalid samples excel header removal [#696](https://github.com/BU-ISCIII/relecov-tools/pull/696).
- Fixed download module behavior when download_only mode is selected [#707](https://github.com/BU-ISCIII/relecov-tools/pull/707).
- Fixed incorrect filename format for logs generated by logs_to_excel module [#705](https://github.com/BU-ISCIII/relecov-tools/pull/705).
- Fixed error with extra_dict-bioinfo-config files as they were being mapped even though they had custom functions to do so [#708](https://github.com/BU-ISCIII/relecov-tools/pull/708)
- Substituted multiple missleading sys.exit() for raise ValueError in read-bioinfo-metadata [#713](https://github.com/BU-ISCIII/relecov-tools/pull/713)
- Remove lineage_assignment_date transformation from handle_pangolin_data [#716](https://github.com/BU-ISCIII/relecov-tools/pull/716)

#### Changed

- Forced log-summary and report.xlsx files to be saved in --log-path if given [#681](https://github.com/BU-ISCIII/relecov-tools/pull/681)
- Validate_instances method is now an static method and does not require SchemaValidation initialization [#684](https://github.com/BU-ISCIII/relecov-tools/pull/684)
- Summarize_errors is now an independent function instead of being part of validate_instances method [#684](https://github.com/BU-ISCIII/relecov-tools/pull/684)
- Unique_id generation is now an independent function instead of being part of validate_instances method [#684](https://github.com/BU-ISCIII/relecov-tools/pull/684)
- Param validation to update unique_id_registry has been moved out of class _\_init__ [#684](https://github.com/BU-ISCIII/relecov-tools/pull/684)
- Param validation to generate invalid_samples.xlsx file has been moved out of class _\_init__ [#684](https://github.com/BU-ISCIII/relecov-tools/pull/684)
- Method get_sample_id_field is now SchemaValidation static method to get a field from a given schema by its ontology [#684](https://github.com/BU-ISCIII/relecov-tools/pull/684)
- Method validate_schema now checks if each property has label and gives warning if not present [#684](https://github.com/BU-ISCIII/relecov-tools/pull/684)
- Updated schema and metadata template version to v3.1.2 removing old date pattern validation [#687](https://github.com/BU-ISCIII/relecov-tools/pull/687)
- Object init in main has been included in try block [#690](https://github.com/BU-ISCIII/relecov-tools/pull/690)
- conf_file param is allowed to be empty besides None in sftp and download module [#690](https://github.com/BU-ISCIII/relecov-tools/pull/690)
- Re-write read-bioinfo-metadata module and long table code [#673](https://github.com/BU-ISCIII/relecov-tools/pull/673)
- Changed pass reads to genome coverage in quality control check [#673](https://github.com/BU-ISCIII/relecov-tools/pull/673)
- add excel_template option to build-schema and handle its path in SchemaBuilder [#693](https://github.com/BU-ISCIII/relecov-tools/pull/693)
- make path parameter optional in prompt_create_outdir function [#693](https://github.com/BU-ISCIII/relecov-tools/pull/693)
- refactor SchemaBuilder initialization and validation logic for clarity and efficiency [#693](https://github.com/BU-ISCIII/relecov-tools/pull/693)
- refactor versioning logic in save_new_schema method to utilize - excel_template for version history [#693](https://github.com/BU-ISCIII/relecov-tools/pull/693)
- Improve message when error raises while reading metadata excel [#694](https://github.com/BU-ISCIII/relecov-tools/pull/694)
- Renamed script files and classes to improve simplicity and clarity [#696](https://github.com/BU-ISCIII/relecov-tools/pull/696)
- added raw strings to prevent syntax warnings [#696](https://github.com/BU-ISCIII/relecov-tools/pull/696)
- Set 3.8 python version as min version [#696](https://github.com/BU-ISCIII/relecov-tools/pull/696)
- fix deprecated warning pkg_resources [#696](https://github.com/BU-ISCIII/relecov-tools/pull/696)
- Included new args in validate module to upload invalid samples and report [#708](https://github.com/BU-ISCIII/relecov-tools/pull/708)
- Invalid files are not removed from download workflow so now metadata can be validated later [#708](https://github.com/BU-ISCIII/relecov-tools/pull/708)
- Invalid folders based on log summary are not skipped during wrapper process [#708](https://github.com/BU-ISCIII/relecov-tools/pull/708)
- Now if sftp_client fetch fails in get_from_sftp method, local temp file is removed [#708](https://github.com/BU-ISCIII/relecov-tools/pull/708)
- Updated validate test files to new upload validation workflow test [#708](https://github.com/BU-ISCIII/relecov-tools/pull/708)
- Validate.lab_code is now extracted from json_data if possible [#708](https://github.com/BU-ISCIII/relecov-tools/pull/708)
- Included new Validate.self attributes to be used by new github test [#708](https://github.com/BU-ISCIII/relecov-tools/pull/708)
- Removed wrapper own code to upload invalid files as it is now done by validate module [#708](https://github.com/BU-ISCIII/relecov-tools/pull/708)
- read-bioinfo-metadata dynamic function exec and eval removed for importlib which is more robust [#713](https://github.com/BU-ISCIII/relecov-tools/pull/713)
- Added --soft_validation to read-bioinfo-metadata test [#713](https://github.com/BU-ISCIII/relecov-tools/pull/713)

#### Removed

- Removed config_file parameter from wrapper, now download, read_lab_metadata and validate params must be provided via extra config [#690](https://github.com/BU-ISCIII/relecov-tools/pull/690).
- Removed duplicate mail-related key in initial_config.yaml [#697](https://github.com/BU-ISCIII/relecov-tools/pull/697).

### Requirements

## [1.5.5] - 2025-06-16 : https://github.com/BU-ISCIII/relecov-tools/releases/tag/v1.5.5

### Credits

- [Pablo Mata](https://github.com/shettland)
- [Sergio Olmos](https://github.com/OPSergio)
- [Jaime Ozáez](https://github.com/jaimeozaez)
- [Alejandro Bernabeu](https://github.com/aberdur)
- [Victor Lopez](https://github.com/victor5lm)
- [Sarai Varona](https://github.com/svarona)
- [Daniel Valle](https://github.com/Daniel-VM)

#### Added enhancements

- Added zip file removal step in upload_results module [#613](https://github.com/BU-ISCIII/relecov-tools/pull/613)
- Now all keys in log_summary files can be merged. Not only for a given lab-code [#615](https://github.com/BU-ISCIII/relecov-tools/pull/615)
- Add GitHub Actions test for read-bioinfo-metadata module [#616](https://github.com/BU-ISCIII/relecov-tools/pull/616)
- Validate that host_age_years and host_age_months are not both filled [#617](https://github.com/BU-ISCIII/relecov-tools/pull/617)
- Included alternative ID selectino when no sequencing_sample_id is provided in excel file [#619](https://github.com/BU-ISCIII/relecov-tools/pull/619)
- Included long_table param for update_db as required for --full_update [#619](https://github.com/BU-ISCIII/relecov-tools/pull/619)
- Fill missing sample_fields required in iskylims with Not Provided in update-db [#620](https://github.com/BU-ISCIII/relecov-tools/pull/620)
- Add support for central CLI and extra_config parameter configuration [#629](https://github.com/BU-ISCIII/relecov-tools/pull/629)
- Include traceback from unexpected errors in logfile [#648](https://github.com/BU-ISCIII/relecov-tools/pull/648)
- Added IRMA quality control and filtering of read-bioinfo-metadata properties based on schema [#649](https://github.com/BU-ISCIII/relecov-tools/pull/649)
- Updated schema version to 3.1.1 with limits for pcr_ct values and dates [#651](https://github.com/BU-ISCIII/relecov-tools/pull/651)
- Added enums for autonomous communities and provinces in to 3.1.1 schema version [#655](https://github.com/BU-ISCIII/relecov-tools/pull/655)

#### Fixes

- Restricting filehandler search to basemodule outdir to fix logfiles with temp_id in its name [#607](https://github.com/BU-ISCIII/relecov-tools/pull/607)
- Create invalid_samples sftp dir only if there are invalid samples in wrapper [#607](https://github.com/BU-ISCIII/relecov-tools/pull/607)
- Fixed wrong call to get batch_id from data in read-lab-metadata [#607](https://github.com/BU-ISCIII/relecov-tools/pull/607)
- Fix incorrect bioinformatics_analysis_date key [#614](https://github.com/BU-ISCIII/relecov-tools/pull/614)
- Fixed analysis_date format in create_summary_tables.py, apart from the way consensus files are retrieved [#624](https://github.com/BU-ISCIII/relecov-tools/pull/624).
- Fix date validators in Excel templates [#617](https://github.com/BU-ISCIII/relecov-tools/pull/617)
- Fix date formatting in read-lab-metadata [#632](https://github.com/BU-ISCIII/relecov-tools/pull/632)
- Map `"NA"` to `"Not Provided [SNOMED:434941000124101]"` in non-required fields of `read-bioinfo-metadata` [#633](https://github.com/BU-ISCIII/relecov-tools/pull/633)
- Fill `None` or `"NA"` values in non-required fields with `"Not Provided [SNOMED:434941000124101]"` in `upload_db` module [#633](https://github.com/BU-ISCIII/relecov-tools/pull/633)
- Improve error handling when `"Library Layout"` field is null in the `download` module [#633](https://github.com/BU-ISCIII/relecov-tools/pull/633)
- Fixed irma config to propperly find vcf files [#635](https://github.com/BU-ISCIII/relecov-tools/pull/635)
- Fixed bug when reading old excel files with openpyxl. Added pandas as fallback reader [#637](https://github.com/BU-ISCIII/relecov-tools/pull/637)
- Created pipeline utils and added function for versions yaml [#638](https://github.com/BU-ISCIII/relecov-tools/pull/638)
- Fixed behaviour of upload_database module [#650](https://github.com/BU-ISCIII/relecov-tools/pull/650)
- Hotfix to solve crash when no latest_template file was found in build-schema [#651](https://github.com/BU-ISCIII/relecov-tools/pull/651)
- Fixed error when header was not in row 2 along with custom msg for dates out of range [#651](https://github.com/BU-ISCIII/relecov-tools/pull/651)
- Fixed possible error when no R1 or no R2 files were provided in excel file for download [#651](https://github.com/BU-ISCIII/relecov-tools/pull/651)

#### Changed

- Update test datasets for GitHub Actions [#614](https://github.com/BU-ISCIII/relecov-tools/pull/614)
- Modified compression function in upload_results module to use 7z instead of AES-ZIP [#618](https://github.com/BU-ISCIII/relecov-tools/pull/618)
- Update relecov_schema to v3.1.0 [#632](https://github.com/BU-ISCIII/relecov-tools/pull/632)
- Homogenised names of autonomous communities and provinces in accordance with INE information [#653](https://github.com/BU-ISCIII/relecov-tools/pull/653)

#### Removed

- Removed hardcoded `sample_entry_date` and legacy fallback logic from `upload_database` mapping. [#610](https://github.com/BU-ISCIII/relecov-tools/pull/610)


### Requirements

- A new dependency has been added to `relecov-tools`: the 7-Zip software is now required. [#622](https://github.com/BU-ISCIII/relecov-tools/pull/622)

## [1.5.4] - 2025-05-19 : https://github.com/BU-ISCIII/relecov-tools/releases/tag/v1.5.4

### Credits

- [Pablo Mata](https://github.com/shettland)
- [Alejandro Bernabeu](https://github.com/aberdur)

#### Added enhancements

#### Fixes

- Fixed self.json_file call before it was set in update-db [#598](https://github.com/BU-ISCIII/relecov-tools/pull/598)
- Included missing http in server_url from configuration.json [#598](https://github.com/BU-ISCIII/relecov-tools/pull/598)
- Enhance Sample Presence Validation for read-bioinfo-metadata Processing [#599](https://github.com/BU-ISCIII/relecov-tools/pull/598)
- Update relecov schema/template to v3.0.9 [#599](https://github.com/BU-ISCIII/relecov-tools/pull/598)

#### Changed

#### Removed

### Requirements

## [1.5.3] - 2025-05-14 : https://github.com/BU-ISCIII/relecov-tools/releases/tag/v1.5.3

### Credits

- [Alejandro Bernabeu](https://github.com/aberdur)
- [Pablo Mata](https://github.com/shettland)
- [Sarai Varona](https://github.com/svarona)

#### Added enhancements

- Included hex code in output filenames pipeline-utils scripts [#591](https://github.com/BU-ISCIII/relecov-tools/pull/591)
- Included hex code in pipeline-manager merged json filename [#591](https://github.com/BU-ISCIII/relecov-tools/pull/591)

#### Fixes

- Fixed multiqc software versions with new pattern [#593](https://github.com/BU-ISCIII/relecov-tools/pull/593)

#### Changed

- Update Metadata Template and Schema Ontology Mappings [#588](https://github.com/BU-ISCIII/relecov-tools/pull/588)
- Remove *dev from relecov_schema.json [#596](https://github.com/BU-ISCIII/relecov-tools/pull/596)

#### Removed

### Requirements

## [1.5.2] - 2025-05-13 : https://github.com/BU-ISCIII/relecov-tools/releases/tag/v1.5.2

### Credits

- [Alejandro Bernabeu](https://github.com/aberdur)
- [Pablo Mata](https://github.com/shettland)
- [Sarai Varona](https://github.com/svarona)

#### Added enhancements

#### Fixes

- Fixed level being set by default to debug instead of level provided in CLI [#584](https://github.com/BU-ISCIII/relecov-tools/pull/584)
- Fixed wrong output dir when --log-path was set via CLI in BaseModule [#584](https://github.com/BU-ISCIII/relecov-tools/pull/584)
- Add irma config and fixed read bioinfo metadata [#578](https://github.com/BU-ISCIII/relecov-tools/pull/578)

#### Changed

- Update relecov_schema.json and metadata template to v3.0.5 [#576](https://github.com/BU-ISCIII/relecov-tools/pull/576)
- Update relecov_schema.json and metadata templates to v3.0.6 [#579](https://github.com/BU-ISCIII/relecov-tools/pull/579)
- Changed zip passlog from debug to info in upload-results [#581](https://github.com/BU-ISCIII/relecov-tools/pull/581)
- Update relecov_schema.json to v3.0.7 [#585](https://github.com/BU-ISCIII/relecov-tools/pull/585)

#### Removed

### Requirements

## [1.5.1] - 2025-05-12 : https://github.com/BU-ISCIII/relecov-tools/releases/tag/v1.5.1

### Credits

- [Pablo Mata](https://github.com/shettland)
- [Alejandro Bernabeu](https://github.com/aberdur)
- [Jaime Ozáez](https://github.com/jaimeozaez)
- [Sara Monzon](https://github.com/saramonzon)
- [Sergio Olmos](https://github.com/OPSergio)
- [Sarai Varona](https://github.com/svarona)
- [Daniel Valle-Millares](https://github.com/Daniel-VM)

#### Added enhancements

- Correctly implemented new log handling in read-bioinfo-metadata [#546](https://github.com/BU-ISCIII/relecov-tools/pull/546)
- Implemented new BaseModule logging functionality in pipeline-manager [#549](https://github.com/BU-ISCIII/relecov-tools/pull/549)
- Included installation with bioconda in README.md [#549](https://github.com/BU-ISCIII/relecov-tools/pull/549)
- Added support for additional notes via .txt file or manual input in `send-email` CLI [#548](https://github.com/BU-ISCIII/relecov-tools/pull/548)
- Restructured and cleaned Jinja templates and ENA templates; moved to assets/mail_templates/ | assets/ena_templates and renamed for clarity [#548](https://github.com/BU-ISCIII/relecov-tools/pull/548)
- Set default folder as RELECOV when running wrapper module [#562](https://github.com/BU-ISCIII/relecov-tools/pull/562)
- Implement unique sample's ID generation using centralized registry [#565](https://github.com/BU-ISCIII/relecov-tools/pull/565).
- Created a new method in BaseModule to extract batch_id from metadata json files so modules can use it during processing [#569](https://github.com/BU-ISCIII/relecov-tools/pull/569)
- Implemented BaseModule standard logging functionality in all the modules that did not have it [#569](https://github.com/BU-ISCIII/relecov-tools/pull/569)

#### Fixes

- Fixed paired and single-end files validation [#537](https://github.com/BU-ISCIII/relecov-tools/pull/537)
- Suppressed unrelated warning when extra-config is not set [#543](https://github.com/BU-ISCIII/relecov-tools/pull/543)
- Add identifier fields to relecov_schema.json [#544](https://github.com/BU-ISCIII/relecov-tools/pull/544)
- Fixed deprecated sequence_file_path_R1 field in read-bioinfo-metadata [#546](https://github.com/BU-ISCIII/relecov-tools/pull/546)
- Fix warning when perLDM is "Data Not Evaluable" [#550](https://github.com/BU-ISCIII/relecov-tools/pull/550)
- Fixed unexpected warning message when organism field is empty [#553](https://github.com/BU-ISCIII/relecov-tools/pull/553)
- Modified warning message when validating invalid date format [#557](https://github.com/BU-ISCIII/relecov-tools/pull/557)
- Fixed wrapper and download subfolder handling and cleaning in remote sftp. [#561](https://github.com/BU-ISCIII/relecov-tools/pull/561)
- Removed noisy temporal remote folders from download_log_summary.json [#561](https://github.com/BU-ISCIII/relecov-tools/pull/561)
- Added institutions missing in SRI database to config json [#554](https://github.com/BU-ISCIII/relecov-tools/pull/554)
- Fixed anatomical material collection config file to match new enums in json_Schema [#567](https://github.com/BU-ISCIII/relecov-tools/pull/567)
- Fix handling of "Not Provided" in configuration.json [#570](https://github.com/BU-ISCIII/relecov-tools/pull/570)
- Hotfix for folder specific download log_summary filename [#571](https://github.com/BU-ISCIII/relecov-tools/pull/571)
- Fix return values and configuration handling in upload-results module [#573](https://github.com/BU-ISCIII/relecov-tools/pull/573)

#### Changed

- Update build-schema "," enums splitting to ";" splitting [#550](https://github.com/BU-ISCIII/relecov-tools/pull/550)
- Update relecov_schema.json [#550](https://github.com/BU-ISCIII/relecov-tools/pull/550)
- Improve json to excel generation to admit excels with more than one lab [#552](https://github.com/BU-ISCIII/relecov-tools/pull/552)
- Made --template_path optional in send-email and upload-results commands, using fallback to config key delivery_template_path_file [#548](https://github.com/BU-ISCIII/relecov-tools/pull/548)
- The configuration necessary to use the mail module is incorporated as an extra-config. [#548](https://github.com/BU-ISCIII/relecov-tools/pull/548)- Add support for qc_failed logic in viralrecon.py and update PR template [#559](https://github.com/BU-ISCIII/relecov-tools/pull/559)
- Better handling if new_key already exists in log_summary.py-rename_log_key(). [#561](https://github.com/BU-ISCIII/relecov-tools/pull/561)
- Update relecov_schema.json and Relecov_template*.xlsx to v3.0.3 [#563](https://github.com/BU-ISCIII/relecov-tools/pull/563)
- Truncated excessively long error and warning messages in the validation summary output. This affects only the summary section; full messages are still stored in individual sample logs. ([#568](https://github.com/BU-ISCIII/relecov-tools/pull/568))

#### Removed

### Requirements

## [1.5.0] - 2025-05-06 : https://github.com/BU-ISCIII/relecov-tools/releases/tag/v1.5.0

### Credits

Code contributions to the release:

- [Pablo Mata](https://github.com/shettland)
- [Alejandro Bernabeu](https://github.com/aberdur)
- [Daniel Valle-Millares](https://github.com/Daniel-VM)
- [Victor Lopez](https://github.com/victor5lm)
- [Jaime Ozáez](https://github.com/jaimeozaez)
- [Juan Ledesma](https://github.com/juanledesma78)
- [Sergio Olmos](https://github.com/OPSergio)
- [Sara Monzon](https://github.com/saramonzon)
- [Sarai Varona](https://github.com/svarona)

### Modules

- Introduced BaseModule as parent class for all other classes. Used to to handle logs. [#466](https://github.com/BU-ISCIII/relecov-tools/pull/466)
- New module add-extra-config for additional custom config [#464](https://github.com/BU-ISCIII/relecov-tools/pull/464)
- Created new `upload-results` module for uploading analysis_results folder back to every COD folder in sftp. [#433](https://github.com/BU-ISCIII/relecov-tools/pull/433)

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
- Update relecov_schema.json to 3.0.0dev version [#435](https://github.com/BU-ISCIII/relecov-tools/pull/435)
- Fix viralrecon_filepaths Path in read-bioinfo-metadata Module [#438](https://github.com/BU-ISCIII/relecov-tools/pull/438)
- Fix adding_ontology_to_enum when enum has no ontology [#439](https://github.com/BU-ISCIII/relecov-tools/pull/439)
- Update relecov_schema.json and read-bioinfo-metadata to v3.0.0 [#442](https://github.com/BU-ISCIII/relecov-tools/pull/442)
- Now logs-to-excel also creates a Json file with merged logs [#445](https://github.com/BU-ISCIII/relecov-tools/pull/445)
- Fix read-bioinfo-metadata: Evaluate qc_test at Batch Level and Handle Non-Evaluable %LDMutations [#447](https://github.com/BU-ISCIII/relecov-tools/pull/447)
- Enhance Schema Modularity, Formatting, and Conditional Validation Across Metadata Modules [#448](https://github.com/BU-ISCIII/relecov-tools/pull/448)
- Add --update Flag to Allow Metadata Update in read-bioinfo-metadata Module [#451](https://github.com/BU-ISCIII/relecov-tools/pull/451)
- Modified create_summary_tables.py to add new columns in epidemiological_data.xlsh (Pangolin software and database version and analysis date) [#454](https://github.com/BU-ISCIII/relecov-tools/pull/454)
- Improve Warning Handling for .gz Files and variants_long_table.csv in read-bioinfo-metadata [#457](https://github.com/BU-ISCIII/relecov-tools/pull/457)
- Add Tracking Summary of Samples per COD and Destination Folder in pipeline-manager [#463](https://github.com/BU-ISCIII/relecov-tools/pull/463)
- Improves the formatting of the Global Report sheet in the summary Excel by truncating overly long warnings and errors individually [#474](https://github.com/BU-ISCIII/relecov-tools/pull/474)
- Updated bioinfo_config.json to add the analysis date to the lineage_analysis_date in the bioinfo-lab-metadata json file. This analysis date is also added as the last column of the pangolin .csv files [#504](https://github.com/BU-ISCIII/relecov-tools/pull/504).
- Improve error handling and messages in logs-to-excel module [507#](https://github.com/BU-ISCIII/relecov-tools/pull/507)
- Add extra template handling in pipeline-manager [#511](https://github.com/BU-ISCIII/relecov-tools/pull/511).
- Added summary stats logging to wrapper module [514#](https://github.com/BU-ISCIII/relecov-tools/pull/514).
- Updated the create_summary_tables.py script to be able to handle single .json files directly [#526](https://github.com/BU-ISCIII/relecov-tools/pull/526).
- Updated README.md for new functionalities in v1.5.0 [#527](https://github.com/BU-ISCIII/relecov-tools/pull/527)

#### Fixes

- Fixed configuration to include mandatory values needed when interacting with Relecov API [#436](https://github.com/BU-ISCIII/relecov-tools/pull/436)
- Fix linting when Pull Request is closed [#404](https://github.com/BU-ISCIII/relecov-tools/pull/404)
- Fix removal of samples with repeated sampleID and .fastq files [#413](https://github.com/BU-ISCIII/relecov-tools/pull/413)
- Fix renaming of folders withou any valid sample [#413](https://github.com/BU-ISCIII/relecov-tools/pull/413)
- Fix download module when deleting corrupted pair-end data [#419](https://github.com/BU-ISCIII/relecov-tools/pull/419)
- Fixed the upload_resultwrapperexceptions properly [#441](https://github.com/BU-ISCIII/relecov-tools/pull/441)
- Update-db now converts data to string prior to API request to avoid crashing [#455](https://github.com/BU-ISCIII/relecov-tools/pull/455)
- Fixed recursive generation of build/lib/build/lib from pyproject.toml [#455](https://github.com/BU-ISCIII/relecov-tools/pull/455)
- Make QC evaluation project-dependent + schema upgrade and CI improvements [#467](https://github.com/BU-ISCIII/relecov-tools/pull/467)
- Fixed datetime generation, avoiding the omission of number zero in case of hours of a single digit [#472](https://github.com/BU-ISCIII/relecov-tools/pull/472)
- Temporal hotfix for invalid folders in remote sftp [#482](https://github.com/BU-ISCIII/relecov-tools/pull/482)
- Fix subfolder handling in wrapper module [#484](https://github.com/BU-ISCIII/relecov-tools/pull/484)
- Reverted temporal hotfix changes for invalid sftp folders [#486](https://github.com/BU-ISCIII/relecov-tools/pull/486)
- Fix metadata upload in dataprocess_wrapper and update schema assets to v3.0.0 [#488](https://github.com/BU-ISCIII/relecov-tools/pull/488)
- Fixed permissions error when redirecting logs between different machines [#489](https://github.com/BU-ISCIII/relecov-tools/pull/489)
- Fix generation and upload of invalid_*.xlsx with headers [#494](https://github.com/BU-ISCIII/relecov-tools/pull/494)
- Included correct handling of CLI --log-path arg fixing #490 [#497](https://github.com/BU-ISCIII/relecov-tools/pull/497)
- Update relecov_schema.json and fastq properties in relecov-tools modules [#501](https://github.com/BU-ISCIII/relecov-tools/pull/501)
- Fix header order in configuration.json [#505](https://github.com/BU-ISCIII/relecov-tools/pull/505)
- Fix *fastq properties in pipeline_manager.py [#509](https://github.com/BU-ISCIII/relecov-tools/pull/509)
- Fixed overriding and wrong configuration in read-lab-metadata for submitting institution [#521](https://github.com/BU-ISCIII/relecov-tools/pull/521)
- Fixed unused keys in initial_config.yaml [#522](https://github.com/BU-ISCIII/relecov-tools/pull/522)

#### Changed

- Temporarily changed bioinfo_config 'quality_control' requirement to false [#379](https://github.com/BU-ISCIII/relecov-tools/pull/379)
- Improve and fix minor issues in build_schema.py [#404](https://github.com/BU-ISCIII/relecov-tools/pull/404)
- Changed utils.write_json_fo_file() name to write_json_to_file() [#412](https://github.com/BU-ISCIII/relecov-tools/pull/412)
- Changed pipeline-manager --template param to --templates_root, now points to root folder [#412](https://github.com/BU-ISCIII/relecov-tools/pull/412)
- Changed property name for consistency with the rest of the properties. [#480](https://github.com/BU-ISCIII/relecov-tools/pull/480)
- Renamed CLI argument --json_schema to --json_schema_file in validate command for consistency with internal function arguments and YAML configuration. [#503](https://github.com/BU-ISCIII/relecov-tools/pull/503)

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
