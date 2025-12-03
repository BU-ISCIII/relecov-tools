#!/usr/bin/env python
import argparse
import sys
from pathlib import Path

from relecov_tools.read_lab_metadata import LabMetadata


def parse_args():
    parser = argparse.ArgumentParser(
        description="Smoke-test read_lab_metadata for the MePRAM project."
    )
    data_dir = Path(__file__).resolve().parent / "data" / "read_lab_metadata"
    parser.add_argument(
        "-m",
        "--metadata_file",
        default=str(data_dir / "mepram_metadata_lab_test.xlsx"),
        help="Metadata Excel file (defaults to the bundled MePRAM template).",
    )
    parser.add_argument(
        "-s",
        "--sample_list_file",
        default=str(data_dir / "mepram_samples_data_test.json"),
        help="samples_data.json file to enrich metadata (defaults to the bundled test file).",
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        default=str(data_dir / "metadata_out"),
        help="Folder where temporary results will be written.",
    )
    parser.add_argument(
        "-p",
        "--project",
        default="mepram",
        help="Project key defined under read_lab_metadata.projects (default: mepram).",
    )
    return parser.parse_args()


def validate_nested_fields(rows):
    if not rows:
        raise AssertionError("Expected metadata rows to be parsed")

    first_row = rows[0]
    if first_row["sequencing_sample_id"] != "sample_good":
        raise AssertionError("Unexpected sequencing_sample_id in first row")

    organism = first_row.get("organism")
    if not (
        isinstance(organism, list) and organism[0]["species"] == "Klebsiella pneumoniae"
    ):
        raise AssertionError("Organism entry not parsed as expected")

    typing = first_row.get("typing")
    if not (isinstance(typing, list) and typing[0]["analysis_type"] == "o-locus"):
        raise AssertionError("Typing array not parsed as expected")
    if typing[0]["value"] != "O6":
        raise AssertionError("Typing value mismatch")

    amr_detection = first_row.get("amr_detection")
    if not (
        isinstance(amr_detection, list)
        and amr_detection[0]["amr_detection_method"].lower() == "sanger"
    ):
        raise AssertionError("AMR detection block not parsed as expected")

    mic = first_row.get("MIC")
    if not (isinstance(mic, list) and mic[0]["MIC_value"] == "<=0.03"):
        raise AssertionError("MIC array not parsed as expected")


def run_mepram_smoke_test(metadata_file, sample_list_file, output_dir, project):
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    reader = LabMetadata(
        metadata_file=str(metadata_file),
        sample_list_file=str(sample_list_file),
        output_dir=str(out_dir),
        project=project,
    )
    rows = reader.read_metadata_file()
    validate_nested_fields(rows)


def main():
    args = parse_args()
    try:
        run_mepram_smoke_test(
            metadata_file=args.metadata_file,
            sample_list_file=args.sample_list_file,
            output_dir=args.output_dir,
            project=args.project,
        )
        print("read_lab_metadata MePRAM smoke test finished successfully.")
    except AssertionError as error:
        print(f"Smoke test failed: {error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
