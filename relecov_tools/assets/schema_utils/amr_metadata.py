#!/usr/bin/env python
import json
import os

import pandas as pd

SOURCE_SHEET = "amr_gene_list"
OUTPUT_FILENAME = "amr_genes.json"


def load_amr_gene_list(excel_file_path: str) -> pd.DataFrame:
    """
    Read the optional amr_gene_list sheet from the input Excel file.

    Args:
        excel_file_path (str): Path to the schema definition Excel file.

    Returns:
        pd.DataFrame: AMR gene list data, or an empty DataFrame if the sheet is absent.
    """
    try:
        df = pd.read_excel(
            excel_file_path,
            sheet_name=SOURCE_SHEET,
            na_values=["nan", "N/A", "NA", ""],
        )
    except ValueError:
        return pd.DataFrame()

    if not df.empty:
        df.columns = [str(column).strip() for column in df.columns]
        if "Name" in df.columns:
            df = df[df["Name"].notna()]

    return df


def clean_amr_value(value) -> str:
    """
    Clean an AMR metadata value for JSON serialization.

    Args:
        value: Raw value from the amr_gene_list sheet.

    Returns:
        str: Stripped string value, or an empty string for NA values.
    """
    return "" if pd.isna(value) else str(value).strip()


def build_amr_genes_json(df: pd.DataFrame, version: str) -> dict:
    """
    Build the amr_genes.json content from an amr_gene_list dataframe.

    The output keeps genes and alleles keyed by their formatted Name value. Alleles
    also include the corresponding gene when gen_for_alleles matches a gene
    Name_CARD value in the same source sheet.

    Args:
        df (pd.DataFrame): Data from the amr_gene_list sheet.
        version (str): Schema version to include in the output JSON.

    Returns:
        dict: JSON-serializable AMR metadata, or an empty dict if there is no data.
    """
    if df.empty:
        return {}

    if "Category" in df.columns:
        gene_rows = df[df["Category"].astype(str).str.strip().str.lower() == "gene"]
    else:
        gene_rows = pd.DataFrame()

    gene_name_by_card = {
        clean_amr_value(row.get("Name_CARD")): clean_amr_value(row.get("Name"))
        for _, row in gene_rows.iterrows()
        if clean_amr_value(row.get("Name_CARD")) and clean_amr_value(row.get("Name"))
    }

    terms = {}
    for _, row in df.iterrows():
        name = clean_amr_value(row.get("Name"))
        if not name:
            continue

        category = clean_amr_value(row.get("Category")).lower()
        name_card = clean_amr_value(row.get("Name_CARD"))
        gene_for_alleles = clean_amr_value(row.get("gen_for_alleles"))

        term = {
            "name_card": name_card,
            "aro_accession": clean_amr_value(row.get("ARO Accession")),
            "name": name,
            "category": category,
            "classification": clean_amr_value(row.get("Classification")),
            "description": clean_amr_value(row.get("Description")),
        }

        if category == "allele":
            term["allele_name"] = name
            term["gene_name_card"] = gene_for_alleles
            term["gene_name"] = gene_name_by_card.get(
                gene_for_alleles, gene_for_alleles
            )
        elif category == "gene":
            term["gene_name_card"] = name_card
            term["gene_name"] = name

        terms[name] = term

    return {
        "version": version,
        "source_sheet": SOURCE_SHEET,
        "terms": terms,
    }


def save_amr_genes_json(
    excel_file_path: str, output_dir: str, version: str
) -> str | None:
    """
    Build and save amr_genes.json from the optional amr_gene_list sheet.

    Args:
        excel_file_path (str): Path to the schema definition Excel file.
        output_dir (str): Directory where amr_genes.json should be written.
        version (str): Schema version to include in the output JSON.

    Returns:
        str | None: Output path when the file is written, otherwise None.
    """
    df = load_amr_gene_list(excel_file_path)
    amr_genes = build_amr_genes_json(df, version)
    if not amr_genes:
        return None

    output_path = os.path.join(output_dir, OUTPUT_FILENAME)
    with open(output_path, "w", encoding="utf-8") as fh:
        fh.write(json.dumps(amr_genes, indent=4, ensure_ascii=False))

    return output_path
