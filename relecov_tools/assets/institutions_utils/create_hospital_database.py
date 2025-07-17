#!/usr/bin/env python

import sys
import re
import pandas as pd
import numpy as np
import json
import argparse
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut


def parse_args(args=None):
    Description = "Merge hospital databases with CCN and CODCNH into a unique database and a .json file"
    Epilog = """Example usage: python create_hospital_database.py -r DOCUMENTOS_ORIGINALES_2024/Metadatos_CARLOS_III_20250714.xlsx -c DOCUMENTOS_ORIGINALES_2024/CNH_2024_12122024.xlsx -o hospitals_ddbb.xlsx -a non_ddbb_adress.json -j regcess_laboratory_address.json -d differences.json -m missing.json -p laboratory_address.json"""
    parser = argparse.ArgumentParser(description=Description, epilog=Epilog)
    parser.add_argument(
        "-r",
        "--regcess",
        default="Metadatos_CARLOS_III_20250711.xlsx",
        help="Path to REGCESS file with the CCN code",
    )
    parser.add_argument(
        "-c",
        "--cnh",
        default="CNH_2024.xlsx",
        help="Path to ministry file of national hospital catalogue with the CODCNH code",
    )
    parser.add_argument(
        "-o",
        "--out_excel",
        default="hospitals_ddbb.xlsx",
        help="Output excel file path.",
    )
    parser.add_argument(
        "-p",
        "--previous_json",
        help="Previous .json file path",
        default=False
    )
    parser.add_argument(
        "-a",
        "--additional_json",
        default=False,
        help="Additional .json file with hospitals not included in the database.",
    )
    parser.add_argument(
        "-j",
        "--output_json",
        default="laboratory_address.json",
        help="Output .json filename",
    )
    parser.add_argument(
        "-d",
        "--differences_json",
        default="diferences.json",
        help="Json file with the keys that changed between the new and the previous dictionary",
    )
    parser.add_argument(
        "-m",
        "--missing_json",
        default="missing.json",
        help="Json file with the hospitals that were removed from the previous dictionary",
    )
    parser.add_argument(
        "-g",
        "--geo_loc_cities",
        default="geo_loc_cities.json",
        help="Json file with latitude and longitude of cities",
    )
    return parser.parse_args(args)


def process_regcess_table(file):
    """Load and clean the REGCESS Excel sheet into a standardized DataFrame.

    Steps:
        1. Read "Metadatos" sheet as DataFrame (CCN, Cód. Centro Autonómico, etc. as str).
        2. Ensure "Cód. Clase de Centro" column exists (fill NaN if missing).
        3. Transform cases: Title Case: Nombre Centro & Dependencia Funcional; lowercase: Email.
        4. Build "Dirección" by concatenating Tipo Vía, Nombre Vía, Número Vía.
        5. Split "Coordenadas" into Latitud and Longitud columns.
        6. Clean commas and whitespace in text columns.
        7. Normalize inverted names ("Coruña, A" → "A Coruña").
        8. Return cleaned DataFrame.

    Args:
        file_path (str): path to the REGCESS Excel file

    Returns:
        regcess_db (pd.DataFrame): cleaned REGCESS table
    """

    # 1. Load with proper dtypes
    regcess_db = pd.read_excel(file, sheet_name="Metadatos", dtype={"CCN": str, "Cód. Centro Autonómico": str, "Código Postal": str, "Teléfono Principal": str})

    # 2. Ensure missing column
    if "Cód. Clase de Centro" not in regcess_db.columns:
        regcess_db["Cód. Clase de Centro"] = np.nan

    # 3. Transform case
    regcess_db["Nombre Centro"] = regcess_db["Nombre Centro"].str.title()
    regcess_db["Dependencia Funcional"] = regcess_db["Dependencia Funcional"].str.title()
    regcess_db["Email"] = regcess_db["Email"].str.lower()
    regcess_db["Email"] = regcess_db["Email"].fillna("Desconocido")

    # 4. Construct Dirección
    regcess_db["Dirección"] = (
        regcess_db["Tipo Vía"].str.title().str.strip() + " " +
        regcess_db["Nombre Vía"].str.title().str.strip() + ", " +
        regcess_db["Número Vía"].astype(str).str.strip()
    )

    # 5. Split Latitude and Longitude in two different columns
    regcess_db['Coordenadas'] = regcess_db['Coordenadas'].astype(str)
    regcess_db[['Latitud', 'Longitud']] = regcess_db['Coordenadas'].str.split(',', expand=True)

    # 6. Clean text columns (remove double witespaces and double commas)
    regcess_db["Nombre Centro"] = regcess_db["Nombre Centro"].apply(lambda x: re.sub(r'\s+', ' ', x) if isinstance(x, str) else x)
    regcess_db["Dependencia Funcional"] = regcess_db["Dependencia Funcional"].apply(lambda x: re.sub(r'\s+', ' ', x) if isinstance(x, str) else x)
    regcess_db["Dirección"] = regcess_db["Dirección"].apply(lambda x: re.sub(r",,+", ",", x) if isinstance(x, str) else x)
    regcess_db["Dirección"] = regcess_db["Dirección"].apply(lambda x: re.sub(r",\s*,", ",", x) if isinstance(x, str) else x)
    regcess_db["Dirección"] = regcess_db["Dirección"].apply(lambda x: re.sub(r'\s+', ' ', x) if isinstance(x, str) else x)

    # 7. Normalize names
    regcess_db["Municipio"] = regcess_db["Municipio"].apply(normalize_names)
    regcess_db["Municipio"] = regcess_db["Municipio"].str.title()
    regcess_db["CCAA"] = regcess_db["CCAA"].apply(normalize_names)
    regcess_db["Provincia"] = regcess_db["Provincia"].apply(normalize_names)

    return regcess_db


def process_cnh_table(file):
    """Load and clean the CNH hospital catalogue sheet.
    Args:
        file_path (str): path to CNH Excel file
    Returns:
        cnh_ddbb (pd.DataFrame): cleaned CNH DataFrame
    """
    cnh_ddbb = pd.read_excel(file, sheet_name="DIRECTORIO DE HOSPITALES", dtype={"CCN": str, "CODCNH": str, "CODIDCOM": str, "Cód. Municipio": str, "Cód. Provincia": str, "Cód. CCAA": str})
    cnh_ddbb["Nombre del Complejo"] = cnh_ddbb["Nombre del Complejo"].str.title()

    return cnh_ddbb


def process_complex(cnh_ddbb, regcess_db):
    """Extract complex hospitals, merge data, and standardize for JSON creation.

    Steps:
        1. Filter CNH entries with 'Forma parte Complejo' == 'S'.
        2. Select relevant columns for complexes.
        3. Merge with regcess_db on 'CCN', inner join.
        4. Drop duplicates by 'CODIDCOM', keep first.
        5. Prefix new CCN with 'COMP_' + CODIDCOM.
        6. Overwrite/hide fields not applicable to complexes:
           - Set 'Cód. Centro Autonómico', 'Dependencia Funcional', 'Cód. Clase de Centro' to 'N/A'
           - Set 'Clase de Centro' to 'Complejo'
        7. Drop 'CODIDCOM' column, rename 'Nombre del Complejo' to 'Nombre Centro'.
        8. Return the processed DataFrame.

    Args:
        cnh_ddbb (pd.DataFrame): CNH DataFrame
        regcess_db (pd.DataFrame): REGCESS DataFrame

    Returns:
        complex_hospitals (pd.DataFrame): DataFrame of complex hospitals
    """
    complex_hospitals = cnh_ddbb[cnh_ddbb['Forma parte Complejo'] == 'S']
    complex_hospitals = complex_hospitals[["CCN", "CODCNH", "Nombre del Complejo", "CODIDCOM", "Cód. Municipio", "Cód. Provincia", 'Cód. CCAA']]
    complex_hospitals = pd.merge(complex_hospitals, regcess_db, on=['CCN'], how="inner", suffixes=('_compl', '_regcess'))

    # Remove hospital name from REGCESS
    complex_hospitals.drop(columns=["Nombre Centro"], inplace=True)
    complex_hospitals["CODCNH"] = complex_hospitals["CODIDCOM"]

    # keep only last hospital for information (ESRI one of the hospitals, randomly)
    complex_hospitals = complex_hospitals.drop_duplicates(subset='CODIDCOM', keep='first')

    # Remove hospital info not aplicable to complex
    # Replace CCN which is assoaciated with hospital and not the complex, with a combination of COMP_ + the complex code CODIDCOM
    complex_hospitals["CCN"] = "COMP_" + complex_hospitals["CODIDCOM"]
    # Remove data from the hospital
    complex_hospitals[["Cód. Centro Autonómico", "Dependencia Funcional", "Cód. Clase de Centro"]] = "N/A"
    # Replaced "Clase de Centro" with "Complejo" instead of the hospital's values.
    complex_hospitals[["Clase de Centro"]] = "Complejo"

    # Fix column names and numbers to fit in the hospital table names
    complex_hospitals = complex_hospitals.drop(columns=["CODIDCOM"]).rename(columns={"Nombre del Complejo": "Nombre Centro"})

    return complex_hospitals


def normalize_names(name):
    """Swap inverted names around a comma and normalize slash-separated parts.
    Args:
        name (str): raw location string, e.g. "Coruña, A" or "Vila Joiosa, la/Villajoyosa"
    Returns:
        str: normalized name, e.g. "A Coruña" or "la Vila Joiosa/Villajoyosa"
    """

    if "," in name:
        if "/" not in name:
            first, second = [p.strip() for p in name.split(",", 1)]
            return f"{second} {first}"
        else:
            parts = name.split("/")
            normalized = []
            for part in parts:
                part = part.strip()
                if "," in part:
                    first, second = [p.strip() for p in part.split(",", 1)]
                    normalized.append(f"{second} {first}")
                else:
                    normalized.append(part)
        return "/".join(normalized)
    else:
        return name


def create_json(hospitals):
    """Convert hospital DataFrame to a JSON dict.
    Args:
        hospitals_df (pd.DataFrame): DataFrame with hospital records

    Returns:
        hospitals_json (dict): hospital info dict
    """
    hospitals_json = {}

    for _, row in hospitals.iterrows():

        if not pd.isna(row["Cód. Clase de Centro"]):
            center_class_code = row["Cód. Clase de Centro"].strip()
        else:
            class_name = row["Clase de Centro"].strip()
            center_class_code = center_class_map(class_name)

        ccn_hospital = row["CCN"]
        hospitals_json[ccn_hospital] = {
            "codcnh": row["CODCNH"],
            "collecting_institution": row["Nombre Centro"].strip(),
            "collecting_institution_address": row["Dirección"],
            "collecting_institution_email": (
                row["Email"].strip() if not pd.isna(row["Email"]) else "Desconocido"
            ),
            "autonom_cod": row["Cód. Centro Autonómico"],
            "geo_loc_state": row["CCAA"].strip(),
            "geo_loc_state_cod": row["Cód. CCAA"],
            "geo_loc_region": row["Provincia"].strip(),
            "geo_loc_region_cod": row["Cód. Provincia"],
            "geo_loc_city": row["Municipio"].strip(),
            "geo_loc_city_cod": row["Cód. Municipio"],
            "post_code": row["Código Postal"],
            "dep_func": row["Dependencia Funcional"].strip(),
            "center_class_code": center_class_code,
            "collecting_institution_finalidad": row["Clase de Centro"].strip(),
            "lab_geo_loc_latitude": row["Latitud"],
            "lab_geo_loc_longitude": row["Longitud"],
            "collecting_institution_phone": row["Teléfono Principal"],
            "geo_loc_country": "Spain",
            "submitting_institution": "",
            "submitting_institution_address": "",
            "submitting_institution_email": ""
        }

    return hospitals_json


def center_class_map(class_name):
    """Map human-readable center class name to its code.
    Args:
        class_name (str): e.g. "Centros de Diagnostico"
    Returns:
        center_class_code (str): code like "C256"
    """
    class_map = {
        "Hospitales Generales": "C11",
        "Hospitales especializados": "C12",
        "Hospitales de media y larga estancia": "C13",
        "Hospitales de salud mental y tratamiento de toxicomanías": "C14",
        "Otros Centros con Internamiento": "C190",
        "Consultas Médicas": "C21",
        "Centros de salud": "C231",
        "Consultorios de atencion primaria": "C232",
        "Centros de Diagnostico": "C256",
        "Centros moviles de asistencia sanitaria": "C257",
    }
    center_class_code = class_map.get(class_name)
    return center_class_code


def add_hospitals(hospital_ddbb_json, regcess_db, add_json):
    """Merge additional hospital entries into existing JSON, tracking differences, and updates additional hospitals with REGCESS information.
    Args:
        hospital_json (dict): existing hospital dict
        regcess_db (pd.DataFrame): REGCESS DataFrame
        add_json (dict): new hospital entries
    Returns:
        tuple: (updated_hospital_json, updated_add_json, differences)
          - hospital_ddbb_json: JSON dict with the merged list of hospitals
          - add_json: JSON dict wih the additional hospitals updated with REGCESS
          - differences: JSON dict with the differences found between add_json and regcess_db
    """

    differences = {}

    for ccn, data in add_json.items():
        if ccn in regcess_db["CCN"].values:
            row = regcess_db[regcess_db["CCN"] == ccn].iloc[0]
            if not pd.isna(row["Cód. Clase de Centro"]):
                center_class_code = row["Cód. Clase de Centro"].strip()
            else:
                class_name = row["Clase de Centro"].strip()
                center_class_code = center_class_map(class_name)

            if row["CCAA"].strip() == data.get("geo_loc_state"):
                geo_loc_state_cod = data.get("geo_loc_state_cod")
            else:
                geo_loc_state_cod = "ADD"
            if row["Provincia"].strip() == data.get("geo_loc_region"):
                geo_loc_region_cod = data.get("geo_loc_region_cod")
            else:
                geo_loc_region_cod = "ADD"
            if row["Municipio"].strip() == data.get("geo_loc_city"):
                geo_loc_city_cod = data.get("geo_loc_city_cod")
            else:
                geo_loc_city_cod = "ADD"

            new_data = {
                "codcnh": data.get("codcnh"),
                "collecting_institution": row["Nombre Centro"].strip(),
                "collecting_institution_address": row["Dirección"],
                "collecting_institution_email": (
                    row["Email"].strip() if not pd.isna(row.get("Email")) else "Desconocido"
                ),
                "autonom_cod": row["Cód. Centro Autonómico"],
                "geo_loc_state": row["CCAA"].strip(),
                "geo_loc_state_cod": geo_loc_state_cod,
                "geo_loc_region": row["Provincia"].strip(),
                "geo_loc_region_cod": geo_loc_region_cod,
                "geo_loc_city": row["Municipio"].strip(),
                "geo_loc_city_cod": geo_loc_city_cod,
                "post_code": row["Código Postal"],
                "dep_func": row["Dependencia Funcional"].strip(),
                "center_class_code": center_class_code,
                "collecting_institution_finalidad": row["Clase de Centro"].strip(),
                "lab_geo_loc_latitude": row["Latitud"],
                "lab_geo_loc_longitude": row["Longitud"],
                "collecting_institution_phone": row["Teléfono Principal"],
                "geo_loc_country": "Spain",
                "submitting_institution": data.get("submitting_institution"),
                "submitting_institution_address": data.get("submitting_institution_address"),
                "submitting_institution_email": data.get("submitting_institution_email")
            }

            for field, new_val in new_data.items():
                old_val = data.get(field, "").strip() if isinstance(data.get(field), str) else data.get(field)
                if old_val != new_val:
                    if field not in differences:
                        differences[field] = {}
                    differences[field].update({old_val: new_val})

            add_json[ccn] = new_data
            hospital_ddbb_json[ccn] = new_data

        else:
            hospital_ddbb_json[ccn] = data

    return hospital_ddbb_json, add_json, differences


def compare_json(prev_json_path, new_hospitals, differences):
    """Compare previous JSON with new entries, find missing and changed fields.
    Args:
        prev_json_path (str): path to previous JSON file
        new_hospitals (dict): updated hospital dict
        differences (dict): existing differences tracker
    Returns:
        tuple: (missing_json, differences)
          - missing_json: JSON dict of hospitals that were removed
          - differences: JSON dict with the differences found between the old and the new hospital list
    """
    missing_json = {}

    with open(prev_json_path, "r", encoding="utf-8") as json_file:
        previous_json = json.load(json_file)

    for prev_ccn, prev_data in previous_json.items():
        data_found = False
        for ccn, data in new_hospitals.items():
            if ccn == prev_ccn:
                data_found = True
                for key in ["submitting_institution", "submitting_institution_address", "submitting_institution_email"]:
                    if key in prev_data:
                        data[key] = prev_data[key]

                for field in data:
                    if field in prev_data and field not in {
                        "submitting_institution", "submitting_institution_address", "submitting_institution_email"
                    }:
                        if data[field] != prev_data[field]:
                            if field not in differences:
                                differences[field] = {}
                            differences[field].update({prev_data[field]: data[field]})
                break
        if not data_found:
            missing_json[prev_ccn] = prev_data

    return missing_json, differences

def write_json(output_json, data):
    """Write a dict to a JSON file with pretty formatting and message.
    Args:
        output_path (str): filename for JSON output
        data (dict): dictionary to print
    """
    with open(output_json, "w", encoding="utf-8") as json_file:
        json.dump(data, json_file, ensure_ascii=False, indent=4)

    print(f"Archivo JSON generado: {output_json}")


def create_cities_coord(hospital_json, geo_loc_file):
    """Ensure all cities have coordinates and build summary strings.
    Args:
        hospital_json (dict): hospital info dict
        geo_loc_file (str): path to JSON with existing coordinates
    """
    with open(geo_loc_file, "r", encoding="utf-8") as json_file:
        geo_loc_json = json.load(json_file)

    geo_loc_cities = set(geo_loc_json.keys())

    cities = []
    states = []
    regions = []
    hospitals = []
    submittings = []
    geolocator = Nominatim(user_agent="my_app")
    for ccn, data in hospital_json.items():
        loc = None
        city = data["geo_loc_city"]
        if city not in cities:
            cities.append(city)
        state = data["geo_loc_state"]
        if state not in states:
            states.append(state)
        region = data["geo_loc_region"]
        if region not in regions:
            regions.append(region)
        hospital = data["collecting_institution"]
        if hospital not in hospitals:
            hospitals.append(hospital)

        submitting = data["submitting_institution"]
        if submitting not in submittings:
            submittings.append(submitting)

        country = data["geo_loc_country"]
        if city not in geo_loc_cities:
            try:
                loc = geolocator.geocode(f"{city}, {state}, {country}", timeout=15)
            except GeocoderTimedOut:
                return None, None
            if loc:
                geo_loc_json[data["geo_loc_city"]] = {
                    "geo_loc_latitude": str(loc.latitude),
                    "geo_loc_longitude": str(loc.longitude)
                }
            else:
                print("City with coordenates not found")
                print(f"{city}, {state}, {country}")

    write_json(geo_loc_file, geo_loc_json)

    schema_data_json = {}
    schema_data_json["cities"] = '; '.join(cities)
    schema_data_json["states"] = '; '.join(states)
    schema_data_json["regions"] = '; '.join(regions)
    schema_data_json["hospitals"] = '; '.join(hospitals)

    write_json("schema_excel_strings.json", schema_data_json)

    return


def main(args=None):
    args = parse_args(args)

    # Load regcess table and transform data
    regcess_db = process_regcess_table(args.regcess)

    # Load ministry table and transform/merge data
    cnh_ddbb = process_cnh_table(args.cnh)

    # Remove columns that we are not using
    regcess_db.drop(columns=["Unnamed: 10", "Coordenadas", "Tipo Vía", "Nombre Vía", "Número Vía"], errors='ignore', inplace=True)

    cod_mapping = cnh_ddbb[["CCN", "CODCNH", "Cód. Municipio", "Cód. Provincia", 'Cód. CCAA']]

    # Merge both tables
    hospital_ddbb = pd.merge(regcess_db, cod_mapping, on=['CCN'], how="inner", suffixes=('_regcess', '_cnh'))

    # Process hospital complex
    complex_table = process_complex(cnh_ddbb, regcess_db)

    hospital_ddbb = pd.concat([hospital_ddbb, complex_table], ignore_index=True)

    hospital_ddbb_json = create_json(hospital_ddbb)

    if args.additional_json:
        with open(args.additional_json, "r", encoding="utf-8") as json_file:
            add_json = json.load(json_file)
        hospital_ddbb_json, add_json, differences = add_hospitals(hospital_ddbb_json, regcess_db, add_json)

    write_json(args.additional_json, add_json)

    hospital_ddbb = pd.DataFrame.from_dict(hospital_ddbb_json, orient='index')
    hospital_ddbb.reset_index(inplace=True)
    hospital_ddbb.rename(columns={'index': 'ccn'}, inplace=True)

    hospital_ddbb.to_excel(args.out_excel, sheet_name="hospitals", index=False)
    print(f"Archivo Excel generado: {args.out_excel}")

    if args.previous_json:
        missing_json, differences = compare_json(args.previous_json, hospital_ddbb_json, differences)
        if len(missing_json) != 0:
            write_json("missing_hospitals.json", missing_json)

    write_json(args.output_json, hospital_ddbb_json)

    if len(differences) != 0:
        write_json(args.differences_json, differences)

    create_cities_coord(hospital_ddbb_json, args.geo_loc_cities)

if __name__ == "__main__":
    sys.exit(main())
