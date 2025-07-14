#!/usr/bin/env python

import sys
import re
import pandas as pd
import numpy as np
import json
import argparse


def parse_args(args=None):
    Description = "Merge hospital databases with CCN and CODCNH into a unique database and a .json file"
    Epilog = """Example usage: python create_hospital_database.py -r LISTADO_HOSPITALES_REGCESS_04122024.xlsx -c CNH_2024.xlsx -o hospitals_ddbb.xlsx -a non_ddbb_adress.json"""
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
    return parser.parse_args(args)


def process_regcess_table(file):
    regcess_db = pd.read_excel(file, sheet_name="Metadatos", dtype={"CCN": str, "Cód. Centro Autonómico": str, "Código Postal": str, "Teléfono Principal": str})
    if "Cód. Clase de Centro" not in regcess_db.columns:
        regcess_db["Cód. Clase de Centro"] = np.nan

    regcess_db["Nombre Centro"] = regcess_db["Nombre Centro"].str.title()
    regcess_db["Dependencia Funcional"] = regcess_db["Dependencia Funcional"].str.title()
    regcess_db["Email"] = regcess_db["Email"].str.lower()
    regcess_db["Email"] = regcess_db["Email"].fillna("Desconocido")

    regcess_db["Dirección"] = (
        regcess_db["Tipo Vía"].str.title().str.strip() + " " +
        regcess_db["Nombre Vía"].str.title().str.strip() + ", " +
        regcess_db["Número Vía"].astype(str).str.strip()
    )

    # Split Latitude and Longitude in two different columns
    regcess_db['Coordenadas'] = regcess_db['Coordenadas'].astype(str)
    regcess_db[['Latitud', 'Longitud']] = regcess_db['Coordenadas'].str.split(',', expand=True)

    # Remove double spaces:
    regcess_db["Nombre Centro"] = regcess_db["Nombre Centro"].apply(lambda x: re.sub(r'\s+', ' ', x) if isinstance(x, str) else x)
    regcess_db["Dependencia Funcional"] = regcess_db["Dependencia Funcional"].apply(lambda x: re.sub(r'\s+', ' ', x) if isinstance(x, str) else x)
    regcess_db["Dirección"] = regcess_db["Dirección"].apply(lambda x: re.sub(r",,+", ",", x) if isinstance(x, str) else x)
    regcess_db["Dirección"] = regcess_db["Dirección"].apply(lambda x: re.sub(r",\s*,", ",", x) if isinstance(x, str) else x)
    regcess_db["Dirección"] = regcess_db["Dirección"].apply(lambda x: re.sub(r'\s+', ' ', x) if isinstance(x, str) else x)

    # Fix columns
    # Replace Coruña, A by A Coruña
    regcess_db["Municipio"] = regcess_db["Municipio"].apply(normalize_names)
    regcess_db["Municipio"] = regcess_db["Municipio"].str.title()
    regcess_db["CCAA"] = regcess_db["CCAA"].apply(normalize_names)
    regcess_db["Provincia"] = regcess_db["Provincia"].apply(normalize_names)
    return regcess_db


def normalize_names(name):
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


def process_cnh_table(file):
    # Load ministry tables
    cnh_ddbb = pd.read_excel(file, sheet_name="DIRECTORIO DE HOSPITALES", dtype={"CCN": str, "Cód. Municipio": str, "Cód. Provincia": str, "Cód. CCAA": str})
    cnh_ddbb["Nombre del Complejo"] = cnh_ddbb["Nombre del Complejo"].str.title()

    return cnh_ddbb


def process_complex(cnh_ddbb, regcess_db):
    complex_hospitals = cnh_ddbb[cnh_ddbb['Forma parte Complejo'] == 'S']
    complex_hospitals = complex_hospitals[["CCN", "CODCNH", "Nombre del Complejo", "CODIDCOM", "Cód. Municipio", "Cód. Provincia", 'Cód. CCAA']]
    complex_hospitals = pd.merge(complex_hospitals, regcess_db, on=['CCN'], how="inner", suffixes=('_compl', '_regcess'))

    # Remove hospital name from REGCESS
    complex_hospitals.drop(columns=["Nombre Centro"], inplace=True)
    complex_hospitals["CODCNH"] = complex_hospitals["CODIDCOM"].astype(int)

    # keep only last hospital for information (ESRI one of the hospitals, randomly)
    complex_hospitals = complex_hospitals.drop_duplicates(subset='CODIDCOM', keep='first')

    # Remove hospital info not aplicable to complex
    # Replace CCN which is assoaciated with hospital and not the complex, with a combination of COMP_ + the complex code CODIDCOM
    complex_hospitals["CCN"] = "COMP_" + complex_hospitals["CODIDCOM"].astype(int).astype(str)
    # Remove data from the hospital
    complex_hospitals[["Cód. Centro Autonómico", "Dependencia Funcional", "Cód. Clase de Centro"]] = "N/A"
    # Replaced "Clase de Centro" with "Complejo" instead of the hospital's values.
    complex_hospitals[["Clase de Centro"]] = "Complejo"

    # Fix column names and numbers to fit in the hospital table names
    complex_hospitals = complex_hospitals.drop(columns=["CODIDCOM"]).rename(columns={"Nombre del Complejo": "Nombre Centro"})

    return complex_hospitals


def create_json(hospitals):
    hospitals_json = {}

    for index, row in hospitals.iterrows():

        if not pd.isna(row["Cód. Clase de Centro"]):
            center_class_code = row["Cód. Clase de Centro"].strip()
        else:
            class_name = row["Clase de Centro"].strip()
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
            "center_class": row["Clase de Centro"].strip(),
            "geo_loc_latitude": row["Latitud"],
            "geo_loc_longitude": row["Longitud"],
            "phone": row["Teléfono Principal"],
            "geo_loc_country": "Spain",
        }

    return hospitals_json


def add_hospitals(hospital_ddbb_json, add_json):

    for ccn, data in add_json.items():
        if ccn in hospital_ddbb_json:
            print("Hospital with CCN: " + ccn)
            print("Already exist in DDBB. Ignoring it")
        else:
            hospital_ddbb_json[ccn] = data

    return hospital_ddbb_json


def merge_json(json_path, hospitals):

    needed_fields = [
        "codcnh",
        "collecting_institution_address",
        "collecting_institution_email",
        "geo_loc_state",
        "geo_loc_region",
        "geo_loc_city",
        "geo_loc_country"
    ]

    missing_json = {}

    with open(json_path, "r", encoding="utf-8") as json_file:
        previous_json = json.load(json_file)

    for new_codcnh, fields in hospitals.items():
        data_found = False
        for original_codcnh, data in previous_json.items():
            if original_codcnh == new_codcnh:
                data_found = True
                data_to_update = {k: fields[k] for k in needed_fields if k in fields}
                previous_json = replace_entry(previous_json, original_codcnh, fields['collecting_institution_name'], data_to_update)
                break
        if not data_found:
            missing_json[new_codcnh] = fields

    return previous_json, missing_json


def replace_entry(previous_dict, prev_key_name, new_key_name, new_values):
    nuevo_dict = {}
    for k, v in previous_dict.items():
        if k == prev_key_name:
            nuevo_valor = v.copy()
            nuevo_valor.update(new_values)
            nuevo_dict[new_key_name] = nuevo_valor
        else:
            nuevo_dict[k] = v
    return nuevo_dict


def write_json(output_json, hospital_json):
    with open(output_json, "w", encoding="utf-8") as json_file:
        json.dump(hospital_json, json_file, ensure_ascii=False, indent=4)

    print(f"Archivo JSON generado: {output_json}")


# def write_coords_json():
#     json_dict_coords = {}

#     for index, row in df.iterrows():
#         hospital_name = to_title_case(row["NOMBRE"])
#         json_dict_coords[hospital_name] = {
#             "geo_loc_latitude": f"{row['Y']:.4f}",
#             "geo_loc_longitude": f"{row['X']:.4f}",
#         }
#     output_file_coords = "geo_loc_centers.json"
#     with open(output_file_coords, "w", encoding="utf-8") as json_file:
#         json.dump(json_dict_coords, json_file, ensure_ascii=False, indent=4)

#     print(f"Archivo JSON generado: {output_file_coords}")


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
        hospital_ddbb_json = add_hospitals(hospital_ddbb_json, add_json)

    hospital_ddbb = pd.DataFrame.from_dict(hospital_ddbb_json, orient='index')
    hospital_ddbb.reset_index(inplace=True)
    hospital_ddbb.rename(columns={'index': 'ccn'}, inplace=True)

    hospital_ddbb.to_excel(args.out_excel, sheet_name="hospitals", index=False)

    if args.previous_json:
        new_json, missing_json = merge_json(args.previous_json, hospital_ddbb_json)
        write_json(args.output_json, new_json)
        write_json("missing_hospitals.json", missing_json)
    else:
        write_json(args.output_json, hospital_ddbb_json)

if __name__ == "__main__":
    sys.exit(main())
