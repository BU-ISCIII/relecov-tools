import pandas as pd
import json

csv_file = "Hospitales_de_Espaadb.csv"
df = pd.read_csv(csv_file)

json_dict = {}


def to_title_case(value, default="Desconocido"):
    if pd.isna(value) or value == "":
        return default
    return str(value).strip().title()


for index, row in df.iterrows():
    hospital_name = to_title_case(row["NOMBRE"])
    json_dict[hospital_name] = {
        "collecting_institution_address": to_title_case(row["DIRECCION"]),
        "collecting_institution_email": (
            row["EMAIL"].strip() if not pd.isna(row["EMAIL"]) else "Desconocido"
        ),
        "collecting_institution_finalidad": to_title_case(
            row["FINALIDAD_ASISITENCIAL"]
        ),
        "geo_loc_state": to_title_case(row["COMUNIDADES"]),
        "geo_loc_region": to_title_case(row.get("PROVINCIAS", None)),
        "geo_loc_city": to_title_case(row.get("MUNICIPIOS", None)),
        "geo_loc_country": to_title_case(row.get("pais", None), default="Spain"),
        "submitting_institution": "",
        "submitting_institution_address": "",
        "submitting_institution_email": "",
    }

output_file = "laboratory_address.json"
with open(output_file, "w", encoding="utf-8") as json_file:
    json.dump(json_dict, json_file, ensure_ascii=False, indent=4)

print(f"Archivo JSON generado: {output_file}")

json_dict_coords = {}


for index, row in df.iterrows():
    hospital_name = to_title_case(row["NOMBRE"])
    json_dict_coords[hospital_name] = {
        "geo_loc_latitude": f"{row['Y']:.4f}",
        "geo_loc_longitude": f"{row['X']:.4f}",
    }
output_file_coords = "geo_loc_centers.json"
with open(output_file_coords, "w", encoding="utf-8") as json_file:
    json.dump(json_dict_coords, json_file, ensure_ascii=False, indent=4)

print(f"Archivo JSON generado: {output_file_coords}")
