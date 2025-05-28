# Spanish Hospitals JSON Converter

This script converts a CSV file with hospital data in Spain into two structured JSON files.

## Input

- `Hospitales_de_Espaadb.csv`  
  Must include columns like `NOMBRE`, `DIRECCION`, `EMAIL`, `FINALIDAD_ASISITENCIAL`, `COMUNIDADES`, `PROVINCIAS`, `MUNICIPIOS`, `pais`, `X`, and `Y`.

## Output

- `laboratory_address.json`: general info per hospital (address, region, purpose, etc.).
- `geo_loc_centers.json`: geolocation info (latitude and longitude).

## How it works

- Reads the CSV with `pandas`.
- Normalizes text to title case.
- Fills missing values with defaults (`"Unknown"` / `"Spain"`).
- Saves results as two JSON files (UTF-8, pretty-printed).

## Run

```bash
python transform.py