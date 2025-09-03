# Usage Guide – Hospital & Geography Utilities

This folder groups **three independent scripts** that keep Relecov LIMS tables and JSON sources in sync. Each script can be run on its own; there is **no required order**, but the most common workflow is:

1. **`create_hospital_database.py`** → generate/refresh the master *laboratory\_address.json* and supporting files.
2. **`update_geo_from_json.py`**     → push new/updated **states & cities** to MySQL.
3. **`populate_lab_requests.py`**    → push new/updated **laboratories** to MySQL.

---

## 0  Common prerequisites

| Requirement                   | Notes                                                             |
| ----------------------------- | ----------------------------------------------------------------- |
| Python ≥ 3.9                  | Tested on 3.10.                                                   |
| `pandas`, `numpy`, `openpyxl` | Only needed for **create\_hospital\_database.py**.                |
| `mysql-connector-python`      | Needed for the two DB‑facing scripts.                             |
| `geopy`                       | Optional; adds auto‑geocoding in *create\_hospital\_database.py*. |
| Database credentials          | Via flags or the env‑vars below.                                  |

### Optional connection env‑vars

| Var                  | Default       | Used by    |
| -------------------- | ------------- | ---------- |
| `DB_SERVER_IP`       | `localhost`   | DB scripts |
| `DB_PORT`            | `3306`        | DB scripts |
| `DB_USER`, `DB_PASS` | *(none)*      | DB scripts |
| `DB_NAME`            | `relecovlims` | DB scripts |

If a flag and an env‑var set the same value, **the flag wins**.

---

## 1  create\_hospital\_database.py

**Purpose** Merge the latest REGCESS and CNH Excel spreadsheets (plus optional JSON add‑ons) into a single master:

* **`laboratory_address.json`** – canonical record for every hospital/lab.
* **`hospitals_ddbb.xlsx`** – Excel snapshot of the same data.
* Side files: `differences.json`, `missing.json`, `geo_loc_cities.json`, etc.

### Minimal run

```bash
python create_hospital_database.py \
  -r Metadatos_CARLOS_III.xlsx \
  -c CNH_2024.xlsx
```

### Most common full run

```bash
python create_hospital_database.py \
  -r Metadatos_CARLOS_III.xlsx \
  -c CNH_2024.xlsx \
  -o hospitals_ddbb.xlsx \
  -a non_ddbb_address.json \
  -j laboratory_address.json \
  -d differences.json \
  -m missing.json \
  -p previous_laboratory_address.json
```

| Flag                      | Meaning                                                    |
| ------------------------- | ---------------------------------------------------------- |
| `-r / --regcess`          | REGCESS Excel (contains **CCN**).                          |
| `-c / --cnh`              | CNH Excel (contains **CODCNH**).                           |
| `-a / --additional_json`  | Extra hospitals not present in REGCESS/CNH.                |
| `-p / --previous_json`    | Compare against previous JSON to capture removals/changes. |
| `-o / --out_excel`        | Where to save the combined Excel.                          |
| `-j / --output_json`      | Where to write the new JSON master.                        |
| `-d / --differences_json` | Diff report between previous & new.                        |
| `-m / --missing_json`     | Hospitals that disappeared.                                |
| `-g / --geo_loc_cities`   | City‑coords JSON (updated if new cities found).            |

**Outputs**

* A fresh *laboratory\_address.json* ready for the downstream scripts.
* Excel and diff files for auditing.

---

## 2  update\_geo\_from\_json.py

**Purpose** Synchronise **core\_state\_in\_country** and **core\_city** tables from JSON.

* Inserts **new states & cities** (use `--insert-missing`).
* Updates coordinates, city & state codes, and ensures `apps_name='wetlab'` when blank.

### Typical usage

```bash
python update_geo_from_json.py \
  --city-json geo_loc_cities.json \
  --lab-json  laboratory_address.json \
  --insert-missing \
  --report update_geo_report.csv
```

| Flag                             | Meaning                                            |
| -------------------------------- | -------------------------------------------------- |
| `--city-json`                    | City‑coords JSON (*geo\_loc\_cities.json*).        |
| `--lab-json`                     | Master hospital JSON (*laboratory\_address.json*). |
| `--insert-missing`               | Allow INSERT of unknown cities/states.             |
| `--dry-run`                      | Don’t commit; rollback instead.                    |
| `--report`                       | CSV summary of `state_*` / `city_*` actions.       |
| Connection flags (`--host` etc.) | Override env‑vars.                                 |

**What it does**

1. Reads existing states/cities from MySQL.
2. Compares against JSON.
3. Generates `state_insert`, `state_update`, `city_insert`, `city_update` actions.
4. Writes an optional CSV report and commits (unless `--dry-run`).

---

## 3  populate\_lab\_requests.py

**Purpose** Synchronise `core_lab_request` (laboratories) with *laboratory\_address.json*.

* Deduplicates using `lab_code_1` (unique) or fallback on normalised `lab_name`.
* Inserts or updates as needed.
* Auto‑fills `apps_name='wetlab'`, `lab_unit='-'`, `lab_contact_name='-'` when blank.

### Typical usage

```bash
python populate_lab_requests.py \
  --lab-json laboratory_address.json \
  --insert-missing \
  --report populate_lab_report.csv
```

| Flag               | Meaning                             |
| ------------------ | ----------------------------------- |
| `--lab-json`       | Master hospital JSON.               |
| `--insert-missing` | Insert labs not yet in DB.          |
| `--dry-run`        | Simulate, then rollback.            |
| `--report`         | CSV summary of `insert` / `update`. |
| Connection flags   | Same as above.                      |

**Deduplication summary**

1. **Match by `lab_code_1`** → update.
2. Else **match by normalised name** if DB row lacks a code.
3. Otherwise insert.

---

## 4  Best practice workflow

1. **Build JSON master**

   ```bash
   python create_hospital_database.py -r REGCESS.xlsx -c CNH.xlsx -j laboratory_address.json
   ```
2. **Dry‑run geo update**

   ```bash
   python update_geo_from_json.py --city-json geo_loc_cities.json --lab-json laboratory_address.json --insert-missing --dry-run
   ```
3. **Dry‑run lab update**

   ```bash
   python populate_lab_requests.py --lab-json laboratory_address.json --insert-missing --dry-run
   ```
4. Review reports ➜ if OK, rerun **without `--dry-run`**.

---