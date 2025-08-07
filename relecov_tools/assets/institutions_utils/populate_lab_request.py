#!/usr/bin/env python3
from __future__ import annotations
import argparse
import csv
import datetime
import json
import logging
import os
import re
import unicodedata
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Set

import mysql.connector

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ────── Constantes de BD ───────────────────────────────────────────────────
CITY_TABLE, CITY_COL_ID, CITY_COL_NAME = "core_city", "id", "city_name"
LAB_TABLE = "core_lab_request"
LAB_COL_ID, LAB_COL_CODE1, LAB_COL_CODING = "id", "lab_code_1", "lab_name_coding"

FIELD_MAP = {
    "collecting_institution_code_1": "lab_code_1",
    "collecting_institution_code_2": "lab_code_2",
    "collecting_institution": "lab_name",
    "collecting_institution_address": "address",
    "collecting_institution_email": "lab_email",
    "collecting_institution_phone": "lab_phone",
    "lab_geo_loc_latitude": "lab_geo_loc_latitude",
    "lab_geo_loc_longitude": "lab_geo_loc_longitude",
    "autonom_cod": "autonom_cod",
    "post_code": "post_code",
    "dep_func": "dep_func",
    "center_class_code": "center_class_code",
    "collecting_institution_function": "lab_function",
}
DEFAULTS = {"lab_unit": "-", "lab_contact_name": "-"}
DEFAULT_APPS = "wetlab"
MAX_CODING_LEN = 50
_quote = lambda c: f"`{c}`"


# ────── Generic helpers ──────────────────────────────────────────────────
def load_json(p: str | Path) -> Any:
    return json.loads(Path(p).read_text(encoding="utf-8"))


def rounded(v, nd=6):
    if v is None or str(v).strip().lower() in {"", "nan", "null", "none"}:
        return None
    try:
        return float(Decimal(str(v)).quantize(Decimal(f"1e-{nd}"), ROUND_HALF_UP))
    except Exception:
        logging.warning("Lat/Lon '%s' no numérico; NULL", v)
        return None


def normalize(txt: str) -> str:
    txt = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", txt).strip().lower()


# ────── unique lab_name_coding ─────────────────────────────────────────────
def clean_words(n: str) -> list[str]:
    n = re.sub(r"\([^)]*\)", " ", n)
    n = unicodedata.normalize("NFKD", n).encode("ascii", "ignore").decode()
    return [w for w in re.split(r"[^A-Za-z0-9]+", n) if w]


def build_code(words: list[str], k: int) -> str:
    return (
        "".join(
            w[0].upper() + w[1:k].lower() if len(w) >= k else w[0].upper()
            for w in words
        )
        or "LAB"
    )


def unique_code(name: str, taken: Set[str]) -> str:
    ws = clean_words(name)
    longest = max((len(w) for w in ws), default=1)
    for k in range(1, longest + 1):
        c = build_code(ws, k)[:MAX_CODING_LEN]
        if c not in taken:
            taken.add(c)
            return c
    base = build_code(ws, 1)[: MAX_CODING_LEN - 4]
    for n in range(1, 1000):
        c = f"{base}{n:03d}"[:MAX_CODING_LEN]
        if c not in taken:
            taken.add(c)
            return c
    raise RuntimeError("Unable to create unique lab_name_coding")


# ────── caches FK y codings ───────────────────────────────────────────────
def cache_cities(cur) -> Dict[str, int]:
    cur.execute(
        f"SELECT {_quote(CITY_COL_NAME)},{_quote(CITY_COL_ID)} FROM {_quote(CITY_TABLE)}"
    )
    return {r[CITY_COL_NAME]: r[CITY_COL_ID] for r in cur.fetchall()}


def cache_codings(cur) -> Set[str]:
    cur.execute(f"SELECT {_quote(LAB_COL_CODING)} FROM {_quote(LAB_TABLE)}")
    return {r[LAB_COL_CODING] for r in cur.fetchall() if r[LAB_COL_CODING]}


# ────── MAIN ───────────────────────────────────────────────────────────────
def main(a):
    data = load_json(a.lab_json)
    conn = mysql.connector.connect(
        host=a.host,
        port=a.port,
        user=a.user,
        password=a.password,
        database=a.database,
        autocommit=False,
        use_pure=True,
    )
    cur = conn.cursor(dictionary=True, buffered=True)
    cities = cache_cities(cur)
    taken = cache_codings(cur)

    q_lab, q_id, q_code1 = map(_quote, (LAB_TABLE, LAB_COL_ID, LAB_COL_CODE1))
    report = []

    for rec in data.values():
        c1 = (rec.get("collecting_institution_code_1") or "").strip()
        name = (rec.get("collecting_institution") or "").strip()
        if not name:
            continue
        name_norm = normalize(name)

        row = None
        # Step 1: by code
        if c1:
            cur.execute(f"SELECT * FROM {q_lab} WHERE {q_code1}=%s", (c1,))
            row = cur.fetchone()

        # Step 2: by name if not found before
        if row is None:
            cur.execute(
                f"SELECT * FROM {q_lab} WHERE LOWER({_quote('lab_name')})=%s",
                (name_norm,),
            )
            tmp = cur.fetchone()
            # only use the match if your lab_code_1 is empty
            if tmp and not (tmp.get(LAB_COL_CODE1) or "").strip():
                row = tmp

        lab_id = row[LAB_COL_ID] if row else None
        if row and row.get(LAB_COL_CODING):
            taken.add(row[LAB_COL_CODING])

        # ---------- Values to be recorded -------------------------------------
        vals: Dict[str, Any] = {}
        for j, col in FIELD_MAP.items():
            if j in rec:
                v = rec[j]
                if col in ("lab_geo_loc_latitude", "lab_geo_loc_longitude"):
                    v = rounded(v)
                vals[col] = v

        # Fill in lab_code_1 if it is empty in the database.
        if row and c1 and not (row.get(LAB_COL_CODE1) or "").strip():
            vals[LAB_COL_CODE1] = c1

        # apps_name = wetlab if missing (in JSON or DB)
        if not row or not (row.get("apps_name") or "").strip():
            vals.setdefault("apps_name", DEFAULT_APPS)

        vals[LAB_COL_CODING] = unique_code(name, taken)
        for k, d in DEFAULTS.items():
            vals.setdefault(k, d)
        if cid := cities.get(rec.get("geo_loc_city")):
            vals["lab_city_id"] = cid

        # ---------- INSERT / UPDATE -------------------------------------
        if lab_id is None:
            if not a.insert_missing:
                continue
            cols, pars = zip(*vals.items()) if vals else ([], [])
            cur.execute(
                f"INSERT INTO {q_lab} ({', '.join(map(_quote, cols))}) "
                f"VALUES ({', '.join(['%s'] * len(pars))})",
                pars,
            )
            report.append({"action": "insert", "id": cur.lastrowid, "lab": name})
        else:
            if vals:
                setc = ", ".join(f"{_quote(c)}=%s" for c in vals)
                cur.execute(
                    f"UPDATE {q_lab} SET {setc} WHERE {q_id}=%s",
                    list(vals.values()) + [lab_id],
                )
                report.append({"action": "update", "id": lab_id, "lab": name})

    if a.dry_run:
        conn.rollback()
        logging.info("DRY-RUN: %d ops", len(report))
    else:
        conn.commit()
        logging.info("%d applied operations", len(report))
    conn.close()

    if report:
        out = Path(a.report or f"populate_lab_report_{datetime.date.today()}.csv")
        with out.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=sorted(report[0].keys()))
            w.writeheader()
            w.writerows(report)
        logging.info("Report in %s", out)


# ────── CLI ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Sync core_lab_request avoiding duplicates")
    p.add_argument("--lab-json", required=True)
    p.add_argument("--host", default=os.getenv("DB_SERVER_IP", "localhost"))
    p.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", 3306)))
    p.add_argument("--user", default=os.getenv("DB_USER"))
    p.add_argument("--password", default=os.getenv("DB_PASS"))
    p.add_argument("--database", default=os.getenv("DB_NAME", "relecovlims"))
    p.add_argument(
        "--insert-missing",
        action="store_true",
        help="Insert labs that do not exist",
    )
    p.add_argument("--dry-run", action="store_true", help="Rollback at the end")
    p.add_argument("--report", help="Report CSV (optional)")
    main(p.parse_args())
