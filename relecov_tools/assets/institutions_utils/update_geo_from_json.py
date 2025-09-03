#!/usr/bin/env python3
from __future__ import annotations
import argparse
import csv
import datetime
import json
import logging
import os
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Any, Dict, Tuple, List

import mysql.connector

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ─── City columns ───────────────────────────────────────────────────────
COL_CITY_NAME = "city_name"
COL_CITY_COD = "geo_loc_city_cod"
COL_LAT = "geo_loc_latitude"
COL_LON = "geo_loc_longitude"
COL_STATE_ID = "belongs_to_state_id"
COL_APPS = "apps_name"

# ─── Table of states ──────────────────────────────────────────────────────
STATE_TABLE = "core_state_in_country"
STATE_COL_ID = "id"
STATE_COL_NAME = "state_name"
STATE_COL_CODE = "geo_loc_state_cod"

APPS_DEFAULT = "wetlab"


# ─── Utilities ────────────────────────────────────────────────────────────
def load_json(path: str | Path) -> Any:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def rounded(val, nd=6):
    if val in (None, ""):
        return None
    return float(
        Decimal(str(val)).quantize(Decimal(f"1e-{nd}"), rounding=ROUND_HALF_UP)
    )


def q(col: str) -> str:
    return f"`{col}`"


# ─── Maps from lab_json ──────────────────────────────────────────────────
def build_maps(lab_json):
    city_cod, state_cod = {}, {}
    for rec in lab_json.values():
        c, cc = rec.get("geo_loc_city"), rec.get("geo_loc_city_cod")
        s, sc = rec.get("geo_loc_state"), rec.get("geo_loc_state_cod")
        if c and cc and c not in city_cod:
            city_cod[c] = cc
        if s and sc and s not in state_cod:
            state_cod[s] = sc
    return city_cod, state_cod


# ─── Secure/update status ────────────────────────────────────────────
def ensure_state(
    cur,
    name: str,
    code: str | None,
    table: str,
    cache: Dict[str, Tuple[int, str | None]],
    report: List[dict],
) -> int:
    """
    • If the status already exists, it returns its ID and updates its code if it has changed.
    • If it does not exist, it inserts it with the code received.
    """
    if name in cache:
        sid, db_code = cache[name]
        # update code if a new one comes in ≠ BD and is not empty
        if code and code != (db_code or ""):
            cur.execute(
                f"UPDATE {q(table)} SET {q(STATE_COL_CODE)}=%s WHERE {q(STATE_COL_ID)}=%s",
                (code, sid),
            )
            cache[name] = (sid, code)
            report.append(
                {
                    "action": "state_update",
                    "state": name,
                    "state_id": sid,
                    "old_code": db_code,
                    "new_code": code,
                }
            )
            logging.info("Updated State code '%s': %s → %s", name, db_code, code)
        return sid

    # Insert new state
    cols, vals = [q(STATE_COL_NAME)], [name]
    if code:
        cols.append(q(STATE_COL_CODE))
        vals.append(code)
    cur.execute(
        f"INSERT INTO {q(table)} ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(vals))})",
        tuple(vals),
    )
    sid = cur.lastrowid
    cache[name] = (sid, code)
    report.append(
        {"action": "state_insert", "state": name, "state_id": sid, "state_code": code}
    )
    logging.info("Inserted state '%s' (id %s)", name, sid)
    return sid


# ─── Main ─────────────────────────────────────────────────────────────
def main(a):
    city_json = load_json(a.city_json)
    lab_json = load_json(a.lab_json)
    city_cod_map, state_cod_map = build_maps(lab_json)

    conn = mysql.connector.connect(
        host=a.host,
        port=a.port,
        user=a.user,
        password=a.password,
        database=a.database,
        autocommit=False,
        use_pure=True,
    )
    cur = conn.cursor(dictionary=True)

    # Cache states
    cur.execute(
        f"SELECT {q(STATE_COL_ID)} id, {q(STATE_COL_NAME)} name, {q(STATE_COL_CODE)} code "
        f"FROM {q(a.state_table)}"
    )
    state_cache = {r["name"]: (r["id"], r["code"]) for r in cur.fetchall()}

    tbl = q(a.table)
    report = []

    # Process each city
    for city, coords in city_json.items():
        # Find the state name from lab_json
        state_name = next(
            (
                r.get("geo_loc_state")
                for r in lab_json.values()
                if r.get("geo_loc_city") == city
            ),
            None,
        )
        state_id = None
        if state_name:
            state_id = ensure_state(
                cur,
                state_name,
                state_cod_map.get(state_name),
                a.state_table,
                state_cache,
                report,
            )

        new_lat = rounded(coords.get("geo_loc_latitude"))
        new_lon = rounded(coords.get("geo_loc_longitude"))
        new_cod = city_cod_map.get(city)

        # Check if the city already exists
        cur.execute(
            f"SELECT id, {q(COL_LAT)} lat, {q(COL_LON)} lon, {q(COL_CITY_COD)} cod, "
            f"{q(COL_STATE_ID)} sid, {q(COL_APPS)} apps "
            f"FROM {tbl} WHERE {q(COL_CITY_NAME)}=%s",
            (city,),
        )
        rows = cur.fetchall()

        if not rows:
            if not a.insert_missing:
                logging.warning(
                    "City '%s' does not exists and --insert-missing off", city
                )
                continue
            cols = [q(COL_CITY_NAME), q(COL_LAT), q(COL_LON), q(COL_APPS)]
            vals = [city, new_lat, new_lon, APPS_DEFAULT]
            if new_cod:
                cols.append(q(COL_CITY_COD))
                vals.append(new_cod)
            if state_id:
                cols.append(q(COL_STATE_ID))
                vals.append(state_id)
            cur.execute(
                f"INSERT INTO {tbl} ({', '.join(cols)}) VALUES ({', '.join(['%s']*len(vals))})",
                tuple(vals),
            )
            report.append(
                {
                    "action": "city_insert",
                    "id": cur.lastrowid,
                    "city": city,
                    "state": state_name,
                }
            )
            logging.info("Insert City '%s'", city)
            continue

        # Possible updates
        for r in rows:
            changes, params = [], []
            if rounded(r["lat"]) != new_lat:
                changes.append(f"{q(COL_LAT)}=%s")
                params.append(new_lat)
            if rounded(r["lon"]) != new_lon:
                changes.append(f"{q(COL_LON)}=%s")
                params.append(new_lon)
            if new_cod and r["cod"] != new_cod:
                changes.append(f"{q(COL_CITY_COD)}=%s")
                params.append(new_cod)
            if state_id and r["sid"] != state_id:
                changes.append(f"{q(COL_STATE_ID)}=%s")
                params.append(state_id)
            if not r["apps"]:
                changes.append(f"{q(COL_APPS)}=%s")
                params.append(APPS_DEFAULT)
            if changes:
                params.append(r["id"])
                cur.execute(
                    f"UPDATE {tbl} SET {', '.join(changes)} WHERE id=%s", tuple(params)
                )
                report.append({"action": "city_update", "id": r["id"], "city": city})

    # commit / rollback
    if a.dry_run:
        conn.rollback()
        logging.info("DRY-RUN: %d ops", len(report))
    else:
        conn.commit()
        logging.info("%d applied operations", len(report))
    conn.close()

    # Save report
    if report:
        out = Path(a.report or f"update_geo_report_{datetime.date.today()}.csv")
        with out.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=sorted({k for r in report for k in r})
            )
            writer.writeheader()
            writer.writerows(report)
        logging.info("Report: %s", out)


# ─── CLI ───────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    p = argparse.ArgumentParser(
        description="Sync states and cities (+apps_name=“wetlab”)"
    )
    p.add_argument("--city-json", required=True)
    p.add_argument("--lab-json", required=True)
    p.add_argument("--host", default=os.getenv("DB_SERVER_IP", "localhost"))
    p.add_argument("--port", type=int, default=int(os.getenv("DB_PORT", 3306)))
    p.add_argument("--user", default=os.getenv("DB_USER"))
    p.add_argument("--password", default=os.getenv("DB_PASS"))
    p.add_argument("--database", default=os.getenv("DB_NAME", "relecovlims"))
    p.add_argument("--table", default="core_city")
    p.add_argument("--state-table", default=STATE_TABLE)
    p.add_argument("--insert-missing", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--report")
    main(p.parse_args())
