#!/usr/bin/env python3
"""
Script: ucct_status_stats_recent.py
Purpose:
  Build a per-lab table with columns:
    - Laboratory
    - Total               (ALWAYS historical sum; prefers #analyzed_samples, fallback #downloaded_samples)
    - Recently Processed  ("{SARS} SARS - {Influenza} Influenza", filtered by since-date if provided)

Filtering:
  - The 'batch' column contains a timestamp-like string (e.g., "2025041593150").
    We use the FIRST 8 DIGITS as YYYYMMDD to filter (inclusive).
  - If --since is omitted, 'Recently Processed' uses the full history.

Output:
  - CSV with per-lab rows and a final [TOTAL] row:
      - Total = historical grand total
      - Recently Processed = filtered sums (SARS and Influenza)
"""

import argparse
import re
from datetime import datetime
from pathlib import Path
import pandas as pd


def normalize_col(s: str) -> str:
    """Lower-case and normalize whitespace for robust column matching."""
    return re.sub(r"\s+", " ", str(s)).strip().lower()


def parse_batch_to_date(batch_value):
    """Parse first 8 digits of 'batch' as YYYYMMDD; return None if invalid."""
    if pd.isna(batch_value):
        return None
    s = re.sub(r"\D", "", str(batch_value))
    if len(s) < 8:
        return None
    ymd = s[:8]
    try:
        return datetime.strptime(ymd, "%Y%m%d")
    except Exception:
        return None


def find_columns(df: pd.DataFrame) -> dict:
    """
    Resolve actual DataFrame columns for required logical fields.
    'analyzed' is optional (fallback to 'downloaded' for Total).
    """
    required = {
        "lab": ["hospital/centro", "hospital", "centro", "laboratorio", "lab"],
        "batch": ["batch", "fecha", "date"],
        "downloaded": ["#downloaded_samples", "downloaded_samples"],
        "analyzed": ["#analyzed_samples", "analyzed_samples"],
        "sarscov2": [
            "#sars-cov-2 samples",
            "#sars-cov-2",
            "sars-cov-2 samples",
            "sars-cov-2",
        ],
        "influenza": ["#influenza samples", "influenza samples"],
    }
    norm_map = {normalize_col(c): c for c in df.columns}

    mapping = {}
    for key, candidates in required.items():
        found = None
        for cand in candidates:
            cand_norm = normalize_col(cand)
            if cand_norm in norm_map:
                found = norm_map[cand_norm]
                break
        if not found:
            tokens = [c.replace("#", "").strip().lower() for c in candidates]
            for norm_c, orig_c in norm_map.items():
                if any(t in norm_c for t in tokens):
                    found = orig_c
                    break
        if not found:
            if key == "analyzed":  # optional
                mapping[key] = None
                continue
            raise ValueError(
                f"Required column not found for '{key}' (tried: {candidates})"
            )
        mapping[key] = found
    return mapping


def coerce_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")
    return df


def compute_tables(df: pd.DataFrame, mapping: dict, since_date=None) -> pd.DataFrame:
    """Build a table with historical 'Total' and filtered 'Recently Processed'."""
    df = df.copy()
    df = coerce_numeric(
        df,
        [
            mapping["downloaded"],
            mapping["analyzed"],
            mapping["sarscov2"],
            mapping["influenza"],
        ],
    )

    # Decide "Total" metric
    total_col = (
        mapping["analyzed"]
        if (mapping["analyzed"] and mapping["analyzed"] in df.columns)
        else mapping["downloaded"]
    )

    # 1) HISTORICAL (full) per-lab totals
    hist = (
        df.groupby(mapping["lab"], dropna=False)[[total_col]]
        .sum()
        .reset_index()
        .rename(columns={mapping["lab"]: "Laboratory", total_col: "Total"})
    )

    # 2) RECENT (filtered) per-lab SARS/Influenza and formatted string
    df["_parsed_batch_date"] = df[mapping["batch"]].apply(parse_batch_to_date)
    if since_date is not None:
        recent_df = df[
            df["_parsed_batch_date"].notna() & (df["_parsed_batch_date"] >= since_date)
        ]
    else:
        recent_df = df

    recent = (
        recent_df.groupby(mapping["lab"], dropna=False)[
            [mapping["sarscov2"], mapping["influenza"]]
        ]
        .sum()
        .reset_index()
        .rename(
            columns={
                mapping["lab"]: "Laboratory",
                mapping["sarscov2"]: "_SARS",
                mapping["influenza"]: "_Influenza",
            }
        )
    )
    recent["Recently Processed"] = recent.apply(
        lambda r: f"{int(r['_SARS'])} SARS - {int(r['_Influenza'])} Influenza", axis=1
    )
    recent = recent[["Laboratory", "Recently Processed"]]

    # Merge historical total with recent processed
    merged = pd.merge(hist, recent, on="Laboratory", how="outer").fillna(
        {"Total": 0, "Recently Processed": "0 SARS - 0 Influenza"}
    )
    merged = merged.sort_values("Laboratory").reset_index(drop=True)

    # 3) Append [TOTAL] row
    overall_total_hist = int(hist["Total"].sum())
    overall_recent_sars = int(recent_df[mapping["sarscov2"]].sum())
    overall_recent_infl = int(recent_df[mapping["influenza"]].sum())
    total_row = pd.DataFrame(
        {
            "Laboratory": ["[TOTAL]"],
            "Total": [overall_total_hist],
            "Recently Processed": [
                f"{overall_recent_sars} SARS - {overall_recent_infl} Influenza"
            ],
        }
    )
    merged = pd.concat([merged, total_row], ignore_index=True)
    return merged


def main():
    import argparse

    ap = argparse.ArgumentParser(
        description="Per-lab stats with historical Total and filtered 'Recently Processed'."
    )
    ap.add_argument("--xlsx", required=True, help="Path to the Excel file (.xlsx)")
    ap.add_argument("--sheet", default="UCCT_Relecov_data_status", help="Sheet name")
    ap.add_argument(
        "--since",
        default=None,
        help="Filter 'Recently Processed' from date YYYYMMDD (inclusive). If omitted, full history.",
    )
    ap.add_argument(
        "--out",
        default="ucct_relecov_lab_stats_recent_hist_total.csv",
        help="Output CSV path",
    )
    args = ap.parse_args()

    xls = pd.ExcelFile(args.xlsx)
    if args.sheet not in xls.sheet_names:
        raise ValueError(
            f"Sheet '{args.sheet}' not found. Available: {xls.sheet_names}"
        )

    df = pd.read_excel(args.xlsx, sheet_name=args.sheet)
    mapping = find_columns(df)
    since_dt = datetime.strptime(args.since, "%Y%m%d") if args.since else None

    res = compute_tables(df, mapping, since_dt)
    res.to_csv(args.out, index=False)
    print(f"Saved: {args.out}")


if __name__ == "__main__":
    main()
