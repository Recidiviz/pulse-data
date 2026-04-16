# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================
"""
Transforms a TX critically understaffed locations xlsx file into a formatted CSV
and uploads it to the TX ingest GCS buckets (staging and prod).

The xlsx is structured with region header rows (e.g. "REGION I") and totals rows
(e.g. "Region I Totals") that are filtered out. The remaining rows are individual
offices. Office names are matched to OFFC_REGION/OFFC_DISTRICT using OfficeDescriptions.csv.

Usage:
    python -m recidiviz.tools.ingest.operations.state_data_manual_uploads.transform_and_upload_tx_understaffed \
        --xlsx_path <path_to_xlsx> \
        --upload_date <YYYY-MM-DD> \
        [--dry_run]
"""
import argparse
import os
import re
import subprocess
import tempfile

import pandas as pd

OFFICE_DESCRIPTIONS_PATH = os.path.join(
    os.path.dirname(__file__), "OfficeDescriptions.csv"
)
FILE_TAG = "manual_upload_critically_understaffed_locations"
STAGING_BUCKET = "recidiviz-staging-direct-ingest-state-us-tx"
PROD_BUCKET = "recidiviz-123-direct-ingest-state-us-tx"

ROMAN_TO_INT = {
    "I": 1,
    "II": 2,
    "III": 3,
    "IV": 4,
    "V": 5,
    "VI": 6,
    "VII": 7,
    "VIII": 8,
    "IX": 9,
    "X": 10,
}

MONTH_TO_INT = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


def _roman_to_arabic(s: str) -> str:
    """Converts trailing Roman numeral in a string to an Arabic number."""
    parts = s.strip().split()
    if parts and parts[-1].upper() in ROMAN_TO_INT:
        parts[-1] = str(ROMAN_TO_INT[parts[-1].upper()])
    return " ".join(parts)


def _normalize_dpo_name(name: str) -> str:
    """Normalizes a DPO name for matching against OFFC_NAME in OfficeDescriptions.csv."""
    name = _roman_to_arabic(name.strip())
    name = name.upper()
    # Try to match "{CITY} {NUM}" → "{CITY} DPO {NUM}"
    match = re.match(r"^(.*)\s(\d+)$", name)
    if match:
        return f"{match.group(1)} DPO {match.group(2)}"
    return f"{name} DPO"


def _build_office_lookup() -> dict[str, tuple[str, str, str]]:
    """Returns a dict mapping normalized OFFC_NAME → (OFFC_REGION, OFFC_DISTRICT, OFFC_TYPE)."""
    df = pd.read_csv(OFFICE_DESCRIPTIONS_PATH, dtype=str)
    lookup = {}
    for _, row in df.iterrows():
        region = str(row["OFFC_REGION"]).zfill(2)
        district = str(row["OFFC_DISTRICT"]).zfill(2)
        offc_type = str(row["OFFC_TYPE"])
        name_key = str(row["OFFC_NAME"]).upper().strip()
        lookup[name_key] = (region, district, offc_type)
    return lookup


def _extract_month_year_from_filename(path: str) -> tuple[int, int]:
    """Extracts month and year from a filename like 'Critically Understaffed - March 2026 (1).xlsx'."""
    basename = os.path.basename(path)
    for month_name, month_num in MONTH_TO_INT.items():
        if month_name in basename.lower():
            year_match = re.search(r"(\d{4})", basename)
            if year_match:
                return month_num, int(year_match.group(1))
    raise ValueError(
        f"Could not extract month/year from filename: {basename}. "
        "Expected format: 'Critically Understaffed - <Month> <Year>.xlsx'"
    )


def transform(xlsx_path: str) -> pd.DataFrame:
    """Reads the xlsx and returns a formatted DataFrame ready for CSV output."""
    raw = pd.read_excel(xlsx_path)
    office_lookup = _build_office_lookup()
    month, year = _extract_month_year_from_filename(xlsx_path)

    # Filter out region header rows and totals rows
    df = (
        raw[~raw["DPO"].str.contains("REGION|Totals", na=True, case=False)]
        .dropna(subset=["ALLOTTED"])
        .copy()
    )

    rows = []
    unmatched = []
    for _, row in df.iterrows():
        dpo = str(row["DPO"]).strip()
        normalized = _normalize_dpo_name(dpo)
        match = office_lookup.get(normalized)
        if match is None:
            unmatched.append(dpo)
            continue
        region, district, offc_type = match
        pct_raw = row["PERCENTAGE NON-CASE CARRYING"]
        pct_str = f"{float(pct_raw) * 100:.2f}%"
        rows.append(
            {
                "DPO": dpo,
                "ALLOTTED": int(row["ALLOTTED"]),
                "VACANT": int(row["VACANT"]),
                "LEAVE": int(row["LEAVE "]),
                "VACANT_AND_LEAVE_TOTAL": int(row["VACANT & LEAVE TOTAL"]),
                "PERCENTAGE_NON_CASE_CARRYING": pct_str,
                "OFFC_REGION": region,
                "OFFC_DISTRICT": district,
                "OFFC_TYPE": offc_type,
                "MONTH": month,
                "YEAR": year,
            }
        )

    if unmatched:
        raise ValueError(
            "Could not match the following DPO names to an office:\n"
            + "\n".join(f"  - {name}" for name in unmatched)
            + "\nPlease update OfficeDescriptions.csv or the normalization logic."
        )

    return pd.DataFrame(rows)


def upload_to_gcs(csv_path: str, upload_date: str, dry_run: bool) -> None:
    """Uploads the CSV to both staging and prod TX ingest buckets."""
    gcs_filename = f"unprocessed_{upload_date}T00:00:00:000000_raw_{FILE_TAG}.csv"
    for bucket in [STAGING_BUCKET, PROD_BUCKET]:
        dest = f"gs://{bucket}/{gcs_filename}"
        cmd = f"gsutil cp {csv_path} {dest}"
        if dry_run:
            print(f"[DRY RUN] Would run: {cmd}")
        else:
            print(f"Uploading to {dest}...")
            result = subprocess.run(  # nosec B603 B607
                ["gsutil", "cp", csv_path, dest], check=False
            )
            if result.returncode != 0:
                raise RuntimeError(f"gsutil cp failed for {dest}")
            print(f"  ✓ Uploaded to {dest}")


def main() -> None:
    """Parses arguments, runs the transformation, and uploads the result to GCS."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--xlsx_path", required=True, help="Path to the xlsx file")
    parser.add_argument(
        "--upload_date",
        required=True,
        help="Date to use in the GCS filename (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        default=False,
        help="Print actions without uploading",
    )
    args = parser.parse_args()

    print(f"Transforming {args.xlsx_path}...")
    df = transform(args.xlsx_path)
    print(f"\nTransformed {len(df)} offices:")
    print(df.to_markdown(tablefmt="grid", index=False))

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", prefix=FILE_TAG + "_", delete=False
    ) as f:
        tmp_path = f.name
        df.to_csv(f, index=False)

    try:
        print(f"\nCSV written to {tmp_path}")
        upload_to_gcs(tmp_path, args.upload_date, args.dry_run)
    finally:
        os.remove(tmp_path)


if __name__ == "__main__":
    main()
