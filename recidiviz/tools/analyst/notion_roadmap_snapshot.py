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
"""Uploads the Notion state-initiative roadmap to the
notion_snapshots.notion_roadmap_snapshots BigQuery table.

Notion API access is blocked on our current plan, so this reads a
manually-downloaded CSV export from ~/Downloads, renames every column to a
BigQuery-safe identifier, adds the Linear project slugId parsed from each row's
"Linear Project" URL, parses the columns the table schema types as DATE or
DATETIME, splits the "Working Period" date range into start and exclusive end
dates, and appends the rows as one snapshot. For example, a row with Initiative
"AZ Meetings", Linear Project
"https://linear.app/recidiviz/project/us-az-meetings-5c55d9954a2a/overview",
Target FSL "March 31, 2026", and Working Period
"July 1, 2025 → August 31, 2025" becomes (columns abridged):

    initiative: "AZ Meetings"
    linear_project: "https://linear.app/recidiviz/project/us-az-meetings-5c55d9954a2a/overview"
    linear_project_slug_id: "5c55d9954a2a"
    target_fsl: 2026-03-31
    working_period: "July 1, 2025 → August 31, 2025"
    working_period_start_date: 2025-07-01
    working_period_end_date_exclusive: 2025-09-01
    snapshot_date: "2026-10-01"

Run manually via
`python -m recidiviz.tools.analyst.notion_roadmap_snapshot --dry-run False`.
Defaults to dry-run mode, which validates the rows against the registered schema
without uploading them.
"""
import argparse
import datetime
import glob
import logging
import re
import zipfile
from pathlib import Path
from typing import Callable

import pandas as pd

from recidiviz.big_query.big_query_utils import normalize_column_name_for_bq
from recidiviz.issue_tracking.roadmap_snapshot_tables import (
    LINEAR_PROJECT_SLUG_ID_COLUMN,
    NOTION_ROADMAP_SNAPSHOTS_ADDRESS,
    SNAPSHOT_DATE_COLUMN,
    UPLOAD_DATETIME_COLUMN,
)
from recidiviz.source_tables.source_table_config import SourceTableConfig
from recidiviz.source_tables.yaml_managed.collect_yaml_managed_source_table_configs import (
    build_source_table_repository_for_yaml_managed_tables,
)
from recidiviz.tools.analyst.source_table_upload_utils import (
    append_df_to_source_table_in_all_projects,
    validate_and_convert_df_columns_to_schema,
)
from recidiviz.utils.params import str_to_bool

logger = logging.getLogger(__name__)

NOTION_ROADMAP_CSV_GLOB_PATTERNS = ("*State Initiative Roadmap*_all.csv",)

# The Notion "Linear Project" column, holding a Linear project URL.
NOTION_LINEAR_PROJECT_URL_COLUMN = "linear_project"

NOTION_WORKING_PERIOD_COLUMN = "working_period"
NOTION_WORKING_PERIOD_START_DATE_COLUMN = "working_period_start_date"
NOTION_WORKING_PERIOD_END_DATE_EXCLUSIVE_COLUMN = "working_period_end_date_exclusive"

# Formats Notion CSV exports use for date properties (e.g. "March 31, 2026") and
# for the created and last edited times (e.g. "March 25, 2026 8:47 AM").
NOTION_DATE_FORMAT = "%B %d, %Y"
NOTION_DATETIME_FORMAT = "%B %d, %Y %I:%M %p"

# Separates the start and end of a date range property, e.g.
# "July 1, 2025 → August 31, 2025".
_NOTION_DATE_RANGE_SEPARATOR = "→"

# Separates the dates in a rollup listing several dates, e.g.
# "November 1, 2019,January 22, 2020". Unlike the comma inside each date, this
# one has no space after it.
_NOTION_DATE_LIST_SEPARATOR_REGEX = re.compile(r",(?=\S)")

# DATE columns this script derives rather than reading from the export.
_DERIVED_DATE_COLUMNS = {
    NOTION_WORKING_PERIOD_START_DATE_COLUMN,
    NOTION_WORKING_PERIOD_END_DATE_EXCLUSIVE_COLUMN,
    SNAPSHOT_DATE_COLUMN,
}


def find_latest_notion_csv(*, downloads_dir: Path = Path.home() / "Downloads") -> Path:
    """Returns the most recently modified Notion "State Initiative Roadmap"
    CSV export found under |downloads_dir|.

    Notion exports as a zip (named like "*_ExportBlock*.zip") containing a
    "Private & Shared" directory with an "*_all.csv" file inside. Any
    not-yet-extracted zip newer than the newest already-extracted matching CSV
    is unzipped in place before searching. Raises FileNotFoundError if no
    matching CSV can be found.
    """
    _unzip_new_notion_export_blocks(downloads_dir)

    candidates = _find_notion_roadmap_csvs(downloads_dir)
    if not candidates:
        raise FileNotFoundError(
            f"No Notion roadmap CSV export found under [{downloads_dir}]. "
            "Re-export the roadmap from Notion (as CSV, include subpages) and "
            "download it before running this script."
        )

    return max(candidates, key=lambda p: p.stat().st_mtime)


def _find_notion_roadmap_csvs(downloads_dir: Path) -> list[Path]:
    """Returns every Notion roadmap CSV found under |downloads_dir| or its
    subdirectories."""
    return [
        Path(p)
        for pattern in NOTION_ROADMAP_CSV_GLOB_PATTERNS
        for p in glob.glob(str(downloads_dir / "**" / pattern), recursive=True)
    ]


_MAX_NESTED_UNZIP_LEVELS = 5


def _unzip_new_notion_export_blocks(downloads_dir: Path) -> None:
    """Unzips any Notion "*_ExportBlock*.zip" file in |downloads_dir| that is
    newer than the newest already-extracted matching CSV, so a freshly
    downloaded export is picked up without a manual unzip step."""
    zip_paths = sorted(downloads_dir.glob("*_ExportBlock*.zip"))
    if not zip_paths:
        return

    existing_csvs = _find_notion_roadmap_csvs(downloads_dir)
    newest_existing_csv_mtime = (
        max(p.stat().st_mtime for p in existing_csvs) if existing_csvs else 0.0
    )

    for zip_path in zip_paths:
        if zip_path.stat().st_mtime <= newest_existing_csv_mtime:
            continue
        extract_dir = zip_path.with_suffix("")
        logger.info("Unzipping new Notion export [%s] to [%s]", zip_path, extract_dir)
        _extract_zip_recursively(zip_path, extract_dir)


def _extract_zip_recursively(zip_path: Path, extract_dir: Path) -> None:
    """Extracts |zip_path| into |extract_dir|, then repeats on any zip files
    found inside the extracted contents, up to |_MAX_NESTED_UNZIP_LEVELS|
    levels.

    Notion sometimes wraps the actual export one level deeper than expected:
    the downloaded "<uuid>_ExportBlock-<uuid>.zip" contains a nested
    "ExportBlock-<uuid>-Part-1.zip" holding the real "Private & Shared" CSVs,
    rather than containing them directly.
    """
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(extract_dir)

    for _ in range(_MAX_NESTED_UNZIP_LEVELS):
        nested_zips = list(extract_dir.glob("**/*.zip"))
        if not nested_zips:
            return
        for nested_zip in nested_zips:
            with zipfile.ZipFile(nested_zip) as zf:
                zf.extractall(nested_zip.parent)
            nested_zip.unlink()


_COLUMN_NAME_SANITIZE_PATTERN = re.compile(r"[^a-z0-9_]+")


def _to_bq_safe_column_name(raw: str) -> str:
    """Returns a snake_case BigQuery column identifier for a raw Notion column
    header, e.g. "Contract to Data (weeks)" -> "contract_to_data_weeks".

    This step only puts the name into the lowercase snake_case shape the
    destination schema uses: it lowercases the header, collapses each run of
    non-identifier characters to a single underscore, and trims the underscores
    off both ends. normalize_column_name_for_bq() then applies BigQuery's own
    column rules, so a name that a leading digit or a reserved word would make
    invalid (e.g. "Range" -> "_range") gets its underscore prefix.
    """
    squashed = _COLUMN_NAME_SANITIZE_PATTERN.sub("_", raw.strip().lower()).strip("_")
    if not squashed:
        raise ValueError(f"Column name [{raw}] has no usable characters")
    return normalize_column_name_for_bq(squashed)


def _dedupe_column_names(names: list[str]) -> list[str]:
    """Returns |names| with any duplicates disambiguated by appending "_2",
    "_3", etc. to later occurrences.
    """
    seen_counts: dict[str, int] = {}
    deduped = []
    for name in names:
        seen_counts[name] = seen_counts.get(name, 0) + 1
        if seen_counts[name] == 1:
            deduped.append(name)
        else:
            deduped.append(f"{name}_{seen_counts[name]}")
    return deduped


_SLUG_ID_PATTERN = re.compile(r"-([0-9a-f]{12})(?:/|$)")


def _extract_slug_id(url: str) -> str | None:
    """Returns the 12-hex-character slugId embedded in a Linear project URL
    (the trailing hyphen-segment before "/overview" or the end of the
    string), or None if |url| is blank or doesn't match.
    """
    stripped = url.strip()
    if not stripped:
        return None
    match = _SLUG_ID_PATTERN.search(stripped)
    return match.group(1) if match else None


def _parse_working_period(
    working_period: str,
) -> tuple[datetime.date | None, datetime.date | None]:
    """Returns the start date and exclusive end date of a Notion "Working Period"
    value, e.g. "July 1, 2025 → August 31, 2025" -> (2025-07-01, 2025-09-01), or
    (None, None) if working_period is blank. Notion shows a range's end date
    inclusively, so the exclusive end is the day after it. A value with no end
    date covers just its start date.
    """
    if not working_period.strip():
        return None, None
    start_str, separator, end_str = working_period.partition(
        _NOTION_DATE_RANGE_SEPARATOR
    )
    start_date = datetime.datetime.strptime(
        start_str.strip(), NOTION_DATE_FORMAT
    ).date()
    end_date_inclusive = (
        datetime.datetime.strptime(end_str.strip(), NOTION_DATE_FORMAT).date()
        if separator
        else start_date
    )
    return start_date, end_date_inclusive + datetime.timedelta(days=1)


def _parse_notion_date(value: str) -> datetime.date | None:
    """Returns the date in a Notion date property value, e.g. "March 31, 2026",
    or None if value is blank. For a date range, e.g.
    "December 2, 2024 → January 13, 2025", returns the range's end date.
    """
    if not value.strip():
        return None
    _, _, last_date_str = value.rpartition(_NOTION_DATE_RANGE_SEPARATOR)
    return datetime.datetime.strptime(last_date_str.strip(), NOTION_DATE_FORMAT).date()


def _parse_notion_datetime(value: str) -> datetime.datetime | None:
    """Returns the date-time in a Notion created or last edited time value, e.g.
    "March 25, 2026 8:47 AM", or None if value is blank."""
    if not value.strip():
        return None
    return datetime.datetime.strptime(value.strip(), NOTION_DATETIME_FORMAT)


def _parse_notion_date_list(value: str) -> list[datetime.date]:
    """Returns every date in a Notion rollup value listing several dates, e.g.
    "November 1, 2019,January 22, 2020" -> [2019-11-01, 2020-01-22], sorted
    ascending. Returns an empty list if value is blank.
    """
    if not value.strip():
        return []
    return sorted(
        datetime.datetime.strptime(date_str.strip(), NOTION_DATE_FORMAT).date()
        for date_str in _NOTION_DATE_LIST_SEPARATOR_REGEX.split(value)
    )


def parse_notion_date_columns(
    df: pd.DataFrame, *, table_config: SourceTableConfig
) -> None:
    """Parses, in place, each column of df that table_config types as DATE,
    DATETIME, or REPEATED DATE from Notion's export format. Skips the DATE
    columns this script derives itself.

    Raises a ValueError naming the column and value if any value doesn't match
    Notion's format, so a change in Notion's export format fails the upload
    instead of writing nulls.
    """
    for field in table_config.schema_fields:
        if field.name in _DERIVED_DATE_COLUMNS:
            continue
        parse: Callable[
            [str], datetime.date | datetime.datetime | list[datetime.date] | None
        ]
        if field.field_type == "DATE" and field.mode == "REPEATED":
            parse = _parse_notion_date_list
        elif field.field_type == "DATE":
            parse = _parse_notion_date
        elif field.field_type == "DATETIME":
            parse = _parse_notion_datetime
        else:
            continue
        parsed_values = []
        for value in df[field.name]:
            try:
                parsed_values.append(parse(value))
            except ValueError as e:
                raise ValueError(
                    f"Notion column [{field.name}] has value [{value}], which "
                    "doesn't match the date format the table schema expects. Check "
                    "the export, or change the column's type in "
                    "notion_roadmap_snapshots.yaml if it no longer holds dates."
                ) from e
        df[field.name] = pd.Series(parsed_values, index=df.index, dtype=object)


def load_notion_roadmap(csv_path: Path) -> pd.DataFrame:
    """Loads the Notion roadmap export at |csv_path| into a DataFrame with
    every column renamed to a BigQuery-safe identifier, plus a derived
    LINEAR_PROJECT_SLUG_ID_COLUMN extracted from the Linear Project URL
    column and the start and exclusive end dates of the Working Period column.
    Prints the original-to-renamed column mapping so column drift in
    future exports is visible.
    """
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)

    renamed = _dedupe_column_names([_to_bq_safe_column_name(col) for col in df.columns])
    rename_map = dict(zip(df.columns, renamed))
    print("Renamed Notion columns:")
    for original, new in rename_map.items():
        if original != new:
            print(f"  {original!r} -> {new!r}")
    df = df.rename(columns=rename_map)

    for expected_column in (
        NOTION_LINEAR_PROJECT_URL_COLUMN,
        NOTION_WORKING_PERIOD_COLUMN,
    ):
        if expected_column not in df.columns:
            raise ValueError(
                f"Expected a [{expected_column}] column after renaming, got "
                f"columns: {list(df.columns)}"
            )
    df[LINEAR_PROJECT_SLUG_ID_COLUMN] = df[NOTION_LINEAR_PROJECT_URL_COLUMN].map(
        _extract_slug_id
    )

    working_periods = [
        _parse_working_period(value) for value in df[NOTION_WORKING_PERIOD_COLUMN]
    ]
    df[NOTION_WORKING_PERIOD_START_DATE_COLUMN] = pd.to_datetime(
        [start for start, _ in working_periods]
    )
    df[NOTION_WORKING_PERIOD_END_DATE_EXCLUSIVE_COLUMN] = pd.to_datetime(
        [end for _, end in working_periods]
    )

    return df


def get_notion_roadmap_snapshots_source_table_config() -> SourceTableConfig:
    """Returns the SourceTableConfig for the
    notion_snapshots.notion_roadmap_snapshots table, as defined in
    notion_roadmap_snapshots.yaml.
    """
    source_table_repository = build_source_table_repository_for_yaml_managed_tables(
        project_id=None
    )
    return source_table_repository.get_config(NOTION_ROADMAP_SNAPSHOTS_ADDRESS)


def upload_notion_roadmap_snapshot(df: pd.DataFrame, *, upload_to_gbq: bool) -> None:
    """Appends df to the notion_snapshots.notion_roadmap_snapshots table in
    every data platform project, after stamping the snapshot date and upload
    datetime and validating its columns against the registered schema. If
    upload_to_gbq is False, only validates.
    """
    table_config = get_notion_roadmap_snapshots_source_table_config()
    df_copy = df.copy()
    parse_notion_date_columns(df_copy, table_config=table_config)
    df_copy[SNAPSHOT_DATE_COLUMN] = datetime.date.today().isoformat()
    df_copy[UPLOAD_DATETIME_COLUMN] = pd.to_datetime("today")

    validate_and_convert_df_columns_to_schema(df_copy, table_config)

    if not upload_to_gbq:
        print(f"Validated [{len(df_copy)}] rows, but did not upload them to BigQuery.")
        return

    append_df_to_source_table_in_all_projects(df_copy, table_config=table_config)


def run(*, upload_to_gbq: bool) -> pd.DataFrame:
    """Loads the latest Notion roadmap export from ~/Downloads, uploads it as a
    snapshot (if upload_to_gbq), and returns the loaded DataFrame either way.
    """
    csv_path = find_latest_notion_csv()
    print(f"Loading Notion roadmap from [{csv_path}]")
    notion_df = load_notion_roadmap(csv_path)
    upload_notion_roadmap_snapshot(notion_df, upload_to_gbq=upload_to_gbq)
    return notion_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    arg_parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    arg_parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs in dry-run mode, only validating the rows against the "
        "registered schema, without uploading them to BigQuery.",
    )
    args = arg_parser.parse_args()

    run(upload_to_gbq=not args.dry_run)
