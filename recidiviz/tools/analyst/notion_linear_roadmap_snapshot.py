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
"""Combines the Notion state-initiative roadmap with Linear project and
milestone data and uploads the result as a snapshot row to BigQuery.

Notion API access is blocked on our current plan, so the roadmap side of the
join comes from a manually-downloaded CSV export sitting in ~/Downloads.
Linear data is fetched live via the Linear GraphQL API and joined to the
Notion rows by the slugId embedded in each initiative's "Linear Project" URL.

Run manually via
`python -m recidiviz.tools.analyst.notion_linear_roadmap_snapshot --dry-run False`,
or call `run(...)` directly from a notebook. Defaults to dry-run mode, which
validates the joined DataFrame against the registered schema without
uploading it to BigQuery.
"""
import argparse
import datetime
import glob
import logging
import re
import zipfile
from enum import Enum
from pathlib import Path

import pandas as pd

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import normalize_column_name_for_bq
from recidiviz.issue_tracking.linear.linear_client import (
    linear_client_from_secret_or_env,
)
from recidiviz.source_tables.source_table_config import SourceTableConfig
from recidiviz.source_tables.yaml_managed.collect_yaml_managed_source_table_configs import (
    build_source_table_repository_for_yaml_managed_tables,
)
from recidiviz.source_tables.yaml_managed.datasets import LINEAR_SNAPSHOTS_DATASET
from recidiviz.tools.analyst.source_table_upload_utils import (
    validate_and_convert_df_columns_to_schema,
)
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool

logger = logging.getLogger(__name__)

ROADMAP_TABLE_ID = "notion_linear_roadmap_snapshots"

NOTION_ROADMAP_CSV_GLOB_PATTERNS = ("*State Initiative Roadmap*_all.csv",)

LINEAR_PROJECT_URL_COLUMN = "linear_project"
LINEAR_PROJECT_SLUG_ID_COLUMN = "linear_project_slug_id"
LINEAR_PROJECT_ID_COLUMN = "linear_project_id"
LINEAR_PROJECT_NAME_COLUMN = "linear_project_name"
LINEAR_PROJECT_STATE_COLUMN = "linear_project_state"
LINEAR_PROJECT_START_DATE_COLUMN = "linear_project_start_date"
LINEAR_PROJECT_TARGET_DATE_COLUMN = "linear_project_target_date"
LINEAR_PROJECT_HEALTH_COLUMN = "linear_project_health"
LINEAR_PROJECT_HEALTH_UPDATED_AT_COLUMN = "linear_project_health_updated_at"

SNAPSHOT_DATE_COLUMN = "snapshot_date"
UPLOAD_DATETIME_COLUMN = "upload_datetime"


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


def load_notion_roadmap(csv_path: Path) -> pd.DataFrame:
    """Loads the Notion roadmap export at |csv_path| into a DataFrame with
    every column renamed to a BigQuery-safe identifier, plus a derived
    |LINEAR_PROJECT_SLUG_ID_COLUMN| extracted from the Linear Project URL
    column. Prints the original-to-renamed column mapping so column drift in
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

    if LINEAR_PROJECT_URL_COLUMN not in df.columns:
        raise ValueError(
            f"Expected a [{LINEAR_PROJECT_URL_COLUMN}] column after renaming, "
            f"got columns: {list(df.columns)}"
        )
    df[LINEAR_PROJECT_SLUG_ID_COLUMN] = df[LINEAR_PROJECT_URL_COLUMN].map(
        _extract_slug_id
    )

    return df


class LaunchStage(Enum):
    """Launch stages in chronological rollout order: core scoping completes,
    then a trusted tester (TT) launch, then a partial launch, then a full
    state launch (FSL).
    """

    CORE_SCOPING_COMPLETE = "core_scoping_complete"
    TT = "tt"
    PARTIAL = "partial"
    FSL = "fsl"


LAUNCH_STAGE_PATTERNS: dict[LaunchStage, re.Pattern[str]] = {
    LaunchStage.CORE_SCOPING_COMPLETE: re.compile(r"scoping complete"),
    LaunchStage.TT: re.compile(r"\btt\b|trusted tester"),
    LaunchStage.PARTIAL: re.compile(r"\bpilot\b|\bpartial\b"),
    LaunchStage.FSL: re.compile(r"\bfsl\b|full state launch|\bfs launch\b|full state"),
}

LAUNCH_STAGE_DATE_COLUMN = {stage: f"{stage.value}_date" for stage in LaunchStage}
LAUNCH_STAGE_MILESTONE_NAME_COLUMN = {
    stage: f"{stage.value}_milestone_name" for stage in LaunchStage
}


def classify_milestone_stage(name: str) -> LaunchStage | None:
    """Returns the single LaunchStage that |name| represents, or None if no
    stage's keyword pattern matches, case-insensitively.

    A milestone name can match more than one stage's pattern (e.g. "TT to
    FSL"); a single Linear milestone must not count for more than one launch
    stage in BigQuery, so this returns the earliest-in-rollout-order match
    (LaunchStage's own iteration order: CORE_SCOPING_COMPLETE, then TT, then
    PARTIAL, then FSL).
    """
    lowered = name.lower()
    for stage, pattern in LAUNCH_STAGE_PATTERNS.items():
        if pattern.search(lowered):
            return stage
    return None


def pick_stage_milestone(milestones: list[dict], *, stage: LaunchStage) -> dict | None:
    """Returns the milestone from |milestones| that best represents |stage|,
    or None if no milestone matches.

    A project can accumulate more than one milestone matching the same stage
    over its life (e.g. a duplicated or renamed milestone). Among matches,
    this prefers the one with the earliest targetDate; if none have a
    targetDate, it takes the first one in Linear's returned order.
    """
    matches = [m for m in milestones if classify_milestone_stage(m["name"]) is stage]
    if not matches:
        return None

    dated_matches = [m for m in matches if m.get("targetDate")]
    if dated_matches:
        return min(dated_matches, key=lambda m: m["targetDate"])
    return matches[0]


def derive_launch_stage_dates(project: dict) -> dict[str, str | None]:
    """Returns a flat dict of "{stage}_date"/"{stage}_milestone_name" fields
    for each launch stage, derived from |project|'s milestones, plus
    |LINEAR_PROJECT_START_DATE_COLUMN|, |LINEAR_PROJECT_TARGET_DATE_COLUMN|,
    |LINEAR_PROJECT_HEALTH_COLUMN|, and
    |LINEAR_PROJECT_HEALTH_UPDATED_AT_COLUMN| from the project's own
    startDate/targetDate/health/healthUpdatedAt.

    A stage with no matching milestone gets null fields rather than falling
    back to the project-level targetDate -- a bare project-level date doesn't
    say which stage it targets.
    """
    milestones = project.get("milestones", {}).get("nodes", [])
    result: dict[str, str | None] = {
        LINEAR_PROJECT_START_DATE_COLUMN: project.get("startDate"),
        LINEAR_PROJECT_TARGET_DATE_COLUMN: project.get("targetDate"),
        LINEAR_PROJECT_HEALTH_COLUMN: project.get("health"),
        LINEAR_PROJECT_HEALTH_UPDATED_AT_COLUMN: project.get("healthUpdatedAt"),
    }
    for stage in LaunchStage:
        milestone = pick_stage_milestone(milestones, stage=stage)
        result[LAUNCH_STAGE_DATE_COLUMN[stage]] = (
            milestone["targetDate"] if milestone else None
        )
        result[LAUNCH_STAGE_MILESTONE_NAME_COLUMN[stage]] = (
            milestone["name"] if milestone else None
        )
    return result


def build_slug_id_index(linear_projects: list[dict]) -> dict[str, dict]:
    """Returns a dict mapping each Linear project's slugId to the project.

    Logs a warning (rather than raising) if two projects share a slugId,
    since that would silently misjoin Notion rows onto the wrong project;
    the later project in |linear_projects| wins.
    """
    index: dict[str, dict] = {}
    for project in linear_projects:
        slug_id = project["slugId"]
        if slug_id in index:
            logger.warning(
                "Duplicate Linear slugId [%s] for projects [%s] and [%s]",
                slug_id,
                index[slug_id]["name"],
                project["name"],
            )
        index[slug_id] = project
    return index


def _linear_columns_for_slug_id(
    slug_id: str | None, *, slug_id_index: dict[str, dict]
) -> dict[str, str | None]:
    """Returns the Linear project and derived launch-stage columns for
    |slug_id|, or all-null columns if |slug_id| is None or unmatched.
    """
    project = slug_id_index.get(slug_id) if slug_id else None
    if project is None:
        columns: dict[str, str | None] = {
            LINEAR_PROJECT_ID_COLUMN: None,
            LINEAR_PROJECT_NAME_COLUMN: None,
            LINEAR_PROJECT_STATE_COLUMN: None,
        }
        columns.update(derive_launch_stage_dates({}))
        return columns

    columns = {
        LINEAR_PROJECT_ID_COLUMN: project["id"],
        LINEAR_PROJECT_NAME_COLUMN: project["name"],
        LINEAR_PROJECT_STATE_COLUMN: project["state"],
    }
    columns.update(derive_launch_stage_dates(project))
    return columns


def join_notion_and_linear(
    notion_df: pd.DataFrame, linear_projects: list[dict]
) -> pd.DataFrame:
    """Left-joins |notion_df| with |linear_projects| on slugId, adding
    Linear project fields and derived launch-stage date fields. Notion rows
    with no matching Linear project (or a blank slugId) keep all Notion
    columns with null Linear/stage columns.
    """
    slug_id_index = build_slug_id_index(linear_projects)

    unmatched_slug_ids = {
        slug_id
        for slug_id in notion_df[LINEAR_PROJECT_SLUG_ID_COLUMN]
        if slug_id and slug_id not in slug_id_index
    }
    if unmatched_slug_ids:
        logger.warning("No Linear project found for slugId(s): %s", unmatched_slug_ids)

    linear_columns_df = pd.DataFrame(
        [
            _linear_columns_for_slug_id(slug_id, slug_id_index=slug_id_index)
            for slug_id in notion_df[LINEAR_PROJECT_SLUG_ID_COLUMN]
        ],
        index=notion_df.index,
    )
    return pd.concat([notion_df, linear_columns_df], axis=1)


def get_roadmap_snapshot_source_table_config() -> SourceTableConfig:
    """Returns the SourceTableConfig for the
    linear_snapshots.notion_linear_roadmap_snapshots table, as defined in
    notion_linear_roadmap_snapshots.yaml.
    """
    table_address = BigQueryAddress(
        dataset_id=LINEAR_SNAPSHOTS_DATASET,
        table_id=ROADMAP_TABLE_ID,
    )
    source_table_repository = build_source_table_repository_for_yaml_managed_tables(
        project_id=None
    )
    return source_table_repository.get_config(table_address)


def upload_roadmap_snapshot_to_bq(
    df: pd.DataFrame, *, upload_to_gbq: bool = False
) -> None:
    """Appends |df| to the
    linear_snapshots.notion_linear_roadmap_snapshots table,
    after stamping |SNAPSHOT_DATE_COLUMN| and |UPLOAD_DATETIME_COLUMN| and
    validating its columns against the registered schema.

    If |upload_to_gbq| is False, the DataFrame is only validated, not
    uploaded.
    """
    table_config = get_roadmap_snapshot_source_table_config()
    df_copy = df.copy()
    df_copy[SNAPSHOT_DATE_COLUMN] = datetime.date.today().isoformat()
    df_copy[UPLOAD_DATETIME_COLUMN] = pd.to_datetime("today")

    validate_and_convert_df_columns_to_schema(df_copy, table_config)

    num_rows = len(df_copy)
    if not upload_to_gbq:
        print("DataFrame was validated, but not uploaded to BigQuery.")
        return

    for project_id in DATA_PLATFORM_GCP_PROJECTS:
        project_specific_address = table_config.address.to_project_specific_address(
            project_id
        )
        print(
            f"Uploading [{num_rows}] rows to `{project_specific_address.to_str()}`..."
        )
        df_copy.to_gbq(
            destination_table=table_config.address.to_str(),
            project_id=project_id,
            table_schema=[col.to_api_repr() for col in table_config.schema_fields],
            if_exists="append",
        )
        print(
            f"Done uploading [{num_rows}] rows to `{project_specific_address.to_str()}`."
        )


def run(*, upload_to_gbq: bool = False) -> pd.DataFrame:
    """Finds the latest Notion roadmap export, joins it with live Linear
    project/milestone data, uploads the result as a snapshot row to
    BigQuery (if |upload_to_gbq|), and returns the resulting DataFrame
    either way.
    """
    csv_path = find_latest_notion_csv()
    print(f"Loading Notion roadmap from [{csv_path}]")
    notion_df = load_notion_roadmap(csv_path)

    # linear_client_from_secret_or_env() resolves a GCP project (to look up
    # the Linear API key in Secret Manager) via metadata.project_id(), which
    # raises when running locally with no project override in place, rather
    # than falling through to the LINEAR_API_KEY environment variable
    # fallback -- so a project must be set explicitly, as in
    # load_views_to_sandbox.py.
    with local_project_id_override(GCP_PROJECT_STAGING):
        linear_client = linear_client_from_secret_or_env()
        linear_projects = linear_client.get_all_projects_with_milestones()

    joined_df = join_notion_and_linear(notion_df, linear_projects)
    upload_roadmap_snapshot_to_bq(joined_df, upload_to_gbq=upload_to_gbq)
    return joined_df


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    arg_parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    arg_parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs in dry-run mode, only validating the joined DataFrame "
        "against the registered schema, without uploading it to BigQuery.",
    )
    args = arg_parser.parse_args()

    run(upload_to_gbq=not args.dry_run)
