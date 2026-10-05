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
"""Builds and writes snapshots of every Linear project to the
linear_snapshots.linear_project_snapshots BigQuery table.

Each snapshot has one row per Linear project, with the project's status, dates,
health, launch-stage milestone dates, and labels. For example, a project with
"TT Launch" and "FSL" milestones, the States/US_XX and Products/Meetings labels,
and status TT becomes a row like (columns abridged):

    linear_project_name: "[US_XX] Meetings"
    linear_project_status: "TT"
    tt_date: "2026-09-14"
    fsl_date: "2026-11-02"
    linear_state_label: "US_XX"
    linear_product_label: "Meetings"
    snapshot_date: "2026-10-01"
"""
import datetime
import re
from enum import Enum

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.issue_tracking.roadmap_snapshot_tables import (
    LINEAR_PROJECT_SLUG_ID_COLUMN,
    LINEAR_PROJECT_SNAPSHOTS_ADDRESS,
    SNAPSHOT_DATE_COLUMN,
    UPLOAD_DATETIME_COLUMN,
)

LINEAR_PROJECT_ID_COLUMN = "linear_project_id"
LINEAR_PROJECT_NAME_COLUMN = "linear_project_name"
LINEAR_PROJECT_STATE_COLUMN = "linear_project_state"
LINEAR_PROJECT_STATUS_COLUMN = "linear_project_status"
LINEAR_PROJECT_START_DATE_COLUMN = "linear_project_start_date"
LINEAR_PROJECT_TARGET_DATE_COLUMN = "linear_project_target_date"
LINEAR_PROJECT_HEALTH_COLUMN = "linear_project_health"
LINEAR_PROJECT_HEALTH_UPDATED_AT_COLUMN = "linear_project_health_updated_at"
LINEAR_STATE_LABEL_COLUMN = "linear_state_label"
LINEAR_PRODUCT_LABEL_COLUMN = "linear_product_label"
LINEAR_ENGINEERING_SIZE_LABEL_COLUMN = "linear_engineering_size_label"
LINEAR_LABELS_COLUMN = "linear_labels"

# Linear project label group names, as configured in the Recidiviz workspace.
# Linear allows at most one label per group on a project.
LINEAR_LABEL_GROUP_COLUMNS = {
    "States": LINEAR_STATE_LABEL_COLUMN,
    "Products": LINEAR_PRODUCT_LABEL_COLUMN,
    "Engineering Size": LINEAR_ENGINEERING_SIZE_LABEL_COLUMN,
}

_WRITE_TIMEOUT_SECONDS = 300


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
    """Returns the single LaunchStage that the milestone name represents, or
    None if no stage's keyword pattern matches, case-insensitively.

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
    """Returns the milestone that best represents stage, or None if no
    milestone matches.

    A project can accumulate more than one milestone matching the same stage
    over its life (e.g. a duplicated or renamed milestone). Among matches,
    this prefers the one with the earliest targetDate; if none have a
    targetDate, it takes the first one in Linear's returned order.
    """
    matches = [m for m in milestones if classify_milestone_stage(m["name"]) is stage]
    if not matches:
        return None

    dated_matches = [m for m in matches if m["targetDate"]]
    if dated_matches:
        return min(dated_matches, key=lambda m: m["targetDate"])
    return matches[0]


def derive_launch_stage_dates(project: dict) -> dict[str, str | None]:
    """Returns the "{stage}_date" and "{stage}_milestone_name" columns for each
    launch stage, from the project's milestones.

    A stage with no matching milestone gets null columns rather than falling
    back to the project's own targetDate, which doesn't say which stage it
    targets.
    """
    milestones = project["milestones"]["nodes"]
    result: dict[str, str | None] = {}
    for stage in LaunchStage:
        milestone = pick_stage_milestone(milestones, stage=stage)
        result[LAUNCH_STAGE_DATE_COLUMN[stage]] = (
            milestone["targetDate"] if milestone else None
        )
        result[LAUNCH_STAGE_MILESTONE_NAME_COLUMN[stage]] = (
            milestone["name"] if milestone else None
        )
    return result


def derive_label_columns(project: dict) -> dict[str, str | None]:
    """Returns the project's label columns: one column per group in
    LINEAR_LABEL_GROUP_COLUMNS holding that group's label name, plus
    LINEAR_LABELS_COLUMN listing every label on the project.

    For example, a project labeled States/US_XX, Products/Meetings, and the
    ungrouped H2 2026 label produces:

        linear_state_label: "US_XX"
        linear_product_label: "Meetings"
        linear_engineering_size_label: None
        linear_labels: "H2 2026, Products/Meetings, States/US_XX"
    """
    labels = project["labels"]["nodes"]
    result: dict[str, str | None] = dict.fromkeys(LINEAR_LABEL_GROUP_COLUMNS.values())
    for label in labels:
        if not label["parent"]:
            continue
        group_name = label["parent"]["name"]
        # Labels in groups other than LINEAR_LABEL_GROUP_COLUMNS' (e.g. a
        # planning-cycle group) only appear in LINEAR_LABELS_COLUMN.
        if group_name not in LINEAR_LABEL_GROUP_COLUMNS:
            continue
        column = LINEAR_LABEL_GROUP_COLUMNS[group_name]
        if result[column] is not None:
            raise ValueError(
                f"Linear project [{project['name']}] has more than one label in "
                f"group [{group_name}]: [{result[column]}] and [{label['name']}]. "
                "Remove the extra label in Linear and rerun."
            )
        result[column] = label["name"]

    label_names = sorted(
        f"{label['parent']['name']}/{label['name']}"
        if label["parent"]
        else label["name"]
        for label in labels
    )
    result[LINEAR_LABELS_COLUMN] = ", ".join(label_names) if label_names else None
    return result


def build_linear_project_snapshot_rows(
    linear_projects: list[dict], *, snapshot_datetime: datetime.datetime
) -> list[dict[str, str | None]]:
    """Returns one linear_project_snapshots row per project in linear_projects
    (as returned by LinearClient.get_all_projects_with_milestones), stamped
    with snapshot_datetime.
    """
    rows = []
    for project in linear_projects:
        row: dict[str, str | None] = {
            LINEAR_PROJECT_ID_COLUMN: project["id"],
            LINEAR_PROJECT_SLUG_ID_COLUMN: project["slugId"],
            LINEAR_PROJECT_NAME_COLUMN: project["name"],
            LINEAR_PROJECT_STATE_COLUMN: project["state"],
            LINEAR_PROJECT_STATUS_COLUMN: project["status"]["name"],
            LINEAR_PROJECT_START_DATE_COLUMN: project["startDate"],
            LINEAR_PROJECT_TARGET_DATE_COLUMN: project["targetDate"],
            LINEAR_PROJECT_HEALTH_COLUMN: project["health"],
            LINEAR_PROJECT_HEALTH_UPDATED_AT_COLUMN: project["healthUpdatedAt"],
        }
        row.update(derive_launch_stage_dates(project))
        row.update(derive_label_columns(project))
        row[SNAPSHOT_DATE_COLUMN] = snapshot_datetime.date().isoformat()
        row[UPLOAD_DATETIME_COLUMN] = snapshot_datetime.isoformat()
        rows.append(row)
    return rows


def write_linear_project_snapshot(
    *, bq_client: BigQueryClient, rows: list[dict[str, str | None]]
) -> None:
    """Appends rows to the linear_snapshots.linear_project_snapshots table in
    bq_client's project and waits for the load to finish.
    """
    if not rows:
        raise ValueError(
            "No Linear projects to snapshot; the Linear API returned none. Check "
            "that the API key can see the workspace's teams."
        )
    load_job = bq_client.load_into_table_async(
        address=LINEAR_PROJECT_SNAPSHOTS_ADDRESS, rows=rows
    )
    load_job.result(timeout=_WRITE_TIMEOUT_SECONDS)
