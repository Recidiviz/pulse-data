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
"""Writes a snapshot of every Linear project to the
linear_snapshots.linear_project_snapshots table in staging and production.

A scheduled job will take this over once there's a Linear API key that can see
every team; until then, run it manually via
`python -m recidiviz.tools.analyst.linear_project_snapshot --dry-run False`.
Uses the personal Linear API key in LINEAR_API_KEY
(https://linear.app/settings/api) if it's set, and otherwise the staging Secret
Manager key, which misses projects on teams outside its scope. Defaults to
dry-run mode, which fetches and builds the rows without writing them.
"""
import argparse
import datetime
import logging

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.issue_tracking.linear.linear_client import (
    linear_client_from_env_or_secret,
)
from recidiviz.issue_tracking.linear.linear_project_snapshot import (
    build_linear_project_snapshot_rows,
    write_linear_project_snapshot,
)
from recidiviz.issue_tracking.roadmap_snapshot_tables import (
    LINEAR_PROJECT_SNAPSHOTS_ADDRESS,
)
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.params import str_to_bool


def run(*, upload_to_gbq: bool) -> None:
    """Fetches every Linear project and, if upload_to_gbq, appends one snapshot
    row per project to linear_project_snapshots in every data platform project.
    """
    # The Secret Manager fallback looks up the key in the current GCP project,
    # which a local run doesn't have unless one is set.
    with local_project_id_override(GCP_PROJECT_STAGING):
        linear_client = linear_client_from_env_or_secret()
    linear_projects = linear_client.get_all_projects_with_milestones()
    rows = build_linear_project_snapshot_rows(
        linear_projects, snapshot_datetime=datetime.datetime.now(datetime.timezone.utc)
    )
    if not upload_to_gbq:
        print(f"Built [{len(rows)}] rows, but did not write them to BigQuery.")
        return

    for project_id in DATA_PLATFORM_GCP_PROJECTS:
        address = LINEAR_PROJECT_SNAPSHOTS_ADDRESS.to_project_specific_address(
            project_id
        )
        print(f"Writing [{len(rows)}] rows to `{address.to_str()}`...")
        write_linear_project_snapshot(
            bq_client=BigQueryClientImpl(project_id=project_id), rows=rows
        )
        print(f"Done writing [{len(rows)}] rows to `{address.to_str()}`.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    arg_parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    arg_parser.add_argument(
        "--dry-run",
        default=True,
        type=str_to_bool,
        help="Runs in dry-run mode, fetching projects and building rows without "
        "writing them to BigQuery.",
    )
    args = arg_parser.parse_args()

    run(upload_to_gbq=not args.dry_run)
