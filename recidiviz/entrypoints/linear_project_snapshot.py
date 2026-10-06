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
"""Entrypoint that writes a daily snapshot of every Linear project to the
linear_snapshots.linear_project_snapshots table in the current GCP project.

Runs as the linear-project-snapshot Cloud Run job, started daily by Cloud
Scheduler.
"""
import argparse
import datetime
import logging

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.entrypoints.entrypoint_interface import EntrypointInterface
from recidiviz.issue_tracking.linear.linear_client import (
    LinearClient,
    get_linear_oauth_access_token,
)
from recidiviz.issue_tracking.linear.linear_project_snapshot import (
    build_linear_project_snapshot_rows,
    write_linear_project_snapshot,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING, in_development
from recidiviz.utils.metadata import set_development_project_id_override
from recidiviz.utils.secrets import get_secret

# Secret Manager secrets holding the client ID and client secret of the "Launch
# Momentum Snapshot" Linear OAuth application, which must be able to read every
# team; an application scoped to fewer teams silently drops those teams'
# projects. Terraform creates the secrets empty
# (cloud-run-linear-project-snapshot.tf), and the values are added as secret
# versions by hand.
LINEAR_OAUTH_CLIENT_ID_SECRET_NAME = "linear_project_snapshot_oauth_client_id"  # nosec
LINEAR_OAUTH_CLIENT_SECRET_SECRET_NAME = (
    "linear_project_snapshot_oauth_client_secret"  # nosec
)


class LinearProjectSnapshot(EntrypointInterface):
    """Entrypoint that writes a daily snapshot of every Linear project to the
    linear_snapshots.linear_project_snapshots table in the current GCP project."""

    @staticmethod
    def get_parser() -> argparse.ArgumentParser:
        return argparse.ArgumentParser()

    @staticmethod
    def run_entrypoint(*, args: argparse.Namespace) -> None:
        """Fetches every Linear project and appends one snapshot row per project."""
        del args
        client_id = get_secret(LINEAR_OAUTH_CLIENT_ID_SECRET_NAME)
        client_secret = get_secret(LINEAR_OAUTH_CLIENT_SECRET_SECRET_NAME)
        if not client_id or not client_secret:
            raise ValueError(
                f"Secrets [{LINEAR_OAUTH_CLIENT_ID_SECRET_NAME}] and "
                f"[{LINEAR_OAUTH_CLIENT_SECRET_SECRET_NAME}] must both have values. "
                "Add the Launch Momentum Snapshot Linear OAuth application's client "
                "ID and client secret as new versions of those secrets in this "
                "project."
            )
        access_token = get_linear_oauth_access_token(
            client_id=client_id, client_secret=client_secret, scope="read"
        )
        linear_projects = LinearClient(
            f"Bearer {access_token}"
        ).get_all_projects_with_milestones()
        rows = build_linear_project_snapshot_rows(
            linear_projects,
            snapshot_datetime=datetime.datetime.now(datetime.timezone.utc),
        )
        write_linear_project_snapshot(bq_client=BigQueryClientImpl(), rows=rows)
        logging.info("Wrote [%s] Linear project snapshot rows.", len(rows))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    if in_development():
        set_development_project_id_override(GCP_PROJECT_STAGING)

    LinearProjectSnapshot.run_entrypoint(
        args=LinearProjectSnapshot.get_parser().parse_args()
    )
