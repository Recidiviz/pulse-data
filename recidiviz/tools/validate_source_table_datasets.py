# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Script to locally validate that source table datasets in BigQuery contain
exactly the tables we expect based on source table YAML configs.

Run this after adding/removing YAML configs or cleaning up BQ tables to confirm
the validation will pass in the deployed DAGs. Each DAG's deployed
validate_source_table_datasets task validates one update group at a time (the
group owned by the DAG it runs in), so this script validates each group's tables
independently. Pass --update-groups to validate only a subset of groups.

Usage:
    python -m recidiviz.tools.validate_source_table_datasets \
        --project-id recidiviz-staging \
        [--update-groups CALC IDENTITY_INGEST]
"""
import argparse
import logging

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.source_tables.source_table_cleanup_validation import (
    validate_clean_source_table_datasets,
)
from recidiviz.source_tables.source_table_config import SourceTableUpdateGroup
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.view_registry.deployed_source_table_repository import (
    build_source_table_repository_for_collected_schemata,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate that source table datasets in BigQuery match YAML configs."
    )
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    parser.add_argument(
        "--update-groups",
        nargs="+",
        choices=[group.value for group in SourceTableUpdateGroup],
        help="Only validate these update groups' tables. Defaults to validating "
        "every update group.",
    )
    return parser.parse_args()


def main(*, project_id: str, update_groups: list[SourceTableUpdateGroup]) -> None:
    """Validates each update group's source-table datasets independently, matching how
    each DAG validates only its own group. Aggregates per-group failures so one failing
    group does not hide the others.
    """
    with local_project_id_override(project_id):
        source_table_repository = build_source_table_repository_for_collected_schemata(
            project_id=project_id,
        )
        bq_client = BigQueryClientImpl()

        failures_by_group: dict[SourceTableUpdateGroup, str] = {}
        for group in update_groups:
            logging.info("Validating source tables for update group [%s]", group.value)
            try:
                validate_clean_source_table_datasets(
                    bq_client=bq_client,
                    source_table_repository=source_table_repository.filter_to_update_group(
                        group
                    ),
                )
            except ValueError as e:
                # Collect each group's validation error so one failing group does not
                # hide failures in the others.
                failures_by_group[group] = str(e)

    if failures_by_group:
        raise ValueError(
            "\n\n".join(
                f"Update group [{group.value}]:\n{message}"
                for group, message in failures_by_group.items()
            )
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    args = parse_args()

    main(
        project_id=args.project_id,
        update_groups=(
            [SourceTableUpdateGroup(group) for group in args.update_groups]
            if args.update_groups
            else list(SourceTableUpdateGroup)
        ),
    )
