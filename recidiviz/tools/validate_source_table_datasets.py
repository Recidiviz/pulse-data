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
the validation will pass in the deployed entrypoint.

Usage:
    python -m recidiviz.tools.validate_source_table_datasets \
        --project-id recidiviz-staging
"""
import argparse
import logging

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.source_table_cleanup_validation import (
    validate_clean_source_table_datasets,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate that source table datasets in BigQuery match YAML configs."
    )
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    args = parse_args()

    with local_project_id_override(args.project_id):
        source_table_repository = build_source_table_repository_for_collected_schemata(
            project_id=args.project_id,
        )

        bq_client = BigQueryClientImpl()

        validate_clean_source_table_datasets(
            bq_client=bq_client,
            source_table_repository=source_table_repository,
        )
