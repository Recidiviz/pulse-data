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
"""
A script to delete user data from a specific state in a dataset. The tables must have a
column that stores a user id value that can be connected to the
`workflows_views.reidentified_dashboard_users` table (this column name can be specified with the
--user_hash_field command-line argument).

Usage:
python -m recidiviz.tools.delete_user_data_from_dataset_by_state \
    --project_id recidiviz-staging \
    --dataset pulse_dashboard_segment_metrics \
    --state_code US_OR
"""

import argparse
import logging
from typing import Callable

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.tools.utils.bigquery_helpers import (
    dataset_id_to_filter_regex,
    run_operation_for_tables_in_datasets,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def delete_data(
    state_code: str, user_hash_field: str
) -> Callable[[BigQueryClient, ProjectSpecificBigQueryAddress], None]:
    """Return a function that deletes rows for users with a specific state_code in a given table
    by looking them up by user_hash."""

    def inner_fn(
        client: BigQueryClient, address: ProjectSpecificBigQueryAddress
    ) -> None:
        table = client.get_table(address.to_project_agnostic_address())
        if table.table_type != "TABLE":
            return
        if not any(field.name == user_hash_field for field in table.schema):
            logging.info(
                "Skipping table %s with no %s column", address.to_str(), user_hash_field
            )
            return

        query_job = client.run_query_async(
            query_str=f"""
DELETE FROM {address.format_address_for_query()} t
WHERE t.{user_hash_field} IN (
    SELECT user_id FROM `recidiviz-staging.workflows_views.reidentified_dashboard_users_materialized`
    WHERE state_code="{state_code}"
    UNION ALL
    SELECT user_id FROM `recidiviz-123.workflows_views.reidentified_dashboard_users_materialized`
    WHERE state_code="{state_code}"
)
""",
            use_query_cache=False,
        )
        logging.info("Deleting rows from %s", address.to_str())
        client.wait_for_big_query_jobs([query_job])

    return inner_fn


def create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--project_id",
        type=str,
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    parser.add_argument(
        "--dataset",
        type=str,
        required=True,
    )

    parser.add_argument(
        "--state_code",
        type=str,
        required=True,
    )

    parser.add_argument("--user_hash_field", type=str, default="user_id")

    return parser


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = create_parser().parse_args()

    with local_project_id_override(args.project_id):
        run_operation_for_tables_in_datasets(
            client=BigQueryClientImpl(),
            prompt=f"Delete data from {args.state_code}",
            operation=delete_data(args.state_code, args.user_hash_field),
            dataset_filter=dataset_id_to_filter_regex(args.dataset),
        )
