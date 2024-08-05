"""Applies relevant row-level permissions to all tables in the given project. 
Will overwrite all existing row-level permissions.
Usage:
    python -m recidiviz.tools.deploy.oneoffs.apply_row_level_permissions_to_all_tables --project_id recidiviz-staging
"""
import argparse
import logging
from typing import Optional

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.tools.utils.bigquery_helpers import (
    dataset_prefix_to_filter_regex,
    run_operation_for_tables_in_datasets,
)
from recidiviz.utils.environment import GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


def apply_row_level_permissions_to_all_tables(dataset_prefix: Optional[str]) -> None:
    def _apply_permissions_for_table(
        client: BigQueryClient, address: ProjectSpecificBigQueryAddress
    ) -> None:
        table = client.get_table(address=address.to_project_agnostic_address())
        client.apply_row_level_permissions(table)

    run_operation_for_tables_in_datasets(
        client=BigQueryClientImpl(),
        prompt="Apply row level permissions",
        operation=_apply_permissions_for_table,
        dataset_filter=dataset_prefix_to_filter_regex(dataset_prefix)
        if dataset_prefix
        else None,
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--project_id",
        help="Which project to apply permissions to.",
        type=str,
        choices=GCP_PROJECTS,
        required=True,
    )

    parser.add_argument(
        "--dataset-prefix",
        help="Optionally, only apply permissions to datasets matching this prefix",
        type=str,
        required=False,
    )

    args = parser.parse_args()

    with local_project_id_override(args.project_id):
        apply_row_level_permissions_to_all_tables(args.dataset_prefix)
