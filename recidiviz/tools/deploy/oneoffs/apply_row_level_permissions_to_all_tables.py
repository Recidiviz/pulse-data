"""Applies relevant row-level permissions to all tables in the given project. 
Will overwrite all existing row-level permissions.
Usage:
    python -m recidiviz.tools.deploy.oneoffs.apply_row_level_permissions_to_all_tables --project_id recidiviz-staging
"""
import argparse
import logging
from typing import Optional

from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.tools.utils.bigquery_helpers import run_operation_for_tables
from recidiviz.utils.environment import GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


def apply_row_level_permissions_to_all_tables(dataset_prefix: Optional[str]) -> None:
    def _apply_permissions_for_table(
        client: BigQueryClient, dataset_id: str, table_id: str
    ) -> None:
        dataset_ref = client.dataset_ref_for_id(dataset_id)
        table = client.get_table(dataset_ref=dataset_ref, table_id=table_id)
        client.apply_row_level_permissions(table)

    run_operation_for_tables(
        client=BigQueryClientImpl(),
        prompt="Apply row level permissions",
        operation=_apply_permissions_for_table,
        dataset_prefix=dataset_prefix,
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
