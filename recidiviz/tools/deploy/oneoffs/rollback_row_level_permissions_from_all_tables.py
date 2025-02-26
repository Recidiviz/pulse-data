"""As part of the roll-out plan, we have a script that will iterate through every table 
applying these row-level permissions. Should we find that row-level permissions are 
causing an issue, this script is intended to rollback all row-level permissions. 
This script will drop row-level permissions on ALL tables in the given project.
Usage:
    python -m recidiviz.tools.deploy.oneoffs.rollback_row_level_permissions_from_all_tables --project_id recidiviz-staging
"""

import argparse
import logging
from typing import Optional

from recidiviz.big_query.big_query_client import BigQueryClient, BigQueryClientImpl
from recidiviz.tools.utils.bigquery_helpers import run_operation_for_tables
from recidiviz.utils.environment import GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


def rollback_row_level_permissions_on_all_tables(dataset_prefix: Optional[str]) -> None:
    def _drop_permissions_for_table(
        client: BigQueryClient, dataset_id: str, table_id: str
    ) -> None:
        dataset_ref = client.dataset_ref_for_id(dataset_id)
        table = client.get_table(dataset_ref=dataset_ref, table_id=table_id)
        client.drop_row_level_permissions(table)

    run_operation_for_tables(
        client=BigQueryClientImpl(),
        prompt="Drop row level permissions",
        operation=_drop_permissions_for_table,
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
        help="Optionally, only drop permissions from datasets matching this prefix",
        type=str,
        required=False,
    )

    args = parser.parse_args()

    with local_project_id_override(args.project_id):
        rollback_row_level_permissions_on_all_tables(args.dataset_prefix)
