"""As part of the roll-out plan, we have a script that will iterate through every table 
applying these row-level permissions. Should we find that row-level permissions are 
causing an issue, this script is intended to rollback all row-level permissions. 
This script will drop row-level permissions on ALL tables in the given project.
Usage:
    python -m recidiviz.tools-deploy.oneoffs.rollback_row_level_permissions_from_all_tables --project_id recidiviz-staging
"""

import argparse
import logging

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.utils.environment import GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


def rollback_row_level_permissions_on_all_tables() -> None:
    client = BigQueryClientImpl()
    datasets = list(client.list_datasets())
    for dataset in datasets:
        dataset_ref = client.dataset_ref_for_id(dataset.dataset_id)
        tables = client.list_tables(dataset.dataset_id)
        for t in tables:
            table = client.get_table(dataset_ref=dataset_ref, table_id=t.table_id)
            client.drop_row_level_permissions(table)


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

    args = parser.parse_args()

    with local_project_id_override(args.project_id):
        rollback_row_level_permissions_on_all_tables()
