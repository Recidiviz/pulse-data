"""Applies relevant row-level permissions to all tables in the given project. 
Will overwrite all existing row-level permissions.
Usage:
    python -m recidiviz.tools/deploy.oneoffs.apply_row_level_permissions_to_all_tables --project_id recidiviz-staging
"""
import argparse
import logging

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.utils.environment import GCP_PROJECTS
from recidiviz.utils.metadata import local_project_id_override


def apply_row_level_permissions_to_all_tables() -> None:
    client = BigQueryClientImpl()
    datasets = list(client.list_datasets())
    for dataset in datasets:
        dataset_ref = client.dataset_ref_for_id(dataset.dataset_id)
        tables = client.list_tables(dataset.dataset_id)
        for t in tables:
            table = client.get_table(dataset_ref=dataset_ref, table_id=t.table_id)
            client.apply_row_level_permissions(table)


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
        apply_row_level_permissions_to_all_tables()
