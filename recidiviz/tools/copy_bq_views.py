# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Script for copying all views from one dataset to another in BigQuery.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.copy_bq_views \
    --source_project_id recidiviz-123 \
    --source_dataset_id my-dataset
    --destination_project_id recidiviz-staging
    --destination_dataset_id new-dataset
"""
import argparse
import logging
import sys
from typing import List, Tuple

from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryView


def copy_bq_views(
    source_project_id: str,
    source_dataset_id: str,
    destination_project_id: str,
    destination_dataset_id: str,
) -> None:
    """Copies all views from the source_project_id.source_dataset_id to the
    destination_project_id.destination_dataset_id."""

    # Construct a BigQuery client with the source_project_id
    source_client = BigQueryClientImpl(project_id=source_project_id)

    # Construct a BigQuery client with the destination_project_id
    destination_client = BigQueryClientImpl(project_id=destination_project_id)
    destination_dataset = bigquery.DatasetReference(
        destination_project_id, destination_dataset_id
    )
    tables_in_source_dataset = source_client.list_tables(source_dataset_id)

    for table_ref in tables_in_source_dataset:
        table = source_client.get_table(
            source_client.dataset_ref_for_id(table_ref.dataset_id), table_ref.table_id
        )
        view_query = table.view_query

        # Only copy this view if there is a view_query to replicate and the view doesn't already exist in the
        # destination dataset
        if view_query and not destination_client.table_exists(
            destination_dataset, table_id=table.table_id
        ):
            # Remove any references to the source_project_id from the view_query
            updated_view_query = view_query.replace(source_project_id, "{project_id}")

            # Retrieve all of the information about the view
            source_client.copy_view(
                view=BigQueryView(
                    project_id=destination_project_id,
                    dataset_id=destination_dataset_id,
                    view_id=table.table_id,
                    description=table.description,
                    view_query_template=updated_view_query,
                ),
                destination_client=destination_client,
                destination_dataset_ref=destination_dataset,
            )


def parse_arguments(argv: List[str]) -> Tuple[argparse.Namespace, List[str]]:
    """Parses the required arguments."""
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--source_project_id",
        required=True,
        help="The project_id where the existing views live",
    )

    parser.add_argument(
        "--source_dataset_id", required=True, help="The name of the dataset to copy."
    )

    parser.add_argument(
        "--destination_project_id",
        required=True,
        help="The project_id where the new views should live. Can be the same as source_project_id.",
    )

    parser.add_argument(
        "--destination_dataset_id",
        required=True,
        help="The dataset where the new views should live.",
    )

    return parser.parse_known_args(argv)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    known_args, _ = parse_arguments(sys.argv)

    copy_bq_views(
        source_project_id=known_args.source_project_id,
        source_dataset_id=known_args.source_dataset_id,
        destination_project_id=known_args.destination_project_id,
        destination_dataset_id=known_args.destination_dataset_id,
    )
