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
"""
Script for copying all views from one dataset to another in BigQuery.

Example usage (run from `pipenv shell`):

python -m recidiviz.tools.copy_bq_views \
    --source-project-id recidiviz-123 \
    --source-dataset-id my-dataset
    --destination-project-id recidiviz-staging
    --destination-dataset-id new-dataset
"""
import argparse
import logging

from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.big_query.big_query_view import BigQueryView


def main(*, source_project_id, source_dataset_id, destination_project_id, destination_dataset_id):
    """Copies all views from the source_project_id.source_dataset_id to the
    destination_project_id.destination_dataset_id."""

    # Construct a BigQuery client with the source_project_id
    source_client = BigQueryClientImpl(project_id=source_project_id)

    # Construct a BigQuery client with the destination_project_id
    destination_client = BigQueryClientImpl(project_id=destination_project_id)

    destination_dataset = bigquery.DatasetReference(destination_project_id, destination_dataset_id)

    tables_in_source_dataset = source_client.list_tables(source_dataset_id)

    for table_ref in tables_in_source_dataset:
        table = source_client.client.get_table(table_ref)

        # Only copy this view if there is a view_query to replicate and the view doesn't already exist in the
        # destination dataset
        if table.view_query and not destination_client.table_exists(destination_dataset, table_id=table.table_id):
            # Retrieve all of the information about the view
            source_client.copy_view(view=BigQueryView(dataset_id=table_ref.dataset_id,
                                                      view_id=table.table_id,
                                                      view_query_template=table.view_query),
                                    destination_client=destination_client,
                                    destination_dataset_ref=destination_dataset)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('--source-project-id', required=True, help='The project_id where the existing views live')

    parser.add_argument('--source-dataset-id', required=True, help='The name of the dataset to copy.')

    parser.add_argument('--destination-project-id', required=True,
                        help='The project_id where the new views should live. Can be the same as source-project-id.')

    parser.add_argument('--destination-dataset-id', required=True, help='The dataset where the new views should live.')

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    main(source_project_id=args.source_project_id, source_dataset_id=args.source_dataset_id,
         destination_project_id=args.destination_project_id, destination_dataset_id=args.destination_dataset_id)
