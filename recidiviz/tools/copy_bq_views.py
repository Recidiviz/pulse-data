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

from recidiviz.calculator.query import bq_utils


def main(*, source_project_id, source_dataset_id, destination_project_id, destination_dataset_id):
    """Copies all views from the source_project_id.source_dataset_id to the
    destination_project_id.destination_dataset_id."""

    # Construct a BigQuery client with the source_project_id
    source_client = bigquery.Client(project=source_project_id)

    # Construct a BigQuery client with the destination_project_id if it differs from the source_project_id
    destination_client = (bigquery.Client(project=destination_project_id)
                          if source_project_id != destination_project_id else source_client)

    destination_dataset = bigquery.DatasetReference(destination_project_id, destination_dataset_id)

    # Create the destination dataset if it doesn't yet exist
    bq_utils.create_dataset_if_necessary(destination_dataset)

    views_to_copy = source_client.list_tables(source_dataset_id)

    for view_ref in views_to_copy:
        # Retrieve all of the information about the view
        view = source_client.get_table(view_ref)
        view_query = view.view_query

        # Only copy this view if there is a view_query to replicate and the view doesn't already exist in the
        # destination dataset
        if not bq_utils.table_exists(destination_dataset, view_ref.table_id) and view_query:
            new_view_ref = destination_dataset.table(view_ref.table_id)
            new_view = bigquery.Table(new_view_ref)
            new_view.view_query = view_query.format(destination_project_id, destination_dataset_id, view_ref.table_id)
            destination_client.create_table(new_view)
            logging.info("Created %s", new_view_ref)


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
