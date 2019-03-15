# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Helper functions for creating and updating BigQuery datasets/tables/views."""

import logging

from google.cloud import bigquery
from google.cloud import exceptions

# Importing only for typing.
from recidiviz.calculator.bq.views import bqview


_client = None
def client() -> bigquery.Client:
    global _client
    if not _client:
        _client = bigquery.Client()
    return _client


def create_dataset_if_necessary(dataset_ref: bigquery.dataset.DatasetReference):
    """Create a BigQuery dataset if it does not exist."""
    try:
        client().get_dataset(dataset_ref)
    except exceptions.NotFound:
        logging.info(
            'Dataset %s does not exist. Creating...', str(dataset_ref))
        dataset = bigquery.Dataset(dataset_ref)
        client().create_dataset(dataset)


def table_exists(
        dataset_ref: bigquery.dataset.DatasetReference,
        table_id: str) -> bool:
    """Check whether or not a BigQuery Table or View exists in a Dataset."""
    table_ref = dataset_ref.table(table_id)

    try:
        client().get_table(table_ref)
        return True
    except exceptions.NotFound:
        logging.warning(
            'Table "%s" does not exist in dataset %s',
            table_id, str(dataset_ref))
        return False


def create_or_update_view(
        dataset_ref: bigquery.dataset.DatasetReference,
        view: bqview.BigQueryView):
    """Create a View if it does not exist, or update its query if it does.

    Args:
        dataset_ref: The BigQuery dataset to store the view in.
        view: The View to create or update.
    """
    view_ref = dataset_ref.table(view.view_id)
    bq_view = bigquery.Table(view_ref)
    bq_view.view_query = view.view_query

    if table_exists(dataset_ref, view.view_id):
        logging.info('Updating existing view %s', str(bq_view))
        client().update_table(bq_view, ['view_query'])
    else:
        logging.info('Creating view %s', str(bq_view))
        client().create_table(bq_view)
