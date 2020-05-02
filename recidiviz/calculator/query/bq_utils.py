# Recidiviz - a data platform for criminal justice reform
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

from google.cloud import bigquery, exceptions

# Importing only for typing.
from recidiviz.calculator.query import bqview

# Location of the GCP project that must be the same for bigquery.Client calls
LOCATION = 'US'

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
            "Dataset [%s] does not exist. Creating...", str(dataset_ref))
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
            "Table [%s] does not exist in dataset [%s]",
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
        logging.info("Updating existing view [%s]", str(bq_view))
        client().update_table(bq_view, ['view_query'])
    else:
        logging.info("Creating view %s", str(bq_view))
        client().create_table(bq_view)


def create_or_update_table_from_view(
        dataset_ref: bigquery.dataset.DatasetReference,
        view: bqview.BigQueryView,
        query: str,
        output_table: str):
    """Queries data in a view and loads it into a table.

    If the table exists, overwrites existing data. Creates the table if it does
    not exist.

    This is a synchronous function that waits for the query job to complete
    before returning.

    Args:
        dataset_ref: The BigQuery dataset where the view is.
        view: The View to query.
        query: The query to run.
        output_table: The name of the table to create or update.
    """
    if table_exists(dataset_ref, view.view_id):
        job_config = bigquery.QueryJobConfig()
        job_config.destination = \
            client().dataset(dataset_ref.dataset_id).table(output_table)
        job_config.write_disposition = \
            bigquery.job.WriteDisposition.WRITE_TRUNCATE

        logging.info("Querying table: %s with query: %s", output_table, query)

        query_job = client().query(
            query=query,
            location=LOCATION,
            job_config=job_config,
        )
        # Waits for job to complete
        query_job.result()
    else:
        logging.error(
            "View [%s] does not exist in dataset [%s]",
            view.view_id, str(dataset_ref))


def export_to_cloud_storage(dataset_ref: bigquery.dataset.DatasetReference,
                            bucket: str, table_name: str, filename: str):
    """Exports the table corresponding to the given view to the bucket.

    Extracts the entire table and exports in JSON format to the given bucket in
    Cloud Storage.

    This is a synchronous function that waits for the query job to complete
    before returning.

    Args:
        dataset_ref: The dataset where the view and table exist.
        bucket: The bucket in Cloud Storage where the export should go.
        table_name: The table or view to export.
        filename: The name of the file to write to.
    """
    if table_exists(dataset_ref, table_name):
        destination_uri = "gs://{}/{}".format(bucket, filename)

        table_ref = dataset_ref.table(table_name)

        job_config = bigquery.ExtractJobConfig()
        job_config.destination_format = \
            bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON

        extract_job = client().extract_table(
            table_ref,
            destination_uri,
            # Location must match that of the source table.
            location=LOCATION,
            job_config=job_config
        )
        # Waits for job to complete
        extract_job.result()
    else:
        logging.error(
            "Table [%s] does not exist in dataset [%s]",
            table_name, str(dataset_ref))


def _table_name_for_view(view: bqview.BigQueryView,
                         state_code: str) -> str:
    """Returns the name of the table where the view's contents are."""
    return view.view_id + '_table_' + state_code


def _destination_filename_for_view(view: bqview.BigQueryView,
                                   state_code: str) -> str:
    """Returns the filename that should be used as an export destination."""
    return state_code + '/' + view.view_id + '.json'


def unnest_district(district_column='supervising_district_external_id'):
    return f"UNNEST ([{district_column}, 'ALL']) AS district"


def unnest_supervision_type(supervision_type_column='supervision_type'):
    return f"UNNEST ([{supervision_type_column}, 'ALL']) AS supervision_type"


def unnest_charge_category(category_column='case_type'):
    return f"UNNEST ([{category_column}, 'ALL']) AS charge_category"


def unnest_metric_period_months():
    return "UNNEST ([1, 3, 6, 12, 36]) AS metric_period_months"


def unnest_race_and_ethnicity():
    return "UNNEST (ARRAY_CONCAT(IFNULL(SPLIT(race), []), IFNULL(SPLIT(ethnicity), []))) race_or_ethnicity"


def metric_period_condition(month_offset=1):
    return f"""DATE(year, month, 1) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE('US/Pacific'), MONTH),
                                                INTERVAL metric_period_months - {month_offset} MONTH)"""
