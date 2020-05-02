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
from concurrent import futures
from typing import List, Optional

import attr
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
        output_table: str) -> Optional[bigquery.QueryJob]:
    """Queries data in a view and loads it into a table.

    If the table exists, overwrites existing data. Creates the table if it does
    not exist.

    It is the callers responsibility to wait for the resulting job to complete.

    Args:
        dataset_ref: The BigQuery dataset where the view is.
        view: The View to query.
        query: The query to run.
        output_table: The name of the table to create or update.
    Returns:
        The QueryJob that was started.
    """
    if not table_exists(dataset_ref, view.view_id):
        logging.error(
            "View [%s] does not exist in dataset [%s]",
            view.view_id, str(dataset_ref))
        return None
    job_config = bigquery.QueryJobConfig()
    job_config.destination = \
        client().dataset(dataset_ref.dataset_id).table(output_table)
    job_config.write_disposition = \
        bigquery.job.WriteDisposition.WRITE_TRUNCATE

    logging.info("Querying table: %s with query: %s", output_table, query)

    return client().query(
        query=query,
        location=LOCATION,
        job_config=job_config,
    )


def export_to_cloud_storage(dataset_ref: bigquery.dataset.DatasetReference,
                            bucket: str, table_name: str, filename: str) -> Optional[bigquery.ExtractJob]:
    """Exports the table to bucket with the given filename.

    Extracts the entire table and exports in JSON format to the given bucket in
    Cloud Storage.

    Note that BigQuery does not support exporting views, they must first be
    materialized into tables.

    It is the callers responsibility to wait for the resulting job to complete.

    Args:
        dataset_ref: The dataset where the table exists.
        bucket: The bucket in Cloud Storage where the export should go.
        table_name: The table to export.
        filename: The name of the file to write to.
    Returns:
        The ExtractJob that was started.
    """
    if not table_exists(dataset_ref, table_name):
        logging.error(
            "Table [%s] does not exist in dataset [%s]",
            table_name, str(dataset_ref))
        return None
    destination_uri = "gs://{}/{}".format(bucket, filename)

    table_ref = dataset_ref.table(table_name)

    job_config = bigquery.ExtractJobConfig()
    job_config.destination_format = \
        bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON

    return client().extract_table(
        table_ref,
        destination_uri,
        # Location must match that of the source table.
        location=LOCATION,
        job_config=job_config
    )


@attr.s(frozen=True)
class ExportViewConfig:
    """Specification for how to export a particular view."""

    # The view to export.
    view: bqview.BigQueryView = attr.ib()

    # The name of the intermediate table to create/update.
    intermediate_table_name: str = attr.ib()

    # The query to use to build the intermediate table.
    intermediate_table_query: str = attr.ib()

    # The desired name of the output file.
    filename: str = attr.ib()


def export_views_to_cloud_storage(
        dataset_ref: bigquery.dataset.DatasetReference, bucket: str,
        export_configs: List[ExportViewConfig]):
    """Exports the views to cloud storage according to the given configs.

    This is a two-step process. First, for each view, the view query is executed
    and the entire result is loaded into a table in BigQuery. Then, for each
    table, the contents are exported to the cloud storage bucket in JSON format.
    This has to be a two-step process because BigQuery doesn't support exporting
    a view directly, it must be materialized in a table first.

    Args:
        dataset_ref: The dataset where the views exist.
        bucket: The bucket in Cloud Storage where the export should go.
        export_configs: List of views along with how to export them.
    """
    query_jobs = []
    for export_config in export_configs:
        query_job = create_or_update_table_from_view(
            dataset_ref, export_config.view,
            export_config.intermediate_table_query,
            export_config.intermediate_table_name)
        if query_job is not None:
            query_jobs.append(query_job)
    futures.wait(query_jobs)

    extract_jobs = []
    for export_config in export_configs:
        extract_job = export_to_cloud_storage(
            dataset_ref, bucket, export_config.intermediate_table_name,
            export_config.filename)
        if extract_job is not None:
            extract_jobs.append(extract_job)
    futures.wait(extract_jobs)


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
