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
"""Manages the storage of data produced by calculations."""
import logging
import datetime
from http import HTTPStatus
from typing import Tuple

import flask
from google.cloud.bigquery import WriteDisposition
from more_itertools import peekable

from recidiviz.big_query.big_query_client import BigQueryClientImpl
from recidiviz.calculator.dataflow_output_storage_config import DATAFLOW_METRICS_COLD_STORAGE_DATASET, \
    MAX_DAYS_IN_DATAFLOW_METRICS_TABLE
from recidiviz.calculator.query.state.dataset_config import DATAFLOW_METRICS_DATASET, REFERENCE_VIEWS_DATASET
from recidiviz.utils.auth import authenticate_request

# Datasets must be at least 12 hours old to be deleted
DATASET_DELETION_MIN_SECONDS = 12 * 60 * 60

calculation_data_storage_manager_blueprint = flask.Blueprint('calculation_data_storage_manager', __name__)


@calculation_data_storage_manager_blueprint.route('/prune_old_dataflow_data')
@authenticate_request
def prune_old_dataflow_data() -> Tuple[str, HTTPStatus]:
    """Calls the move_old_dataflow_metrics_to_cold_storage function."""
    move_old_dataflow_metrics_to_cold_storage()

    return '', HTTPStatus.OK


@calculation_data_storage_manager_blueprint.route('/delete_empty_datasets')
@authenticate_request
def delete_empty_datasets() -> Tuple[str, HTTPStatus]:
    """Calls the _delete_empty_datasets function."""
    _delete_empty_datasets()

    return '', HTTPStatus.OK


def _delete_empty_datasets() -> None:
    """Deletes all empty datasets in BigQuery."""
    bq_client = BigQueryClientImpl()
    datasets = bq_client.list_datasets()

    for dataset_resource in datasets:
        dataset_ref = bq_client.dataset_ref_for_id(dataset_resource.dataset_id)
        dataset = bq_client.get_dataset(dataset_ref)
        tables = peekable(bq_client.list_tables(dataset.dataset_id))
        created_time = dataset.created
        dataset_age_seconds = (datetime.datetime.now() - created_time).total_seconds()

        if not tables and dataset_age_seconds > DATASET_DELETION_MIN_SECONDS:
            logging.info("Dataset %s is empty and was not created very recently. Deleting...", dataset_ref.dataset_id)
            bq_client.delete_dataset(dataset_ref)


def move_old_dataflow_metrics_to_cold_storage() -> None:
    """Moves old output in Dataflow metrics tables to tables in a cold storage dataset. We only keep the
    MAX_DAYS_IN_DATAFLOW_METRICS_TABLE days worth of data in a Dataflow metric table at once. All other
    output is moved to cold storage.
    """
    bq_client = BigQueryClientImpl()
    dataflow_metrics_dataset = DATAFLOW_METRICS_DATASET
    cold_storage_dataset = DATAFLOW_METRICS_COLD_STORAGE_DATASET
    dataflow_metrics_tables = bq_client.list_tables(dataflow_metrics_dataset)

    for table_ref in dataflow_metrics_tables:
        table_id = table_ref.table_id

        source_data_join_clause = """LEFT JOIN
                          (SELECT DISTINCT job_id AS keep_job_id FROM
                          `{project_id}.{reference_views_dataset}.most_recent_job_id_by_metric_and_state_code_materialized`)
                        ON job_id = keep_job_id
                        LEFT JOIN 
                          (SELECT DISTINCT created_on AS keep_created_date FROM
                          `{project_id}.{dataflow_metrics_dataset}.{table_id}`
                          ORDER BY created_on DESC
                          LIMIT {day_count_limit})
                        ON created_on = keep_created_date
                        """.format(
                            project_id=table_ref.project,
                            dataflow_metrics_dataset=table_ref.dataset_id,
                            reference_views_dataset=REFERENCE_VIEWS_DATASET,
                            table_id=table_id,
                            day_count_limit=MAX_DAYS_IN_DATAFLOW_METRICS_TABLE
                        )

        # Exclude these columns leftover from the exclusion join from being added to the metric tables in cold storage
        columns_to_exclude_from_transfer = ['keep_job_id', 'keep_created_date']

        # This filter will return the rows that should be moved to cold storage
        insert_filter_clause = "WHERE keep_job_id IS NULL AND keep_created_date IS NULL"

        # Query for rows to be moved to the cold storage table
        insert_query = """
            SELECT * EXCEPT({columns_to_exclude}) FROM
            `{project_id}.{dataflow_metrics_dataset}.{table_id}`
            {source_data_join_clause}
            {insert_filter_clause}
        """.format(
            columns_to_exclude=', '.join(columns_to_exclude_from_transfer),
            project_id=table_ref.project,
            dataflow_metrics_dataset=table_ref.dataset_id,
            table_id=table_id,
            source_data_join_clause=source_data_join_clause,
            insert_filter_clause=insert_filter_clause
        )

        # Move data from the Dataflow metrics dataset into the cold storage table, creating the table if necessary
        insert_job = bq_client.insert_into_table_from_query(
            destination_dataset_id=cold_storage_dataset,
            destination_table_id=table_id,
            query=insert_query,
            allow_field_additions=True,
            write_disposition=WriteDisposition.WRITE_APPEND)

        # Wait for the insert job to complete before running the replace job
        insert_job.result()

        # This will return the rows that were not moved to cold storage and should remain in the table
        replace_query = """
            SELECT * EXCEPT({columns_to_exclude}) FROM
            `{project_id}.{dataflow_metrics_dataset}.{table_id}`
            {source_data_join_clause}
            WHERE keep_job_id IS NOT NULL OR keep_created_date IS NOT NULL
        """.format(
            columns_to_exclude=', '.join(columns_to_exclude_from_transfer),
            project_id=table_ref.project,
            dataflow_metrics_dataset=table_ref.dataset_id,
            table_id=table_id,
            source_data_join_clause=source_data_join_clause,
        )

        # Replace the Dataflow table with only the rows that should remain
        replace_job = bq_client.create_table_from_query_async(
            dataflow_metrics_dataset, table_ref.table_id, query=replace_query, overwrite=True)

        # Wait for the replace job to complete before moving on
        replace_job.result()
