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

"""Helper functions to create and update BigQuery Views."""

import concurrent
import logging
from typing import Optional

from google.cloud import bigquery
from google.cloud import exceptions

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.persistence.database.export.cloud_sql_to_bq_export_config import CloudSqlToBQConfig
from recidiviz.utils import metadata

_BQ_LOAD_WAIT_TIMEOUT_SECONDS = 300


def start_table_load(
        big_query_client: BigQueryClient,
        table_name: str,
        cloud_sql_to_bq_config: CloudSqlToBQConfig) -> Optional[bigquery.job.LoadJob]:
    """Loads a table from CSV data in GCS to BigQuery.

    Given a table name, retrieve the export URI and schema from cloud_sql_to_bq_config,
    then load the table into BigQuery.

    This starts the job, but does not wait until it completes.

    Tables are created if they do not exist, and overwritten if they do exist.

    Because we are using bigquery.WriteDisposition.WRITE_TRUNCATE, the table's
    data will be completely wiped and overwritten with the contents of the CSV.

    Args:
        big_query_client: A BigQueryClient.
        table_name: Table to import. Table must be defined in the base schema.
        cloud_sql_to_bq_config: Export config class for a specific SchemaType.
    Returns:
        (load_job, table_ref) where load_job is the LoadJob object containing
            job details, and table_ref is the destination TableReference object.
            If the job fails to start, returns None.
    """
    uri = cloud_sql_to_bq_config.get_gcs_export_uri_for_table(table_name)

    logging.info("GCS URI [%s] in project [%s]", uri, metadata.project_id())

    bq_schema = cloud_sql_to_bq_config.get_bq_schema_for_table(table_name)
    dataset_ref = cloud_sql_to_bq_config.get_dataset_ref(big_query_client)

    load_job = big_query_client.load_table_from_cloud_storage_async(
        source_uri=uri,
        destination_dataset_ref=dataset_ref,
        destination_table_id=table_name,
        destination_table_schema=bq_schema
    )

    return load_job


def wait_for_table_load(big_query_client: BigQueryClient,
                        load_job: bigquery.job.LoadJob) -> bool:
    """Wait for a table LoadJob to finish, and log its status.

    Args:
        big_query_client: A BigQueryClient for querying the result table
        load_job: BigQuery LoadJob whose result to wait for.
    Returns:
        True if no errors were raised, else False.
    """
    try:
        # Wait for table load job to complete.
        load_job.result(_BQ_LOAD_WAIT_TIMEOUT_SECONDS)
        logging.info("Load job %s for table %s.%s.%s completed successfully.",
                     load_job.job_id,
                     load_job.destination.project,
                     load_job.destination.dataset_id,
                     load_job.destination.table_id)

        destination_table = big_query_client.get_table(
            big_query_client.dataset_ref_for_id(load_job.destination.dataset_id),
            load_job.destination.table_id)
        logging.info("Loaded %d rows in table %s.%s.%s",
                     destination_table.num_rows,
                     load_job.destination.project,
                     load_job.destination.dataset_id,
                     load_job.destination.table_id)
        return True
    except (exceptions.NotFound,
            exceptions.BadRequest,
            concurrent.futures.TimeoutError): # type: ignore
        logging.exception("Failed to load table %s.%s.%s",
                          load_job.destination.project,
                          load_job.destination.dataset_id,
                          load_job.destination.table_id)
        return False


def start_table_load_and_wait(
        big_query_client: BigQueryClient,
        table_name: str,
        cloud_sql_to_bq_config: CloudSqlToBQConfig) -> bool:
    """Loads a table from CSV data in GCS to BigQuery, waits until completion.

    See start_table_load and wait_for_table_load for details.

    Returns:
        True if no errors were raised, else False.
    """

    load_job = start_table_load(big_query_client, table_name, cloud_sql_to_bq_config)
    if load_job:
        table_load_success = wait_for_table_load(big_query_client, load_job)

        return table_load_success

    return False


def load_all_tables_concurrently(
        big_query_client: BigQueryClient,
        cloud_sql_to_bq_config: CloudSqlToBQConfig) -> None:
    """Start all table LoadJobs concurrently.

    Wait until completion to log results."""
    tables = cloud_sql_to_bq_config.get_tables_to_export()

    # Kick off all table LoadJobs at the same time.
    load_jobs = [
        start_table_load(big_query_client, table.name, cloud_sql_to_bq_config)
        for table in tables
    ]

    # Wait for all jobs to finish, log results.
    for load_job in load_jobs:
        if load_job:
            wait_for_table_load(big_query_client, load_job)
