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
from typing import Optional, Tuple

from google.cloud import bigquery
from google.cloud import exceptions


# Importing only for typing.
import sqlalchemy

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.persistence.database.export import export_config
from recidiviz.persistence.database.sqlalchemy_engine_manager import SchemaType

_BQ_LOAD_WAIT_TIMEOUT_SECONDS = 300


def start_table_load(
        big_query_client: BigQueryClient,
        dataset_ref: bigquery.dataset.DatasetReference,
        table_name: str,
        schema_type: SchemaType) -> Optional[bigquery.job.LoadJob]:
    """Loads a table from CSV data in GCS to BigQuery.

    Given a table name, retrieve the export URI and schema from export_config,
    then load the table into BigQuery.

    This starts the job, but does not wait until it completes.

    Tables are created if they do not exist, and overwritten if they do exist.

    Because we are using bigquery.WriteDisposition.WRITE_TRUNCATE, the table's
    data will be completely wiped and overwritten with the contents of the CSV.

    Args:
        big_query_client: A BigQueryClient.
        dataset_ref: The BigQuery dataset to load the table into. Gets created
            if it does not already exist.
        table_name: Table to import. Table must be defined
            in the export_config.*_TABLES_TO_EXPORT for the given module
        schema_type: The schema of the table being loaded, either
            SchemaType.JAILS or SchemaType.STATE.
    Returns:
        (load_job, table_ref) where load_job is the LoadJob object containing
            job details, and table_ref is the destination TableReference object.
            If the job fails to start, returns None.
    """
    if schema_type == SchemaType.JAILS:
        export_schema = export_config.COUNTY_TABLE_EXPORT_SCHEMA
    elif schema_type == SchemaType.STATE:
        export_schema = export_config.STATE_TABLE_EXPORT_SCHEMA
    elif schema_type == SchemaType.OPERATIONS:
        export_schema = export_config.OPERATIONS_TABLE_EXPORT_SCHEMA
    else:
        logging.exception("Unknown schema type: %s", schema_type)
        return None

    uri = export_config.gcs_export_uri(table_name)

    try:
        bq_schema = [
            bigquery.SchemaField(
                field['name'], field['type'], field['mode'])
            for field in export_schema[table_name]
        ]
    except KeyError:
        logging.exception(
            "Unknown table name '%s'. Is it listed in "
            "the TABLES_TO_EXPORT for the %s module?", schema_type, table_name)
        return None

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
        dataset_ref: bigquery.dataset.DatasetReference,
        table_name: str, schema_type: SchemaType) -> bool:
    """Loads a table from CSV data in GCS to BigQuery, waits until completion.

    See start_table_load and wait_for_table_load for details.

    Returns:
        True if no errors were raised, else False.
    """

    load_job = start_table_load(big_query_client, dataset_ref, table_name, schema_type)
    if load_job:
        table_load_success = wait_for_table_load(big_query_client, load_job)

        return table_load_success

    return False


def load_all_tables_concurrently(
        big_query_client: BigQueryClient,
        dataset_ref: bigquery.dataset.DatasetReference,
        tables: Tuple[sqlalchemy.Table, ...],
        schema_type: SchemaType) -> None:
    """Start all table LoadJobs concurrently.

    Wait until completion to log results."""

    # Kick off all table LoadJobs at the same time.
    load_jobs = [
        start_table_load(big_query_client, dataset_ref, table.name, schema_type)
        for table in tables
    ]

    # Wait for all jobs to finish, log results.
    for load_job in load_jobs:
        if load_job:
            wait_for_table_load(big_query_client, load_job)
