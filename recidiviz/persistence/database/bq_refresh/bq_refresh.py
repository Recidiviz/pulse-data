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

from google.cloud import bigquery
from google.cloud import exceptions

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import CloudSqlToBQConfig
from recidiviz.utils import metadata

_BQ_LOAD_WAIT_TIMEOUT_SECONDS = 300

TEMP_TABLE_NAME = '{table_name}_temp'


def refresh_bq_table_from_gcs_export_synchronous(big_query_client: BigQueryClient,
                                                 table_name: str,
                                                 cloud_sql_to_bq_config: CloudSqlToBQConfig) -> None:
    """Loads data from Cloud SQL export and rows excluded from the SQL export from the current BQ table
        into a target BQ table. If target BQ table does not exist, it is created.

        For example:
        1. Load data from GCS to temp table and wait.
        2. Load data from stale BQ table to temp table and wait and filter for rows excluded from SQL export.
            If stale BQ table does not exist, create the table. If the temp table has schema fields missing in the
            stale BQ table, add missing fields to the BQ table query.
        3. Load data from the temp table to the final BQ table. Overwrite all the data with the temp table and add any
            missing fields to the destination table.
        4. Delete temporary table.

        Waits until each BigQuery load is completed.

        Args:
            big_query_client: A BigQueryClient.
            table_name: Table to import from temp table. Table must be defined
                in the metadata_base class for its corresponding SchemaType.
            cloud_sql_to_bq_config: The config class for the given SchemaType.
        Returns:
            If the table load succeeds, returns None. If it fails it raises a ValueError.
    """
    temp_table_name = TEMP_TABLE_NAME.format(table_name=table_name)
    # Load GCS exported CSVs to temp table
    load_table_from_gcs_and_wait(
        big_query_client,
        table_name,
        cloud_sql_to_bq_config,
        destination_table_id=temp_table_name
    )

    # Load rows excluded from CloudSQL export to temp table if table exists.
    # If table does not exist, create BQ destination table.
    dataset_ref = cloud_sql_to_bq_config.get_dataset_ref(big_query_client)

    if big_query_client.table_exists(dataset_ref=dataset_ref, table_id=table_name):
        load_rows_excluded_from_refresh_into_temp_table_and_wait(
            big_query_client,
            table_name,
            cloud_sql_to_bq_config,
            destination_table_id=temp_table_name)
    else:
        logging.info("Destination table [%s.%s] does not exist! Creating table from schema.",
                     cloud_sql_to_bq_config.dataset_id, table_name)

        create_table_success = big_query_client.create_table_with_schema(
            dataset_id=cloud_sql_to_bq_config.dataset_id,
            table_id=table_name,
            schema_fields=cloud_sql_to_bq_config.get_bq_schema_for_table(table_name))

        if not create_table_success:
            raise ValueError(f'Failed to create table [{table_name}. Skipping table refresh from GCS.')

    logging.info('Loading BQ Table [%s] from temp table [%s]', table_name, temp_table_name)

    load_job = big_query_client.load_table_from_table_async(
        source_dataset_id=cloud_sql_to_bq_config.dataset_id,
        source_table_id=temp_table_name,
        destination_dataset_id=cloud_sql_to_bq_config.dataset_id,
        destination_table_id=table_name)

    table_load_success = wait_for_table_load(big_query_client, load_job)

    if not table_load_success:
        raise ValueError(f"Failed to load BigQuery table [{table_name}] from temp table [{temp_table_name}].")

    delete_temp_table_if_exists(big_query_client, temp_table_name, cloud_sql_to_bq_config)


def load_rows_excluded_from_refresh_into_temp_table_and_wait(big_query_client: BigQueryClient,
                                                             table_name: str,
                                                             cloud_sql_to_bq_config: CloudSqlToBQConfig,
                                                             destination_table_id: str) -> None:
    """Load the stale rows excluded from the CLoudSQL export to the temporary table.

        New columns in the CloudSQL export data that are missing from the stale BQ Table will be added to the schema,
        using the flag hydrate_missing_columns_with_null.

        Because we are using bigquery.WriteDisposition.WRITE_APPEND, the table is not truncated and new data
        is appended.

        Args:
            big_query_client: A BigQueryClient.
            table_name: Table to select from to copy rows into the temp table.
            cloud_sql_to_bq_config: Export config class for a specific SchemaType.
            destination_table_id: Name for the temp table. If it doesn't exist, it will be created.
        Returns:
            If the table load succeeds, returns None. If it fails it raises a ValueError.
        """
    table_refresh_query_builder = \
        cloud_sql_to_bq_config.get_stale_bq_rows_for_excluded_regions_query_builder(table_name)

    load_job = big_query_client.insert_into_table_from_table_async(
        source_dataset_id=cloud_sql_to_bq_config.dataset_id,
        source_table_id=table_name,
        destination_dataset_id=cloud_sql_to_bq_config.dataset_id,
        destination_table_id=destination_table_id,
        source_data_filter_clause=table_refresh_query_builder.filter_clause(),
        hydrate_missing_columns_with_null=True,
        allow_field_additions=True)

    table_load_success = wait_for_table_load(big_query_client, load_job)

    if not table_load_success:
        raise ValueError(f'Failed to load temp table with excluded rows from existing BQ table. Skipping copy of '
                         f'temp table [{destination_table_id}] to BQ table [{table_name}]')


def load_table_from_gcs_and_wait(big_query_client: BigQueryClient,
                                 table_name: str,
                                 cloud_sql_to_bq_config: CloudSqlToBQConfig,
                                 destination_table_id: str) -> None:
    """Loads a table from CSV data in GCS to BigQuery.

    Given a table name and a destination_table_id, retrieve the export URI and schema from cloud_sql_to_bq_config,
    then load the table into the destination_table_id.

    This starts the job, but does not wait until it completes.

    Tables are created if they do not exist, and overwritten if they do exist.

    Because we are using bigquery.WriteDisposition.WRITE_TRUNCATE, the table's
    data will be completely wiped and overwritten with the contents of the CSV.

    Args:
        big_query_client: A BigQueryClient.
        table_name: Table to import. Table must be defined in the base schema.
        cloud_sql_to_bq_config: Export config class for a specific SchemaType.
        destination_table_id: Optional destination table name. If none is given,
        the provided table name is used.
    Returns:
        If the table load succeeds, returns None. If it fails it raises a ValueError.
    """
    uri = cloud_sql_to_bq_config.get_gcs_export_uri_for_table(table_name)

    logging.info("GCS URI [%s] in project [%s]", uri, metadata.project_id())

    bq_schema = cloud_sql_to_bq_config.get_bq_schema_for_table(table_name)
    dataset_ref = cloud_sql_to_bq_config.get_dataset_ref(big_query_client)

    load_job = big_query_client.load_table_from_cloud_storage_async(
        source_uri=uri,
        destination_dataset_ref=dataset_ref,
        destination_table_id=destination_table_id,
        destination_table_schema=bq_schema
    )

    table_load_success = wait_for_table_load(big_query_client, load_job)

    if not table_load_success:
        raise ValueError(f'Copy from cloud storage to temp table failed. Skipping refresh for BQ table [{table_name}]')


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


def delete_temp_table_if_exists(big_query_client: BigQueryClient,
                                temp_table_name: str,
                                cloud_sql_to_bq_config: CloudSqlToBQConfig) -> None:
    dataset_ref = cloud_sql_to_bq_config.get_dataset_ref(big_query_client)
    if not big_query_client.table_exists(dataset_ref=dataset_ref, table_id=temp_table_name):
        logging.info('Delete temp table failed, table [%s] does not exist.', temp_table_name)
        return
    big_query_client.delete_table(dataset_id=cloud_sql_to_bq_config.dataset_id, table_id=temp_table_name)
    logging.info('Deleted temporary table [%s]', temp_table_name)
