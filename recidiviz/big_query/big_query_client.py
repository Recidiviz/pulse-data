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
"""Wrapper around the bigquery.Client with convenience functions for querying, creating, copying and exporting BigQuery
tables and views.
"""
import abc
import datetime
import json
import logging
import os
import re
import time
from collections import defaultdict
from concurrent import futures
from typing import IO, Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple

import __main__
import pandas as pd
import pytz
import requests
from google.api_core import retry
from google.api_core.client_options import ClientOptions
from google.api_core.future.polling import PollingFuture
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery, exceptions
from google.cloud.bigquery_datatransfer import (
    CheckValidCredsRequest,
    DataTransferServiceClient,
    ScheduleOptions,
    StartManualTransferRunsRequest,
    TransferConfig,
    TransferState,
)
from google.protobuf import timestamp_pb2
from more_itertools import one, peekable

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.constants import BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH
from recidiviz.big_query.export.export_query_config import ExportQueryConfig
from recidiviz.common.constants.states import StateCode
from recidiviz.common.retry_predicate import ssl_error_retry_predicate
from recidiviz.ingest.direct.dataset_config import (
    raw_latest_views_dataset_for_region,
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import environment, metadata
from recidiviz.utils.environment import in_test
from recidiviz.utils.size import total_size
from recidiviz.utils.string import StrictStringFormatter

_clients_by_project_id_by_region: Dict[str, Dict[str, bigquery.Client]] = defaultdict(
    dict
)

BQ_CLIENT_MAX_POOL_CONNECTIONS = 128
BQ_CLIENT_MAX_POOL_SIZE = 128

DEFAULT_VANTA_DATASET_OWNER = "joshua"


def client(project_id: str, region: str) -> bigquery.Client:
    """Returns a BigQuery client for the given project / region"""
    if (
        project_id not in _clients_by_project_id_by_region
        or region not in _clients_by_project_id_by_region[project_id]
    ):
        if environment.in_test():
            # If we are running from inside a test, the BQ Client should talk to the
            # local BQ emulator.
            # Import test setup utils inline as an extra precaution against importing test code in production envs
            from recidiviz.tests.test_setup_utils import (  # pylint: disable=import-outside-toplevel
                BQ_EMULATOR_PROJECT_ID,
                get_bq_emulator_port,
            )

            client_options = ClientOptions(
                api_endpoint=f"http://0.0.0.0:{get_bq_emulator_port()}"
            )
            new_client = bigquery.Client(
                BQ_EMULATOR_PROJECT_ID,
                client_options=client_options,
                credentials=AnonymousCredentials(),
            )
        else:
            new_client = bigquery.Client(project=project_id, location=region)

        # Update the number of allowed connections to allow for more parallel BQ
        # requests to the same Client. See:
        # https://github.com/googleapis/python-storage/issues/253#issuecomment-687068266.
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=BQ_CLIENT_MAX_POOL_CONNECTIONS,
            pool_maxsize=BQ_CLIENT_MAX_POOL_SIZE,
        )
        # pylint: disable=protected-access
        new_client._http.mount("https://", adapter)
        new_client._http._auth_request.session.mount("https://", adapter)

        _clients_by_project_id_by_region[project_id][region] = new_client
    return _clients_by_project_id_by_region[project_id][region]


DATASET_BACKUP_TABLE_EXPIRATION_MS = 7 * 24 * 60 * 60 * 1000  # 7 days

CROSS_REGION_COPY_STATUS_ATTEMPT_SLEEP_TIME_SEC = 10
# Timeout for just checking the status of the cross-region copy.
DEFAULT_GET_TRANSFER_RUN_TIMEOUT_SEC = 30
DEFAULT_CROSS_REGION_COPY_TIMEOUT_SEC = 15 * 60
# Required value for the data_source_id field when copying datasets between regions
CROSS_REGION_COPY_DATA_SOURCE_ID = "cross_region_copy"
CROSS_REGION_COPY_DISPLAY_NAME_TEMPLATE = (
    "Cross-region copy {source_dataset_id} -> {destination_dataset_id} [{ts}]"
)


class BigQueryClient:
    """Interface for a wrapper around the bigquery.Client with convenience functions for querying, creating, copying and
    exporting BigQuery tables and views.
    """

    # Default region that will be associated with all bigquery.Client() calls
    DEFAULT_REGION = "US"

    @property
    @abc.abstractmethod
    def project_id(self) -> str:
        """The Google Cloud project id for this client."""

    @abc.abstractmethod
    def dataset_ref_for_id(self, dataset_id: str) -> bigquery.DatasetReference:
        """Returns a BigQuery DatasetReference for the dataset with the given dataset name."""

    @abc.abstractmethod
    def create_dataset_if_necessary(
        self,
        dataset_ref: bigquery.DatasetReference,
        default_table_expiration_ms: Optional[int] = None,
    ) -> bigquery.Dataset:
        """Create a BigQuery dataset if it does not exist, with the optional dataset table expiration if provided."""

    @abc.abstractmethod
    def dataset_exists(self, dataset_ref: bigquery.DatasetReference) -> bool:
        """Check whether or not a BigQuery Dataset exists.
        Args:
            dataset_ref: The BigQuery dataset to look for

        Returns:
            True if the dataset exists, False otherwise.
        """

    @abc.abstractmethod
    def dataset_is_empty(self, dataset_ref: bigquery.DatasetReference) -> bool:
        """Check whether or not a BigQuery Dataset is empty (and could be deleted).
        Args:
            dataset_ref: The BigQuery dataset to look for

        Returns:
            True if the dataset is empty, False otherwise.
        """

    @abc.abstractmethod
    def delete_dataset(
        self,
        dataset_ref: bigquery.DatasetReference,
        delete_contents: bool = False,
        not_found_ok: bool = False,
    ) -> None:
        """Deletes a BigQuery dataset
        Args:
            dataset_ref: The BigQuery dataset to delete
            delete_contents: Whether to delete all tables within the dataset. If set to
                False and the dataset has tables, this method fails.
            not_found_ok: If False, this raises an exception when the dataset_ref is
                not found.
        """

    @abc.abstractmethod
    def get_dataset(self, dataset_ref: bigquery.DatasetReference) -> bigquery.Dataset:
        """Fetches a BigQuery dataset.
        Args:
            dataset_ref: The BigQuery dataset to look for

        Returns:
            A bigquery.Dataset object if it exists.
        """

    @abc.abstractmethod
    def list_datasets(self) -> Iterator[bigquery.dataset.DatasetListItem]:
        """List BigQuery datasets. Does not perform a full fetch of each dataset.

        Returns:
            An Iterator of bigquery.DatasetListItems.
        """

    @abc.abstractmethod
    def table_exists(
        self, dataset_ref: bigquery.DatasetReference, table_id: str
    ) -> bool:
        """Check whether or not a BigQuery Table or View exists in a Dataset.

        Args:
            dataset_ref: The BigQuery dataset to search
            table_id: The string table name to look for

        Returns:
            True if the table or view exists, False otherwise.
        """

    @abc.abstractmethod
    def get_table(
        self, dataset_ref: bigquery.DatasetReference, table_id: str
    ) -> bigquery.Table:
        """Fetches the Table for a BigQuery Table or View in the dataset. Throws if it does not exist.

        Args:
            dataset_ref: The BigQuery dataset to search
            table_id: The string table name of the table to return

        Returns:
            A bigquery.Table instance.
        """

    @abc.abstractmethod
    def list_tables(self, dataset_id: str) -> Iterator[bigquery.table.TableListItem]:
        """Returns a list of tables and views in the dataset with the given dataset id."""

    @abc.abstractmethod
    def list_tables_excluding_views(
        self, dataset_id: str
    ) -> Iterator[bigquery.table.TableListItem]:
        """Returns a list of tables (skipping views) in the dataset with the given dataset id."""

    @abc.abstractmethod
    def get_row_counts_for_tables(self, dataset_id: str) -> Dict[str, int]:
        """Returns the row counts for each table in a dataset.

        If a table has no rows, it will be omitted from the output. If the dataset does
        not exist, the output will be empty."""

    @abc.abstractmethod
    def create_table(
        self, table: bigquery.Table, overwrite: bool = False
    ) -> bigquery.Table:
        """Creates a new table in big query. If |overwrite| is False and th tabl
        already exists, raises an AlreadyExists error.

        Args:
            table: The Table to create.
            overwrite: Whether to overwrite a table if one already exists where the
                table is to be created.

        Returns:
            The Table that was just created.
        """

    @abc.abstractmethod
    def create_or_update_view(
        self, view: BigQueryView, might_exist: bool = True
    ) -> bigquery.Table:
        """Create a View if it does not exist, or update its query if it does.

        This runs synchronously and waits for the job to complete.

        Args:
            view: The View to create or update.
            might_exist: If it is possible this view already exists, so we
            should optimistically attempt to update it.

        Returns:
            The Table that was just created.
        """

    @abc.abstractmethod
    def drop_row_level_permissions(self, table: bigquery.Table) -> None:
        """Removes all row access policies from the table."""

    @abc.abstractmethod
    def apply_row_level_permissions(self, table: bigquery.Table) -> None:
        """Applies the relevant row access policies to the table."""

    @abc.abstractmethod
    def load_table_from_cloud_storage_async(
        self,
        source_uris: List[str],
        destination_dataset_ref: bigquery.DatasetReference,
        destination_table_id: str,
        destination_table_schema: List[bigquery.SchemaField],
        write_disposition: str,
        skip_leading_rows: int = 0,
    ) -> bigquery.job.LoadJob:
        """Loads a table from CSV data in GCS to BigQuery.

        Given a desired table name, source data URI(s) and destination schema, loads the
        table into BigQuery.

        This starts the job, but does not wait until it completes.

        The table is created if it does not exist, and overwritten with the contents of
        the new data if they do exist and the write_disposition is WRITE_TRUNCATE. If
        the write_disposition is WRITE_APPEND, data will be added to the table if it
        already exists.

        Args:
            source_uris: The paths in Google Cloud Storage to read contents from (starts with 'gs://').
            destination_dataset_ref: The BigQuery dataset to load the table into. Gets created
                if it does not already exist.
            destination_table_id: String name of the table to import.
            destination_table_schema: Defines a list of field schema information for each expected column in the input
                file.
            write_disposition: Indicates whether BigQuery should overwrite the table
                completely (WRITE_TRUNCATE) or adds to the table with new rows
                (WRITE_APPEND). By default, WRITE_APPEND is used.
            skip_leading_rows: Optional number of leading rows to skip on each input
                file. Defaults to zero
        Returns:
            The LoadJob object containing job details.
        """

    @abc.abstractmethod
    def load_into_table_from_dataframe_async(
        self,
        source: pd.DataFrame,
        destination_dataset_ref: bigquery.DatasetReference,
        destination_table_id: str,
    ) -> bigquery.job.LoadJob:
        """Loads a table from a pandas DataFrame to BigQuery.

        Given a desired table name, source DataFrame and destination schema, loads the table into BigQuery.
        If schema information is not provided for a given column, its type will be inferred from the DataFrame.

        This starts the job, but does not wait until it completes.

        Tables are created if they do not exist.

        Args:
            source: A DataFrame containing the contents to load into the table.
            destination_dataset_ref: The BigQuery dataset to load the table into. Gets created
                if it does not already exist.
            destination_table_id: String name of the table to import.
        Returns:
            The LoadJob object containing job details.
        """

    @abc.abstractmethod
    def load_into_table_from_file_async(
        self,
        source: IO,
        destination_dataset_ref: bigquery.DatasetReference,
        destination_table_id: str,
        schema: List[bigquery.SchemaField],
    ) -> bigquery.job.LoadJob:
        """Loads a table from a file to BigQuery.

        Given a desired table name, source file handle, and schema, loads the table into BigQuery.

        This starts the job, but does not wait until it completes.

        Tables are created if they do not exist.

        Args:
            source: A file handle opened in binary mode for reading.
            destination_dataset_ref: The BigQuery dataset to load the table into. Gets created
                if it does not already exist.
            destination_table_id: String name of the table to import.
            schema: The BigQuery schema of the destination table. Additional columns can be added
                to the schema in this manner, but not removed.
        Returns:
            The LoadJob object containing job details.
        """

    @abc.abstractmethod
    def export_table_to_cloud_storage_async(
        self,
        source_table_dataset_ref: bigquery.dataset.DatasetReference,
        source_table_id: str,
        destination_uri: str,
        destination_format: str,
        print_header: bool,
    ) -> Optional[bigquery.ExtractJob]:
        """Exports the table corresponding to the given view to the path in Google Cloud Storage denoted by
        |destination_uri|.

        Extracts the entire table and exports in the specified format to the given bucket in Cloud Storage.

        It is the caller's responsibility to wait for the resulting job to complete.

        Args:
            source_table_dataset_ref: The BigQuery dataset where the table exists.
            source_table_id: The string table name to export to cloud storage.
            destination_uri: The path in Google Cloud Storage to write the contents of the table to (starts with
                'gs://').
            destination_format: The format the contents of the table should be outputted as (e.g. CSV or
                NEWLINE_DELIMITED_JSON).
            print_header: Indicates whether to print out a header row in the results.

        Returns:
            The ExtractJob object containing job details, or None if the job fails to start.
        """

    @abc.abstractmethod
    def export_query_results_to_cloud_storage(
        self,
        *,
        export_configs: List[ExportQueryConfig],
        print_header: bool,
        use_query_cache: bool,
    ) -> None:
        """Exports the queries to cloud storage according to the given configs.

        This is a three-step process. First, each query is executed and the entire result is loaded into a temporary
        table in BigQuery. Then, for each table, the contents are exported to the cloud storage bucket in the format
        specified in the config. Finally, once all exports are complete, the temporary tables are deleted.

        The query output must be materialized in a table first because BigQuery doesn't support exporting a view or
        query directly.

        This runs synchronously and waits for the jobs to complete.

        Args:
            export_configs: List of queries along with how to export their results.
            print_header: Indicates whether to print out a header row in the results.
            use_query_cache: Whether to look for the result in the query cache. See:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery.FIELDS.use_query_cache
        """

    @abc.abstractmethod
    def run_query_async(
        self,
        *,
        query_str: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        use_query_cache: bool,
    ) -> bigquery.QueryJob:
        """Runs a query in BigQuery asynchronously.

        It is the caller's responsibility to wait for the resulting job to complete.

        Note: treating the resulting job like an iterator waits implicitly. For example:

            query_job = client.run_query_async(query_str)
            for row in query_job:
                ...

        Args:
            query_str: The query to execute
            query_parameters: Parameters for the query
            use_query_cache: Whether to look for the result in the query cache. See:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery.FIELDS.use_query_cache

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def paged_read_and_process(
        self,
        query_job: bigquery.QueryJob,
        page_size: int,
        process_page_fn: Callable[[List[bigquery.table.Row]], None],
    ) -> None:
        """Reads the given result set from the given query job in pages to limit how many rows are read into memory at
        any given time, processing the results of each row with the given callable.

        Args:
            query_job: the query job from which to process results.
            page_size: the maximum number of rows to read in at a time.
            process_page_fn: a callable function which takes in the paged rows and performs some operation.
        """

    @abc.abstractmethod
    def copy_view(
        self,
        view: BigQueryView,
        destination_client: "BigQueryClient",
        destination_dataset_ref: bigquery.DatasetReference,
    ) -> bigquery.Table:
        """Copies a view from this client's project to a destination project and dataset. If the dataset does not
        already exist, a new one will be created.

        This runs synchronously and waits for the job to complete.

        Args:
            view: The view to copy.
            destination_client: A BigQueryClient for the destination project. Can be the this client instance if the
                copy is being performed within the same project.
            destination_dataset_ref: A reference to the dataset within the destination project.

        Returns:
            The Table (view) just created.

        """

    @abc.abstractmethod
    def create_table_from_query_async(
        self,
        *,
        dataset_id: str,
        table_id: str,
        query: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        overwrite: Optional[bool] = False,
        clustering_fields: Optional[List[str]] = None,
        use_query_cache: bool,
    ) -> bigquery.QueryJob:
        """Creates a table at the given address with the output from the given query.
        If overwrite is False, a 'duplicate' error is returned in the job result if the
        table already exists and contains data. If overwrite is True, overwrites the
        table if it already exists.

        Args:
            dataset_id: The name of the dataset where the table should be created.
            table_id: The name of the table to be created.
            query: The query to run. The result will be loaded into the new table.
            query_parameters: Optional parameters for the query
            overwrite: Whether or not to overwrite an existing table.
            clustering_fields: Columns by which to cluster the table.
            use_query_cache: Whether to look for the result in the query cache. See:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery.FIELDS.use_query_cache

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def insert_into_table_from_table_async(
        self,
        *,
        source_dataset_id: str,
        source_table_id: str,
        destination_dataset_id: str,
        destination_table_id: str,
        use_query_cache: bool,
        source_data_filter_clause: Optional[str] = None,
        hydrate_missing_columns_with_null: bool = False,
        allow_field_additions: bool = False,
        write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
    ) -> bigquery.QueryJob:
        """Inserts rows from the source table into the destination table. May include an optional filter clause
        to only insert a subset of rows into the destination table.

        Args:
            source_dataset_id: The name of the source dataset.
            source_table_id: The name of the source table from which to query.
            destination_dataset_id: The name of the destination dataset.
            destination_table_id: The name of the table to insert into.
            source_data_filter_clause: An optional clause to filter the contents of the source table that are inserted
                into the destination table. Must start with "WHERE".
            hydrate_missing_columns_with_null: If True, schema fields in the destination table that are missing
                from the source table will be selected as NULL. Defaults to False. If this is False, the request will
                fail if the source table/query is missing columns that the destination table has.
            allow_field_additions: Whether or not to allow new columns to be created in the destination table if the
                schema in the source table does not exactly match the destination table. Defaults to False. If this is
                False, the request will fail if the destination table is missing columns that the source table/query
                has.
            write_disposition: Indicates whether BigQuery should overwrite the table completely (WRITE_TRUNCATE) or
                adds to the table with new rows (WRITE_APPEND). By default, WRITE_APPEND is used.
            use_query_cache: Whether to look for the result in the query cache. See:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery.FIELDS.use_query_cache

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def insert_into_table_from_query_async(
        self,
        *,
        destination_dataset_id: str,
        destination_table_id: str,
        query: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        allow_field_additions: bool = False,
        write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
        clustering_fields: Optional[List[str]] = None,
        use_query_cache: bool,
    ) -> bigquery.QueryJob:
        """Inserts the results of the given query into the table at the given address.
        Creates a table if one does not yet exist. If |allow_field_additions| is set to
        False and the table exists, the schema of the query result must match the schema
        of the destination table.

        Args:
            destination_dataset_id: The name of the dataset where the result should be inserted.
            destination_table_id: The name of the table where the result should be inserted.
            query: The query to run. The result will be loaded into the table.
            query_parameters: Optional parameters for the query.
            allow_field_additions: Whether or not to allow new columns to be created in the destination table if the
                schema in the query result does not exactly match the destination table. Defaults to False.
            write_disposition: What to do if the destination table already exists. Defaults to WRITE_APPEND, which will
                append rows to an existing table.
            clustering_fields: Columns by which to cluster the table.
            use_query_cache: Whether to look for the result in the query cache. See:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery.FIELDS.use_query_cache

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def stream_into_table(
        self,
        dataset_ref: bigquery.DatasetReference,
        table_id: str,
        rows: Sequence[Dict[str, Any]],
    ) -> None:
        """Inserts the provided rows into the specified table.

        The table must already exist, and the rows must conform to the existing schema.

        Args:
            dataset_ref: The dataset containing the table into which the rows should be inserted.
            table_id: The name of the table into which the rows should be inserted.
            rows: A sequence of dictionaries representing the rows to insert into the table.
        """

    @abc.abstractmethod
    def load_into_table_async(
        self,
        dataset_ref: bigquery.DatasetReference,
        table_id: str,
        rows: Sequence[Dict[str, Any]],
        *,
        write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
    ) -> bigquery.job.LoadJob:
        """Inserts the provided rows into the specified table.

        The table must already exist, and the rows must conform to the existing schema.

        Args:
            dataset_ref: The dataset containing the table into which the rows should be inserted.
            table_id: The name of the table into which the rows should be inserted.
            rows: A sequence of dictionaries representing the rows to insert into the table.
            write_disposition: What to do if the destination table already exists. Defaults to WRITE_APPEND, which will
                append rows to an existing table.
        Returns:
            The LoadJob object containing job details.
        """

    @abc.abstractmethod
    def delete_from_table_async(
        self, dataset_id: str, table_id: str, filter_clause: Optional[str] = None
    ) -> bigquery.QueryJob:
        """Deletes rows from the given table that match the filter clause. If no filter is provided, all rows are
        deleted.

        Args:
            dataset_id: The name of the dataset where the table lives.
            table_id: The name of the table to delete from.
            filter_clause: An optional clause that filters the contents of the table to determine which rows should be
             deleted. If present, it must start with "WHERE".

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def materialize_view_to_table(
        self, view: BigQueryView, use_query_cache: bool
    ) -> bigquery.Table:
        """Materializes the result of a view's view_query into a table. The view's
        materialized_address must be set. The resulting table is put in the same
        project as the view, and it overwrites any previous materialization of the view.

        Args:
            view: The BigQueryView to materialize into a table.
            use_query_cache: Whether to look for the result in the query cache. See:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery.FIELDS.use_query_cache
        """

    @abc.abstractmethod
    def create_table_with_schema(
        self,
        dataset_id: str,
        table_id: str,
        schema_fields: List[bigquery.SchemaField],
        clustering_fields: Optional[List[str]] = None,
        date_partition_field: Optional[str] = None,
    ) -> bigquery.Table:
        """Creates a table in the given dataset with the given schema fields. Raises an
        error if a table with the same table_id already exists in the dataset.

        Args:
            dataset_id: The name of the dataset where the table should be created
            table_id: The name of the table to be created
            schema_fields: A list of fields defining the table's schema
            clustering_fields: A list of fields to cluster the table by. The clustering
                columns that are specified are used to co-locate related data. For more:
                https://cloud.google.com/bigquery/docs/clustered-tables.
            date_partition_field: The name of a single field to partition this table on
                using TimePartitioningType.DAY. Field may have time DATE, DATETIME or
                TIMESTAMP. See https://cloud.google.com/bigquery/docs/partitioned-tables#partitioning_versus_clustering
                for details on the tradeoffs of clustering vs partitioning. You can also combine partitioning with
                clustering. Data is first partitioned and then data in each partition is
                clustered by the clustering columns.

        Returns:
            The bigquery.Table that is created.
        """

    @abc.abstractmethod
    def update_schema(
        self,
        dataset_id: str,
        table_id: str,
        desired_schema_fields: List[bigquery.SchemaField],
        allow_field_deletions: bool = True,
    ) -> None:
        """Updates the schema of the table to match the desired_schema_fields. This may result in both adding and
        dropping fields from the table's schema. Raises an exception if fields in desired_schema_fields conflict with
        existing fields' modes or types.

        Args:
            dataset_id: The name of the dataset where the table lives.
            table_id: The name of the table to modify.
            desired_schema_fields: A list of fields describing the desired table schema.
            allow_field_deletions: A boolean of whether we should delete excess fields or not
        """

    @abc.abstractmethod
    def delete_table(
        self, dataset_id: str, table_id: str, not_found_ok: bool = False
    ) -> None:
        """Provided the |dataset_id| and |table_id|, attempts to delete the given table from BigQuery.

        Args:
            dataset_id: The name of the dataset where the table lives.
            table_id: The name of the table to delete.
            not_found_ok: If False, this raises an exception when the table is not found.
        """

    @abc.abstractmethod
    def set_table_expiration(
        self, dataset_id: str, table_id: str, expiration: datetime.datetime
    ) -> None:
        """Set the table expiration to `expiration` for the given table.

        Args:
            dataset_id: The name of the dataset where the table lives.
            table_id: The name of the table to update.
            expiration: The datetime when the table should expire.
        """

    @abc.abstractmethod
    def copy_dataset_tables_across_regions(
        self,
        source_dataset_id: str,
        destination_dataset_id: str,
        overwrite_destination_tables: bool = False,
        timeout_sec: float = DEFAULT_CROSS_REGION_COPY_TIMEOUT_SEC,
    ) -> None:
        """Copies tables (but NOT views) from |source_dataset_id| to
        |destination_dataset_id|. This only works if the datasets live in different
        regions (e.g. 'us' vs 'us-east1'), because the API for doing an inter-region
        dataset copy is inexplicably different than for copies between regions.

        If `overwrite_destination_tables` is set, then any tables that already exist in
        the destination dataset will be overwritten with the contents from the source
        dataset, or removed if no matching table exists in the source dataset.
        """

    @abc.abstractmethod
    def copy_dataset_tables(
        self,
        source_dataset_id: str,
        destination_dataset_id: str,
        schema_only: bool = False,
        overwrite_destination_tables: bool = False,
    ) -> None:
        """Copies all tables in the source dataset to the destination dataset,
        which must be empty if it exists. If the destination dataset does not exist,
        we will create one.

        If `schema_only` is provided, creates a matching set of tables with the same
        schema in the destination dataset but does not copy contents of each table. If
        `schema_only` is provided, copies the schema of tables *and* views.

        If `schema_only` is False, *DOES NOT COPY VIEWS*.
        """

    @abc.abstractmethod
    def copy_table(
        self,
        source_dataset_id: str,
        source_table_id: str,
        destination_dataset_id: str,
        schema_only: bool = False,
        overwrite: bool = False,
    ) -> Optional[bigquery.job.CopyJob]:
        """Copies the table in the source dataset to the destination dataset,
        which must be empty if it exists. If the destination dataset does not
        exist, we will create one.

        If `schema_only` is provided, creates a matching table with the same schema
        in the destination dataset but does not copy contents of the table.
        """

    @abc.abstractmethod
    def backup_dataset_tables_if_dataset_exists(
        self, dataset_id: str
    ) -> Optional[bigquery.DatasetReference]:
        """Creates a backup of all tables (but NOT views) in |dataset_id| in a new
        dataset with the format `my_dataset_backup_yyyy_mm_dd`. For example, the dataset
        `state` might backup to `state_backup_2021_05_06`.
        """

    @abc.abstractmethod
    def update_datasets_to_match_reference_schema(
        self, reference_dataset_id: str, stale_schema_dataset_ids: List[str]
    ) -> None:
        """Updates the schemas of the datasets in |stale_schema_dataset_ids| to match
        the schema of the reference dataset. This means:
          - Adding tables that exist in the reference but not in the stale schema dataset
          - Deleting tables that do not exist in the reference dataset
          - Updating the schemas of matching tables to be equal

        Disclaimer: Use with caution! This will delete data.
        """

    @abc.abstractmethod
    def wait_for_big_query_jobs(self, jobs: Sequence[PollingFuture]) -> List[Any]:
        """Waits for a list of jobs to complete. These can by any of the async job types
        the BQ client returns, e.g. QueryJob, CopyJob, LoadJob, etc.

        If any job throws this function will throw with the first encountered exception.
        """

    @abc.abstractmethod
    def add_timestamp_suffix_to_dataset_id(self, dataset_id: str) -> str:
        """Adds a timestamp to the end of the dataset name with the following format:
        'dataset_id_YYYY_MM_DD_HH_MM_SS_mmmmmm'
        """


class BigQueryClientImpl(BigQueryClient):
    """Wrapper around the bigquery.Client with convenience functions for querying, creating, copying and exporting
    BigQuery tables and views.
    """

    def __init__(
        self, project_id: Optional[str] = None, region_override: Optional[str] = None
    ):
        if not project_id:
            project_id = metadata.project_id()

        if not project_id:
            raise ValueError(
                "Must provide a project_id if metadata.project_id() returns None"
            )

        self._project_id = project_id
        self.region = region_override or self.DEFAULT_REGION
        self.client = client(self._project_id, region=self.region)

    @property
    def project_id(self) -> str:
        return self._project_id

    def dataset_ref_for_id(self, dataset_id: str) -> bigquery.DatasetReference:
        return bigquery.DatasetReference.from_string(
            dataset_id, default_project=self._project_id
        )

    def create_dataset_if_necessary(
        self,
        dataset_ref: bigquery.DatasetReference,
        default_table_expiration_ms: Optional[int] = None,
    ) -> bigquery.Dataset:
        try:
            dataset = self.client.get_dataset(dataset_ref)
        except exceptions.NotFound:
            logging.info("Dataset [%s] does not exist. Creating...", str(dataset_ref))
            dataset_to_create = bigquery.Dataset(dataset_ref)

            if default_table_expiration_ms:
                logging.info(
                    "Setting default table expiration to %d milliseconds for dataset [%s].",
                    default_table_expiration_ms,
                    str(dataset_ref),
                )
                dataset_to_create.default_table_expiration_ms = (
                    default_table_expiration_ms
                )

            created_dataset = self.client.create_dataset(
                dataset_to_create,
                # Do not fail if another process has already created this dataset
                # between the get_dataset() call and now.
                exists_ok=True,
            )

            # Set labels to keep Vanta happy.
            # Note: It is possible that another process is creating and adding labels to
            # the dataset at the same time. We use a new `dataset_to_update` instead of
            # the `created_dataset` so that the `etag` is None and we overwrite the
            # labels no matter what (i.e. last writer wins).
            dataset_to_update = bigquery.Dataset(dataset_ref)
            owner, description = self._get_owner_and_description(created_dataset)
            dataset_to_update.description = description
            if owner:
                owner = self._ensure_valid_bigquery_label_value(owner)
            else:
                owner = DEFAULT_VANTA_DATASET_OWNER
            description_label = self._ensure_valid_bigquery_label_value(description)
            dataset_to_update.labels = {
                "vanta-owner": owner,
                "vanta-description": description_label,
            }
            return self.client.update_dataset(
                dataset_to_update, ["description", "labels"]
            )

        return dataset

    def _ensure_valid_bigquery_label_value(self, label_value: str) -> str:
        """Ensures that labels meet BigQuery requirements.

        The requirements for values are that they can contain only lowercase letters, numeric characters, underscores,
        and dashes, and have a maximum length of 63 characters: https://cloud.google.com/bigquery/docs/labels-intro.
        This method converts the label string to lowercase, replaces any disallowed characters with a dash, and
        truncates the length to 63.
        """

        label_value = re.sub(r"[^\w_-]", "-", label_value.lower())
        if len(label_value) > 63:
            label_value = label_value[:63]
        return label_value

    def _get_owner_and_description(
        self, dataset: bigquery.Dataset
    ) -> Tuple[Optional[str], str]:
        """Gets the individual owner (if there is one) and a description of the dataset.

        The dataset must already exist, otherwise there will not be any owners.
        """
        date = datetime.date.today().isoformat()
        # __main__ has no __file__ attribute when pytest-xdist is running the process
        if in_test() and not hasattr(__main__, "__file__"):
            script_name = "UNKNOWN"
        else:
            script_name = os.path.splitext(os.path.basename(__main__.__file__))[0]
        # Get "individual" (non service account) owners of the dataset
        owner_emails = [
            entry.entity_id
            for entry in dataset.access_entries
            if entry.role == "OWNER"
            and entry.entity_type == bigquery.enums.EntityTypes.USER_BY_EMAIL
            and entry.entity_id
        ]

        # If there are any, then we know it was created manually by an individual.
        if owner_emails:
            owner_usernames = [
                email.split("@")[0]
                for email in owner_emails
                if email.endswith("@recidiviz.org")
            ]
            owner = owner_usernames[0] if owner_usernames else None
            return (
                owner,
                f"Generated from {script_name} by {owner or 'unknown'} on {date}",
            )

        # Otherwise, it was created by infrastructure.
        return None, f"Generated automatically by infrastructure on {date}"

    def dataset_exists(self, dataset_ref: bigquery.DatasetReference) -> bool:
        try:
            self.client.get_dataset(dataset_ref)
            return True
        except exceptions.NotFound:
            return False

    def dataset_is_empty(self, dataset_ref: bigquery.DatasetReference) -> bool:
        tables = peekable(self.client.list_tables(dataset_ref.dataset_id))
        routines = peekable(self.client.list_routines(dataset_ref.dataset_id))

        return not tables and not routines

    def delete_dataset(
        self,
        dataset_ref: bigquery.DatasetReference,
        delete_contents: bool = False,
        not_found_ok: bool = False,
    ) -> None:
        return self.client.delete_dataset(
            dataset_ref, delete_contents=delete_contents, not_found_ok=not_found_ok
        )

    def get_dataset(self, dataset_ref: bigquery.DatasetReference) -> bigquery.Dataset:
        return self.client.get_dataset(dataset_ref)

    def list_datasets(self) -> Iterator[bigquery.dataset.DatasetListItem]:
        return self.client.list_datasets()

    def table_exists(
        self, dataset_ref: bigquery.DatasetReference, table_id: str
    ) -> bool:
        table_ref = dataset_ref.table(table_id)

        try:
            self.client.get_table(table_ref)
            return True
        except exceptions.NotFound:
            return False

    def _build_create_row_access_policy_query(
        self,
        policy_name: str,
        table: bigquery.Table,
        access_group_email: str,
        filter_clause: str,
    ) -> str:
        return f"""CREATE OR REPLACE ROW ACCESS POLICY
                {policy_name}
                ON `{table.project}.{table.dataset_id}.{table.table_id}`
                GRANT TO ("group:{access_group_email}") 
                FILTER USING ({filter_clause});
                """

    def table_has_field(self, table: bigquery.Table, field: str) -> bool:
        return any(f.name == field for f in table.schema)

    def states_with_special_access_policies(self) -> Dict[str, str]:
        restricted_access_state_code_to_access_group: Dict[str, str] = {
            "US_OZ": "s-oz-data@recidiviz.org",
            # TODO(#20822) "US_MI": "s-mi-data@recidiviz.org",
            # TODO(#20823) "US_PA": "s-pa-data@recidiviz.org",
        }
        return restricted_access_state_code_to_access_group

    def apply_column_based_row_level_policies(
        self, table: bigquery.Table, state_code_field: str
    ) -> None:
        """Applies row-level permissions to the provided table based on the state code
        in the provided |state_code_field| column.
        """
        restricted_access_state_code_to_access_group = (
            self.states_with_special_access_policies()
        )

        logging.info(
            "Applying row-level access policies to [%s.%s.%s] based on field [%s]",
            table.project,
            table.dataset_id,
            table.table_id,
            state_code_field,
        )
        queries = []
        for (
            state_code,
            access_group_email,
        ) in restricted_access_state_code_to_access_group.items():
            # Create explicit row-level policies for states with rigid access
            # control requirements.
            queries.append(
                self._build_create_row_access_policy_query(
                    policy_name=f"EXPLICIT_ACCESS_TO_{state_code}_{state_code_field}",
                    table=table,
                    access_group_email=access_group_email,
                    filter_clause=f'{state_code_field} = "{state_code}"',
                )
            )

        filtered_states_str = ", ".join(
            [
                f'"{state_code}"'
                for state_code in restricted_access_state_code_to_access_group
            ]
        )
        # Create policy for all states with lax access control. This should
        # handle row access to most state data
        queries.append(
            self._build_create_row_access_policy_query(
                policy_name=f"NON_RESTRICTIVE_STATE_DATA_ACCESS_{state_code_field}",
                table=table,
                access_group_email="s-default-state-data@recidiviz.org",
                filter_clause=f"{state_code_field} NOT IN ({filtered_states_str})",
            )
        )
        # Create policy to grant admins access to view, edit and copy ALL state
        # data
        queries.append(
            self._build_create_row_access_policy_query(
                policy_name=f"ADMIN_ACCESS_TO_ALL_STATE_DATA_{state_code_field}",
                table=table,
                access_group_email="s-big-query-admins@recidiviz.org",
                filter_clause="TRUE",
            )
        )
        query = "\n".join(queries)
        query_job = self.client.query(query)
        query_job.result()

    def restricted_access_dataset_to_access_group(self) -> Dict[str, str]:
        """All tables belonging to datasets defined in this function should
        have row level permissions applied
        """
        restricted_access_state_code_to_access_group = (
            self.states_with_special_access_policies()
        )
        restricted_access_dataset_to_access_group = {}
        for (
            state_code,
            security_group,
        ) in restricted_access_state_code_to_access_group.items():
            for instance in DirectIngestInstance:
                instance_datasets_with_raw_data = [
                    raw_tables_dataset_for_region(
                        state_code=StateCode(state_code),
                        instance=instance,
                        sandbox_dataset_prefix=None,
                    ),
                    raw_latest_views_dataset_for_region(
                        state_code=StateCode(state_code),
                        instance=instance,
                        sandbox_dataset_prefix=None,
                    ),
                ]
                for dataset in instance_datasets_with_raw_data:
                    restricted_access_dataset_to_access_group[dataset] = security_group

        return restricted_access_dataset_to_access_group

    def apply_dataset_based_row_level_policies(self, table: bigquery.Table) -> None:
        """Applies row-level permissions to all rows in the provided table, if it is
        an identified restricted-access dataset (e.g. a raw data dataset).
        """
        restricted_access_dataset_to_access_group = (
            self.restricted_access_dataset_to_access_group()
        )
        if table.dataset_id not in restricted_access_dataset_to_access_group:
            return

        logging.info(
            """Applying row-level access policy to [%s.%s.%s], 
            which is in a restricted-access dataset.""",
            table.project,
            table.dataset_id,
            table.table_id,
        )
        queries = []
        # Create policy for that allows member of the state data security group
        # to access all records
        queries.append(
            self._build_create_row_access_policy_query(
                policy_name="RESTRICT_DATASET_TO_MEMBERS_OF_STATE_SECURITY_GROUP",
                table=table,
                access_group_email=restricted_access_dataset_to_access_group[
                    table.dataset_id
                ],
                filter_clause="TRUE",
            )
        )
        # Create policy to grant admins access to view, edit and copy all rows
        queries.append(
            self._build_create_row_access_policy_query(
                policy_name="ADMIN_ACCESS_TO_ALL_ROWS",
                table=table,
                access_group_email="s-big-query-admins@recidiviz.org",
                filter_clause="TRUE",
            )
        )
        query = "\n".join(queries)
        query_job = self.client.query(query)
        query_job.result()

    def drop_row_level_permissions(self, table: bigquery.Table) -> None:
        query = f"""
            -- Dropping row-level permissions 
            DROP ALL ROW ACCESS POLICIES 
            ON `{table.project}.{table.dataset_id}.{table.table_id}`
        """
        query_job = self.client.query(query)
        query_job.result()

    def apply_row_level_permissions(self, table: bigquery.Table) -> None:
        # Checking that view_query is None to verify that it is a table. Row
        # level permissions cannot be applied to views.
        if table.view_query is not None:
            logging.info(
                "Will not apply row_level_permissions to view [%s.%s] - returning.",
                table.dataset_id,
                table.table_id,
            )
            return
        self.drop_row_level_permissions(table)
        for field in ["state_code", "region_code"]:
            if self.table_has_field(table, field):
                self.apply_column_based_row_level_policies(table, field)

        # We need to apply row level permissions to all tables that have
        # dataset-level restricted access policies, even if they have no
        # state_code/region_code columns
        if table.dataset_id in self.restricted_access_dataset_to_access_group():
            self.apply_dataset_based_row_level_policies(table)

    def list_tables(self, dataset_id: str) -> Iterator[bigquery.table.TableListItem]:
        return self.client.list_tables(dataset_id)

    def list_tables_excluding_views(
        self, dataset_id: str
    ) -> Iterator[bigquery.table.TableListItem]:
        return (
            table
            for table in self.client.list_tables(dataset_id)
            if table.table_type == "TABLE"
        )

    def get_row_counts_for_tables(self, dataset_id: str) -> Dict[str, int]:
        if not self.dataset_exists(
            self.dataset_ref_for_id(dataset_id)
        ) or self.dataset_is_empty(self.dataset_ref_for_id(dataset_id)):
            return {}

        results = self.run_query_async(
            query_str=f"""
                SELECT _TABLE_SUFFIX as table_id, COUNT(*) as num_rows
                FROM `{self.project_id}.{dataset_id}.*`
                GROUP BY _TABLE_SUFFIX
                """,
            use_query_cache=False,
        )
        return {row["table_id"]: row["num_rows"] for row in results}

    def get_table(
        self, dataset_ref: bigquery.DatasetReference, table_id: str
    ) -> bigquery.Table:
        table_ref = dataset_ref.table(table_id)
        return self.client.get_table(table_ref)

    def create_table(
        self, table: bigquery.Table, overwrite: bool = False
    ) -> bigquery.Table:
        return self.client.create_table(table, exists_ok=overwrite)

    def create_or_update_view(
        self, view: BigQueryView, might_exist: bool = True
    ) -> bigquery.Table:
        if not view.should_deploy():
            raise ValueError(
                f"Cannot create / update view [{view.address}] - should_deploy() is "
                f"False."
            )
        bq_view = bigquery.Table(view)
        bq_view.view_query = view.view_query
        bq_view.description = view.bq_description

        try:
            if might_exist:
                try:
                    logging.info("Optimistically updating view [%s]", str(bq_view))
                    return self.client.update_table(
                        bq_view, ["view_query", "description"]
                    )
                except exceptions.NotFound:
                    logging.info(
                        "Creating view [%s] as it was not found while attempting to update",
                        str(bq_view),
                    )
                    return self.client.create_table(bq_view)
            else:
                logging.info("Creating view [%s]", str(bq_view))
                return self.client.create_table(bq_view)
        except exceptions.BadRequest as e:
            raise ValueError(
                f"Cannot update view query for [{view.address.to_str()}]"
            ) from e

    def load_table_from_cloud_storage_async(
        self,
        source_uris: List[str],
        destination_dataset_ref: bigquery.DatasetReference,
        destination_table_id: str,
        destination_table_schema: List[bigquery.SchemaField],
        write_disposition: str,
        skip_leading_rows: int = 0,
    ) -> bigquery.job.LoadJob:
        """Triggers a load job, i.e. a job that will copy all of the data from the given
        Cloud Storage source into the given BigQuery destination. Returns once the job
        has been started.
        """
        self._validate_schema(
            BigQueryAddress(
                dataset_id=destination_dataset_ref.dataset_id,
                table_id=destination_table_id,
            ),
            destination_table_schema,
        )
        self.create_dataset_if_necessary(destination_dataset_ref)

        destination_table_ref = destination_dataset_ref.table(destination_table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.schema = destination_table_schema
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.allow_quoted_newlines = True
        job_config.write_disposition = write_disposition
        job_config.skip_leading_rows = skip_leading_rows

        load_job = self.client.load_table_from_uri(
            source_uris, destination_table_ref, job_config=job_config
        )

        logging.info(
            "Started load job [%s] for table [%s.%s.%s]",
            load_job.job_id,
            destination_table_ref.project,
            destination_table_ref.dataset_id,
            destination_table_ref.table_id,
        )

        return load_job

    def load_into_table_from_dataframe_async(
        self,
        source: pd.DataFrame,
        destination_dataset_ref: bigquery.DatasetReference,
        destination_table_id: str,
    ) -> bigquery.job.LoadJob:
        self.create_dataset_if_necessary(destination_dataset_ref)

        destination_table_ref = destination_dataset_ref.table(destination_table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.allow_quoted_newlines = True
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND

        load_job = self.client.load_table_from_dataframe(
            source, destination_table_ref, job_config=job_config
        )

        logging.info(
            "Started load job [%s] for table [%s.%s.%s]",
            load_job.job_id,
            destination_table_ref.project,
            destination_table_ref.dataset_id,
            destination_table_ref.table_id,
        )

        return load_job

    def load_into_table_from_file_async(
        self,
        source: IO,
        destination_dataset_ref: bigquery.DatasetReference,
        destination_table_id: str,
        schema: List[bigquery.SchemaField],
    ) -> bigquery.job.LoadJob:
        self._validate_schema(
            BigQueryAddress(
                dataset_id=destination_dataset_ref.dataset_id,
                table_id=destination_table_id,
            ),
            schema,
        )
        self.create_dataset_if_necessary(destination_dataset_ref)

        destination_table_ref = destination_dataset_ref.table(destination_table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.allow_quoted_newlines = True
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_APPEND
        job_config.schema_update_options = [
            bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
        ]
        job_config.schema = schema

        load_job = self.client.load_table_from_file(
            source,
            destination_table_ref,
            job_config=job_config,
            rewind=True,  # ensure we're at the beginning of the file
        )

        logging.info(
            "Started load job [%s] for table [%s.%s.%s]",
            load_job.job_id,
            destination_table_ref.project,
            destination_table_ref.dataset_id,
            destination_table_ref.table_id,
        )

        return load_job

    def export_table_to_cloud_storage_async(
        self,
        source_table_dataset_ref: bigquery.DatasetReference,
        source_table_id: str,
        destination_uri: str,
        destination_format: str,
        print_header: bool,
    ) -> Optional[bigquery.ExtractJob]:
        if not print_header and destination_format != bigquery.DestinationFormat.CSV:
            raise ValueError(
                f"Export called incorrectly with print_header=False and destination_format={destination_format}"
            )

        if not self.table_exists(source_table_dataset_ref, source_table_id):
            logging.error(
                "Table [%s] does not exist in dataset [%s]",
                source_table_id,
                str(source_table_dataset_ref),
            )
            return None

        table_ref = source_table_dataset_ref.table(source_table_id)

        job_config = bigquery.ExtractJobConfig()
        job_config.destination_format = destination_format
        job_config.print_header = print_header

        return self.client.extract_table(
            table_ref,
            destination_uri,
            # Location must match that of the source table.
            location=self.region,
            job_config=job_config,
        )

    def export_query_results_to_cloud_storage(
        self,
        *,
        export_configs: List[ExportQueryConfig],
        print_header: bool,
        use_query_cache: bool,
    ) -> None:
        try:
            query_jobs = []
            for export_config in export_configs:
                query_job = self.create_table_from_query_async(
                    dataset_id=export_config.intermediate_dataset_id,
                    table_id=export_config.intermediate_table_name,
                    query=export_config.query,
                    query_parameters=export_config.query_parameters,
                    overwrite=True,
                    use_query_cache=use_query_cache,
                )
                if query_job is not None:
                    query_jobs.append(query_job)

            logging.info("Waiting on [%d] query jobs to finish", len(query_jobs))
            for query_job in query_jobs:
                query_job.result()

            logging.info("Completed [%d] query jobs.", len(query_jobs))

            extract_jobs_to_config = {}
            for export_config in export_configs:
                extract_job = self.export_table_to_cloud_storage_async(
                    self.dataset_ref_for_id(export_config.intermediate_dataset_id),
                    export_config.intermediate_table_name,
                    export_config.output_uri,
                    export_config.output_format,
                    print_header,
                )
                if extract_job is not None:
                    extract_jobs_to_config[extract_job] = export_config

            logging.info(
                "Waiting on [%d] extract jobs to finish", len(extract_jobs_to_config)
            )

            for export_job, export_config in extract_jobs_to_config.items():
                try:
                    export_job.result()
                except Exception as e:
                    logging.exception(
                        "Extraction failed for table: %s",
                        export_config.intermediate_table_name,
                    )
                    raise e
            logging.info("Completed [%d] extract jobs.", len(extract_jobs_to_config))

        finally:
            logging.info(
                "Deleting [%d] temporary intermediate tables.", len(export_configs)
            )
            for export_config in export_configs:
                self.delete_table(
                    dataset_id=export_config.intermediate_dataset_id,
                    table_id=export_config.intermediate_table_name,
                    not_found_ok=True,
                )
            logging.info("Done deleting temporary intermediate tables.")

    def delete_table(
        self, dataset_id: str, table_id: str, not_found_ok: bool = False
    ) -> None:
        dataset_ref = self.dataset_ref_for_id(dataset_id)
        table_ref = dataset_ref.table(table_id)
        logging.info(
            "Deleting table/view [%s] from dataset [%s].", table_id, dataset_id
        )
        self.client.delete_table(table_ref, not_found_ok=not_found_ok)

    def run_query_async(
        self,
        *,
        query_str: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        use_query_cache: bool,
    ) -> bigquery.QueryJob:
        job_config = bigquery.QueryJobConfig(use_query_cache=use_query_cache)
        job_config.query_parameters = query_parameters or []

        return self.client.query(
            query=query_str,
            location=self.region,
            job_config=job_config,
        )

    def paged_read_and_process(
        self,
        query_job: bigquery.QueryJob,
        page_size: int,
        process_page_fn: Callable[[List[bigquery.table.Row]], None],
    ) -> None:
        logging.debug(
            "Paginating results [%s] at a time, relying on [%s] to process each batch",
            page_size,
            process_page_fn.__name__,
        )

        rows_read: List[bigquery.table.Row] = []
        total_rows = 0

        # when we specify the page_size, each network call will only fetch page_size
        # results and do the pagination for us. we just iterate over the reuslts, and
        # at the end of each network call, calls process_page_fn with the results
        # that have been fetched for us.
        for total_rows, row in enumerate(query_job.result(page_size=page_size)):

            if total_rows != 0 and total_rows % page_size == 0:
                process_page_fn(rows_read)
                logging.info(
                    "Processed [%s] rows. Read [%s] rows total",
                    len(rows_read),
                    total_rows,
                )
                rows_read = []

            rows_read.append(row)

        if rows_read:
            process_page_fn(rows_read)
            logging.info(
                "Processed [%s] rows. Read [%s] rows total", len(rows_read), total_rows
            )

        logging.info(
            "Completed processing [%s] rows for [%s]",
            total_rows,
            process_page_fn.__name__,
        )

    def copy_view(
        self,
        view: BigQueryView,
        destination_client: BigQueryClient,
        destination_dataset_ref: bigquery.DatasetReference,
    ) -> bigquery.Table:
        if destination_client.table_exists(destination_dataset_ref, view.view_id):
            raise ValueError(f"Table [{view.view_id}] already exists in dataset!")

        # Create the destination dataset if it doesn't yet exist
        destination_client.create_dataset_if_necessary(destination_dataset_ref)

        new_view_ref = destination_dataset_ref.table(view.view_id)
        new_view = bigquery.Table(new_view_ref)
        new_view.view_query = StrictStringFormatter().format(
            view.view_query,
            project_id=destination_client.project_id,
            dataset_id=destination_dataset_ref.dataset_id,
            view_id=view.view_id,
        )

        # copy clustering fields, if found in source view
        if view.clustering_fields:
            new_view.clustering_fields = view.clustering_fields
        table = destination_client.create_table(new_view)
        logging.info("Created %s", new_view_ref)
        return table

    def create_table_from_query_async(
        self,
        *,
        dataset_id: str,
        table_id: str,
        query: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        overwrite: Optional[bool] = False,
        clustering_fields: Optional[List[str]] = None,
        use_query_cache: bool,
    ) -> bigquery.QueryJob:
        # If overwrite is False, errors if the table already exists and contains data. Else, overwrites the table if
        # it already exists.
        write_disposition = (
            bigquery.job.WriteDisposition.WRITE_TRUNCATE
            if overwrite
            else bigquery.job.WriteDisposition.WRITE_EMPTY
        )

        logging.info("Creating table: %s with query: %s", table_id, query)

        return self.insert_into_table_from_query_async(
            destination_dataset_id=dataset_id,
            destination_table_id=table_id,
            query=query,
            query_parameters=query_parameters,
            write_disposition=write_disposition,
            clustering_fields=clustering_fields,
            use_query_cache=use_query_cache,
        )

    def _insert_into_table_from_table_async(
        self,
        *,
        source_dataset_id: str,
        source_table_id: str,
        destination_dataset_id: str,
        destination_table_id: str,
        use_query_cache: bool,
        source_data_filter_clause: Optional[str] = None,
        allow_field_additions: bool = False,
        hydrate_missing_columns_with_null: bool = False,
        write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
    ) -> bigquery.QueryJob:
        """Loads data from a source table to a destination table, depending on the write_disposition passed in
        it can either append or overwrite the destination table. Defaults to WRITE_APPEND.
        """
        source_dataset_ref = self.dataset_ref_for_id(source_dataset_id)
        destination_dataset_ref = self.dataset_ref_for_id(destination_dataset_id)

        if not self.table_exists(destination_dataset_ref, destination_table_id):
            raise ValueError(
                f"Destination table [{self.project_id}.{destination_dataset_id}.{destination_table_id}]"
                f" does not exist!"
            )

        source_table = self.get_table(source_dataset_ref, source_table_id)
        destination_table = self.get_table(
            destination_dataset_ref, destination_table_id
        )

        select_columns = "*"

        if hydrate_missing_columns_with_null:
            schema_fields_missing_from_source = self._get_excess_schema_fields(
                source_table.schema, destination_table.schema
            )
            if schema_fields_missing_from_source:
                missing_columns = [
                    f"CAST(NULL AS {missing_column.field_type}) AS {missing_column.name}"
                    for missing_column in schema_fields_missing_from_source
                ]
                select_columns += f", {', '.join(missing_columns)}"

        query = f"SELECT {select_columns} FROM `{self.project_id}.{source_dataset_id}.{source_table_id}`"

        if source_data_filter_clause:
            self._validate_source_data_filter_clause(source_data_filter_clause)
            query = f"{query} {source_data_filter_clause}"

        logging.info(
            "Copying data from: %s.%s to: %s.%s",
            source_dataset_id,
            source_table_id,
            destination_dataset_id,
            destination_table_id,
        )

        return self.insert_into_table_from_query_async(
            destination_dataset_id=destination_dataset_id,
            destination_table_id=destination_table_id,
            query=query,
            allow_field_additions=allow_field_additions,
            write_disposition=write_disposition,
            use_query_cache=use_query_cache,
        )

    @staticmethod
    def _validate_source_data_filter_clause(filter_clause: str) -> None:
        if not filter_clause.startswith("WHERE"):
            raise ValueError(
                f"Found filter clause [{filter_clause}] that does not begin with WHERE"
            )

    def insert_into_table_from_query_async(
        self,
        *,
        destination_dataset_id: str,
        destination_table_id: str,
        query: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        allow_field_additions: bool = False,
        write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
        clustering_fields: Optional[List[str]] = None,
        use_query_cache: bool,
    ) -> bigquery.QueryJob:
        destination_dataset_ref = self.dataset_ref_for_id(destination_dataset_id)

        self.create_dataset_if_necessary(destination_dataset_ref)

        query_job_config = bigquery.job.QueryJobConfig(use_query_cache=use_query_cache)
        query_job_config.destination = destination_dataset_ref.table(
            destination_table_id
        )

        query_job_config.write_disposition = write_disposition
        query_job_config.query_parameters = query_parameters or []

        if clustering_fields:
            query_job_config.clustering_fields = clustering_fields

            # if new clustering fields are different, delete existing table
            # only if the write_disposition is WRITE_TRUNCATE
            try:
                existing_table = self.get_table(
                    destination_dataset_ref, destination_table_id
                )
                if existing_table.clustering_fields != clustering_fields:
                    if write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE:
                        self.delete_table(destination_dataset_id, destination_table_id)
                    else:
                        raise ValueError(
                            "Trying to materialize into a table using different "
                            "clustering fields than what currently exists requires "
                            "'WRITE_TRUNCATE' write_disposition."
                        )
            except exceptions.NotFound:
                pass

        if allow_field_additions:
            query_job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]

        logging.info(
            "Inserting into table [%s.%s] result of query: %s",
            destination_dataset_id,
            destination_table_id,
            query,
        )

        return self.client.query(
            query=query,
            location=self.region,
            job_config=query_job_config,
        )

    def insert_into_table_from_table_async(
        self,
        *,
        source_dataset_id: str,
        source_table_id: str,
        destination_dataset_id: str,
        destination_table_id: str,
        use_query_cache: bool,
        source_data_filter_clause: Optional[str] = None,
        hydrate_missing_columns_with_null: bool = False,
        allow_field_additions: bool = False,
        write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
    ) -> bigquery.QueryJob:
        return self._insert_into_table_from_table_async(
            source_dataset_id=source_dataset_id,
            source_table_id=source_table_id,
            destination_dataset_id=destination_dataset_id,
            destination_table_id=destination_table_id,
            source_data_filter_clause=source_data_filter_clause,
            hydrate_missing_columns_with_null=hydrate_missing_columns_with_null,
            allow_field_additions=allow_field_additions,
            write_disposition=write_disposition,
            use_query_cache=use_query_cache,
        )

    def stream_into_table(
        self,
        dataset_ref: bigquery.DatasetReference,
        table_id: str,
        rows: Sequence[Dict],
    ) -> None:
        logging.info(
            "Inserting %d rows into %s.%s", len(rows), dataset_ref.dataset_id, table_id
        )

        # Warn on any large rows
        for row in rows:
            estimated_size = total_size(row, include_duplicates=True)
            if estimated_size > (100 * 2**10):  # 100 KiB
                logging.warning("Row is larger than 100 KiB: %s", repr(row)[:1000])

        errors = self.client.insert_rows(self.get_table(dataset_ref, table_id), rows)
        if errors:
            raise RuntimeError(
                f"Failed to insert rows into {dataset_ref.dataset_id}.{table_id}:\n"
                + "\n".join(str(error) for error in errors)
            )

    def load_into_table_async(
        self,
        dataset_ref: bigquery.DatasetReference,
        table_id: str,
        rows: Sequence[Dict],
        *,
        write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
    ) -> bigquery.job.LoadJob:
        logging.info(
            "Inserting %d rows into %s.%s", len(rows), dataset_ref.dataset_id, table_id
        )

        # Warn on any large rows
        for row in rows:
            json_row = json.dumps(row)
            estimated_size = len(row)
            if estimated_size > (100 * 2**10):  # 100 KiB
                logging.warning("Row is larger than 100 KiB: %s", json_row[:1000])

        job_config = bigquery.LoadJobConfig()
        job_config.write_disposition = write_disposition

        return self.client.load_table_from_json(
            rows, self.get_table(dataset_ref, table_id), job_config=job_config
        )

    def delete_from_table_async(
        self, dataset_id: str, table_id: str, filter_clause: Optional[str] = None
    ) -> bigquery.QueryJob:
        if filter_clause and not filter_clause.startswith("WHERE"):
            raise ValueError(
                "Cannot delete from a table without a valid filter clause starting with WHERE."
            )

        # BQ requires a WHERE clause for all DELETE statements. The `WHERE true` filter deletes all rows.
        filter_str = filter_clause if filter_clause else "WHERE true"
        delete_query = (
            f"DELETE FROM `{self.project_id}.{dataset_id}.{table_id}` {filter_str}"
        )

        logging.info(
            "Deleting data from %s.%s matching this filter: %s",
            dataset_id,
            table_id,
            filter_str,
        )

        return self.client.query(delete_query)

    def materialize_view_to_table(
        self, view: BigQueryView, use_query_cache: bool
    ) -> bigquery.Table:
        if view.materialized_address is None:
            raise ValueError(
                "Trying to materialize a view that does not have a set "
                "materialized_address."
            )

        dst_dataset_id = view.materialized_address.dataset_id
        dst_table_id = view.materialized_address.table_id
        logging.info(
            "Materializing %s.%s into a table with address: %s.%s",
            view.dataset_id,
            view.view_id,
            dst_dataset_id,
            dst_table_id,
        )

        create_job = self.create_table_from_query_async(
            dataset_id=dst_dataset_id,
            table_id=dst_table_id,
            query=view.direct_select_query,
            overwrite=True,
            clustering_fields=view.clustering_fields,
            use_query_cache=use_query_cache,
        )
        create_job.result()

        description = view.materialized_table_bq_description
        table = self.get_table(self.dataset_ref_for_id(dst_dataset_id), dst_table_id)
        if description == table.description:
            return table

        table.description = description
        return self.client.update_table(table, ["description"])

    def create_table_with_schema(
        self,
        dataset_id: str,
        table_id: str,
        schema_fields: List[bigquery.SchemaField],
        clustering_fields: Optional[List[str]] = None,
        date_partition_field: Optional[Optional[str]] = None,
    ) -> bigquery.Table:
        self._validate_schema(
            BigQueryAddress(
                dataset_id=dataset_id,
                table_id=table_id,
            ),
            schema_fields,
        )

        dataset_ref = self.dataset_ref_for_id(dataset_id)

        if self.table_exists(dataset_ref, table_id):
            raise ValueError(
                f"Trying to create a table that already exists: {dataset_id}.{table_id}."
            )

        table_ref = bigquery.TableReference(dataset_ref, table_id)
        table = bigquery.Table(table_ref, schema_fields)

        if clustering_fields is not None:
            table.clustering_fields = clustering_fields

        if date_partition_field:
            field_type = one(
                f.field_type for f in schema_fields if f.name == date_partition_field
            )

            # BigQuery only allows partitioning on tables of these types. See:
            # https://cloud.google.com/bigquery/docs/partitioned-tables#date_timestamp_partitioned_tables
            if field_type not in (
                bigquery.enums.SqlTypeNames.DATE.value,
                bigquery.enums.SqlTypeNames.DATETIME.value,
                bigquery.enums.SqlTypeNames.TIMESTAMP.value,
            ):
                raise ValueError(
                    f"Date partition field [{date_partition_field}] has unsupported "
                    f"type: [{field_type}]."
                )

            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=date_partition_field,
            )

        logging.info("Creating table %s.%s", dataset_id, table_id)
        return self.client.create_table(table)

    def set_table_expiration(
        self, dataset_id: str, table_id: str, expiration: datetime.datetime
    ) -> None:
        dataset_ref = self.dataset_ref_for_id(dataset_id)
        table = self.get_table(dataset_ref=dataset_ref, table_id=table_id)
        table.expires = expiration
        self.client.update_table(table, fields=["expires"])

    @staticmethod
    def _get_excess_schema_fields(
        base_schema: List[bigquery.SchemaField],
        extended_schema: List[bigquery.SchemaField],
    ) -> List[bigquery.SchemaField]:
        """Returns any fields from extended_schema not named in base_schema."""
        table_schema_field_names = {field.name for field in base_schema}
        desired_schema_field_names = {field.name for field in extended_schema}
        missing_desired_field_names = (
            desired_schema_field_names - table_schema_field_names
        )
        return [
            field
            for field in extended_schema
            if field.name in missing_desired_field_names
        ]

    def _add_or_update_existing_schema_fields(
        self,
        table: bigquery.Table,
        desired_schema_fields: List[bigquery.SchemaField],
    ) -> None:
        """Updates the schema of the table to include the schema_fields if they are not
        already present in the Table's schema. Assumes that no field_type or mode values
        are not being changed for existing fields (BQ will disallow this). Does not
        delete existing schema fields.
        """
        existing_table_schema_sorted = sorted(table.schema, key=lambda c: c.name)
        updated_table_schema_sorted = sorted(
            desired_schema_fields, key=lambda c: c.name
        )

        if updated_table_schema_sorted == existing_table_schema_sorted:
            logging.info(
                "Schema for table %s.%s is already equal to the desired schema.",
                table.dataset_id,
                table.table_id,
            )
            return

        # Update the table schema with the missing fields
        logging.info(
            "Updating schema of table %s to: %s", table.table_id, desired_schema_fields
        )
        table.schema = desired_schema_fields
        self.client.update_table(table, ["schema"])

    def _remove_unused_fields_from_schema(
        self,
        dataset_id: str,
        table_id: str,
        existing_schema: List[bigquery.SchemaField],
        desired_schema_fields: List[bigquery.SchemaField],
        allow_field_deletions: bool,
    ) -> Optional[bigquery.QueryJob]:
        """Compares the schema of the given table to the desired schema fields and
        drops any unused columns.

        n.b.: Using ALTER TABLE statements does not immediately free up storage, see
        https://cloud.google.com/bigquery/docs/managing-table-schemas#delete_a_column
        for more info
        """
        deprecated_fields = self._get_excess_schema_fields(
            desired_schema_fields, existing_schema
        )

        if not deprecated_fields:
            return None

        logging.info(
            "Schema for table %s.%s has [%s] excess fields to drop.",
            dataset_id,
            table_id,
            len(deprecated_fields),
        )

        if not allow_field_deletions:
            raise ValueError(
                f"Found deprecated fields [{deprecated_fields}] for table:"
                f" {dataset_id}.{table_id} but field deletions is not allowed."
            )

        drop_statements = ", ".join(
            [f"DROP COLUMN {field.name}" for field in deprecated_fields]
        )

        drop_query = f"""
            ALTER TABLE `{dataset_id}.{table_id}` {drop_statements}
        """

        return self.run_query_async(
            query_str=drop_query,
            use_query_cache=False,
        )

    def _assert_is_valid_schema_field_update(
        self,
        table_address: BigQueryAddress,
        old_schema_field: bigquery.SchemaField,
        new_schema_field: bigquery.SchemaField,
    ) -> None:
        if old_schema_field.field_type != new_schema_field.field_type:
            raise ValueError(
                f"Trying to change the field type of an existing field in "
                f"{table_address.dataset_id}.{table_address.table_id}. Existing "
                f"field {old_schema_field.name} has type "
                f"{old_schema_field.field_type}. Cannot change this type to "
                f"{new_schema_field.field_type}."
            )

        if old_schema_field.mode != new_schema_field.mode:
            raise ValueError(
                f"Cannot change the mode of field {old_schema_field} to {new_schema_field.mode}."
            )

    def _validate_schema(
        self, address: BigQueryAddress, schema: List[bigquery.SchemaField]
    ) -> None:
        """Checks that the given schema is valid (no duplicate field names, no extra
        long descriptions).
        """
        seen_fields = set()
        for field in schema:
            if field.name in seen_fields:
                raise ValueError(
                    f"Found multiple columns with name [{field.name}] in new schema "
                    f"for table [{address.to_str()}]."
                )
            if (
                field.description
                and len(field.description) > BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH
            ):
                raise ValueError(
                    f"Attempting to set description for field [{field.name}] on table "
                    f"[{address.to_str()}] that is too long. Max allowed length "
                    f"is {BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH} characters."
                )
            seen_fields.add(field.name)

    def update_schema(
        self,
        dataset_id: str,
        table_id: str,
        desired_schema_fields: List[bigquery.SchemaField],
        allow_field_deletions: bool = True,
    ) -> None:
        self._validate_schema(
            BigQueryAddress(dataset_id=dataset_id, table_id=table_id),
            desired_schema_fields,
        )
        dataset_ref = self.dataset_ref_for_id(dataset_id)
        try:
            table = self.client.get_table(dataset_ref.table(table_id))
        except exceptions.NotFound as e:
            raise ValueError(
                f"Cannot update schema fields for a table that does not exist: "
                f"{dataset_id}.{table_id}"
            ) from e

        existing_schema = table.schema
        desired_schema_map = {field.name: field for field in desired_schema_fields}

        for field in existing_schema:
            if field.name in desired_schema_map:
                self._assert_is_valid_schema_field_update(
                    BigQueryAddress(dataset_id=dataset_id, table_id=table_id),
                    field,
                    desired_schema_map[field.name],
                )

        # Remove any deprecated fields first as it involves copying the entire view
        removal_job = self._remove_unused_fields_from_schema(
            dataset_id=dataset_id,
            table_id=table_id,
            existing_schema=existing_schema,
            desired_schema_fields=desired_schema_fields,
            allow_field_deletions=allow_field_deletions,
        )

        if removal_job:
            # Wait for the removal job to complete before running the job to add fields
            removal_job.result()

            # If we removed fields, we need to query for the table again to get the
            # updated schema.
            table = self.client.get_table(dataset_ref.table(table_id))

        self._add_or_update_existing_schema_fields(
            table=table,
            desired_schema_fields=desired_schema_fields,
        )

    def copy_table(
        self,
        source_dataset_id: str,
        source_table_id: str,
        destination_dataset_id: str,
        schema_only: bool = False,
        overwrite: bool = False,
    ) -> Optional[bigquery.job.CopyJob]:
        source_dataset_ref = self.dataset_ref_for_id(source_dataset_id)
        source_table = self.get_table(source_dataset_ref, source_table_id)

        # If we are copying the contents then we can only copy actual tables.
        if not schema_only and source_table.table_type != "TABLE":
            logging.warning(
                "Skipping copy of item with type [%s]: [%s]",
                source_table.table_type,
                source_table.table_id,
            )

            return None

        source_table_ref = bigquery.TableReference(
            self.dataset_ref_for_id(source_dataset_id), source_table.table_id
        )
        destination_table_ref = bigquery.TableReference(
            self.dataset_ref_for_id(destination_dataset_id), source_table.table_id
        )

        if schema_only:
            source_table = self.client.get_table(source_table_ref)
            dest_table = bigquery.Table(destination_table_ref, source_table.schema)
            # Some views require special properties (such as _FILE_NAME) from external data tables,
            # so we need to set an external data configuration on the referenced table for them to
            # compile. We set the source URI to an empty file so we aren't actually copying data.
            if source_table.external_data_configuration:
                external_config = source_table.external_data_configuration
                if (
                    source_table.external_data_configuration.source_format
                    == "NEWLINE_DELIMITED_JSON"
                ):
                    external_config.source_uris = [
                        f"gs://{self.project_id}-configs/empty.json"
                    ]
                if source_table.external_data_configuration.source_format == "CSV":
                    external_config.source_uris = [
                        f"gs://{self.project_id}-configs/empty.csv"
                    ]
                dest_table.external_data_configuration = external_config

            self.create_table(
                dest_table,
                overwrite=overwrite,
            )
        else:
            job_config = bigquery.CopyJobConfig()
            job_config.write_disposition = (
                bigquery.job.WriteDisposition.WRITE_TRUNCATE
                if overwrite
                else bigquery.job.WriteDisposition.WRITE_EMPTY
            )

            return self.client.copy_table(
                source_table_ref, destination_table_ref, job_config=job_config
            )

        return None

    def copy_dataset_tables(
        self,
        source_dataset_id: str,
        destination_dataset_id: str,
        schema_only: bool = False,
        overwrite_destination_tables: bool = False,
    ) -> None:
        # Get source tables
        source_table_ids = [
            table.table_id for table in self.list_tables(source_dataset_id)
        ]

        # Check existing destination tables
        initial_destination_table_ids = [
            table.table_id for table in self.list_tables(destination_dataset_id)
        ]

        if overwrite_destination_tables:
            for table_id in initial_destination_table_ids:
                if table_id not in source_table_ids:
                    self.delete_table(destination_dataset_id, table_id)
        else:
            if initial_destination_table_ids:
                raise ValueError(
                    f"Destination dataset [{destination_dataset_id}] for copy is not empty."
                )

        logging.info(
            "Copying tables in dataset [%s] to dataset [%s]",
            source_dataset_id,
            destination_dataset_id,
        )
        copy_jobs: List[bigquery.job.CopyJob] = []
        for table_id in source_table_ids:
            copy_job = self.copy_table(
                source_dataset_id=source_dataset_id,
                source_table_id=table_id,
                destination_dataset_id=destination_dataset_id,
                schema_only=schema_only,
                overwrite=overwrite_destination_tables,
            )

            if copy_job:
                copy_jobs.append(copy_job)

        if copy_jobs:
            self.wait_for_big_query_jobs(jobs=copy_jobs)

    def backup_dataset_tables_if_dataset_exists(
        self, dataset_id: str
    ) -> Optional[bigquery.DatasetReference]:
        backup_dataset_id = self.add_timestamp_suffix_to_dataset_id(
            f"{dataset_id}_backup"
        )

        if not self.dataset_exists(self.dataset_ref_for_id(dataset_id)):
            return None

        backup_dataset_ref = self.dataset_ref_for_id(backup_dataset_id)
        self.create_dataset_if_necessary(
            backup_dataset_ref,
            default_table_expiration_ms=DATASET_BACKUP_TABLE_EXPIRATION_MS,
        )
        self.copy_dataset_tables(
            source_dataset_id=dataset_id,
            destination_dataset_id=backup_dataset_id,
        )
        return backup_dataset_ref

    def wait_for_big_query_jobs(self, jobs: Sequence[PollingFuture]) -> List[Any]:
        logging.info("Waiting for [%s] query jobs to complete", len(jobs))
        results = []

        ssl_retry_policy = retry.Retry(predicate=ssl_error_retry_predicate)

        with futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
        ) as executor:
            job_futures = [
                executor.submit(job.result, retry=ssl_retry_policy) for job in jobs
            ]
            for f in futures.as_completed(job_futures):
                results.append(f.result())
        return results

    def copy_dataset_tables_across_regions(
        self,
        source_dataset_id: str,
        destination_dataset_id: str,
        overwrite_destination_tables: bool = False,
        timeout_sec: float = DEFAULT_CROSS_REGION_COPY_TIMEOUT_SEC,
    ) -> None:
        source_dataset_ref = self.dataset_ref_for_id(source_dataset_id)
        destination_dataset_ref = self.dataset_ref_for_id(destination_dataset_id)

        # Get source tables
        source_table_ids = {
            table.table_id
            for table in self.list_tables_excluding_views(source_dataset_id)
        }
        source_tables_by_id = {
            table_id: self.get_table(source_dataset_ref, table_id)
            for table_id in source_table_ids
        }

        # Check existing destination tables
        initial_destination_table_ids = {
            table.table_id
            for table in self.list_tables_excluding_views(destination_dataset_id)
        }
        if overwrite_destination_tables:
            for table_id in initial_destination_table_ids:
                if table_id not in source_table_ids:
                    self.delete_table(destination_dataset_id, table_id)
        else:
            if initial_destination_table_ids:
                raise ValueError(
                    f"Destination dataset [{destination_dataset_id}] for copy is not empty."
                )

        # Start the transfer
        transfer_client = DataTransferServiceClient()

        # Check credentialing
        logging.info(
            "Checking credentials for transfer client: %s, %s",
            self.project_id,
            CROSS_REGION_COPY_DATA_SOURCE_ID,
        )
        data_source_name = (
            f"projects/{self.project_id}/dataSources/{CROSS_REGION_COPY_DATA_SOURCE_ID}"
        )
        credentialing_response = transfer_client.check_valid_creds(
            request=CheckValidCredsRequest(name=data_source_name),
        )
        logging.info("Credentialing response: %s", credentialing_response)

        if not self.dataset_exists(self.dataset_ref_for_id(destination_dataset_id)):
            raise ValueError(
                f"Cannot copy data to dataset [{destination_dataset_id}] which does not exist"
            )

        display_name = StrictStringFormatter().format(
            CROSS_REGION_COPY_DISPLAY_NAME_TEMPLATE,
            source_dataset_id=source_dataset_id,
            destination_dataset_id=destination_dataset_id,
            ts=datetime.datetime.now(pytz.UTC).isoformat(),
        )
        transfer_config = TransferConfig(
            destination_dataset_id=destination_dataset_id,
            display_name=display_name,
            data_source_id=CROSS_REGION_COPY_DATA_SOURCE_ID,
            params={
                "source_project_id": self.project_id,
                "source_dataset_id": source_dataset_id,
                "overwrite_destination_table": overwrite_destination_tables,
            },
            schedule_options=ScheduleOptions(disable_auto_scheduling=True),
        )
        transfer_config = transfer_client.create_transfer_config(
            parent=f"projects/{self.project_id}",
            transfer_config=transfer_config,
        )
        logging.info("Created transfer config [%s]", transfer_config.name)

        # Check for success
        try:
            requested_run_time = timestamp_pb2.Timestamp()
            requested_run_time.FromDatetime(datetime.datetime.now(tz=pytz.UTC))
            response = transfer_client.start_manual_transfer_runs(
                request=StartManualTransferRunsRequest(
                    {
                        "parent": transfer_config.name,
                        "requested_run_time": requested_run_time,
                    }
                )
            )

            run = one(response.runs)
            logging.info(
                "Scheduled transfer run [%s] for transfer config [%s]",
                run.name,
                transfer_config.name,
            )

            timeout_time = datetime.datetime.now() + datetime.timedelta(
                seconds=timeout_sec
            )
            while True:
                logging.info("Checking status of transfer run [%s]", run.name)

                destination_table_ids = {
                    table.table_id
                    for table in self.list_tables_excluding_views(
                        destination_dataset_id
                    )
                }
                missing_tables = source_table_ids - destination_table_ids

                stale_tables = set()
                for destination_table_id in destination_table_ids:
                    destination_table = self.get_table(
                        destination_dataset_ref, destination_table_id
                    )
                    source_table = source_tables_by_id[destination_table.table_id]
                    # We compare against the time that the source table was last
                    # modified, not the time that the transfer began, because the
                    # transfer may not update the destination table at all if the source
                    # table has not changed since the last refresh:
                    # https://cloud.google.com/bigquery/docs/copying-datasets#table_limitations
                    if destination_table.modified <= source_table.modified:
                        stale_tables.add(destination_table.table_id)

                if not missing_tables and not stale_tables:
                    logging.info("Transfer run succeeded")

                    run = transfer_client.get_transfer_run(
                        request={"name": run.name},
                        timeout=DEFAULT_GET_TRANSFER_RUN_TIMEOUT_SEC,
                    )

                    if run.state != TransferState.SUCCEEDED:
                        logging.error(
                            "All expected tables found in destination "
                            "dataset [%s], but transfer run has state [%s].",
                            destination_dataset_id,
                            run.state,
                        )
                    break

                if timeout_time < datetime.datetime.now():
                    raise TimeoutError(
                        f"Did not complete dataset copy before timeout of "
                        f"[{timeout_sec}] seconds expired."
                    )

                logging.info(
                    "Transfer run in progress, missing [%s] tables and [%s] tables are "
                    "stale - sleeping for %s seconds",
                    missing_tables,
                    stale_tables,
                    CROSS_REGION_COPY_STATUS_ATTEMPT_SLEEP_TIME_SEC,
                )
                time.sleep(CROSS_REGION_COPY_STATUS_ATTEMPT_SLEEP_TIME_SEC)
        finally:
            logging.info("Deleting transfer config [%s]", transfer_config.name)
            transfer_client.delete_transfer_config(
                request={"name": transfer_config.name}
            )
            logging.info("Finished deleting transfer config [%s]", transfer_config.name)

    def update_datasets_to_match_reference_schema(
        self, reference_dataset_id: str, stale_schema_dataset_ids: List[str]
    ) -> None:
        reference_dataset_ref = self.dataset_ref_for_id(reference_dataset_id)
        reference_tables = self.list_tables(reference_dataset_id)
        reference_table_schemas = {
            t.table_id: self.get_table(reference_dataset_ref, t.table_id).schema
            for t in reference_tables
        }

        for stale_schema_dataset_id in stale_schema_dataset_ids:
            stale_dataset_ref = self.dataset_ref_for_id(stale_schema_dataset_id)
            stale_schema_tables = self.list_tables(stale_schema_dataset_id)
            stale_table_schemas = {
                t.table_id: self.get_table(stale_dataset_ref, t.table_id).schema
                for t in stale_schema_tables
            }

            for table_id, reference_schema in reference_table_schemas.items():
                if table_id not in stale_table_schemas:
                    self.create_table_with_schema(
                        stale_schema_dataset_id,
                        table_id,
                        reference_schema,
                    )

            for table_id, stale_table_schema in stale_table_schemas.items():
                if table_id not in reference_table_schemas:
                    self.delete_table(stale_schema_dataset_id, table_id)
                    continue

                if reference_table_schemas[table_id] == stale_table_schema:
                    continue

                self.update_schema(
                    stale_schema_dataset_id,
                    table_id,
                    desired_schema_fields=reference_table_schemas[table_id],
                )

    def add_timestamp_suffix_to_dataset_id(self, dataset_id: str) -> str:
        timestamp = (
            datetime.datetime.now()
            .isoformat()
            .replace("T", "_")
            .replace("-", "_")
            .replace(":", "_")
            .replace(".", "_")
        )

        return f"{dataset_id}_{timestamp}"
