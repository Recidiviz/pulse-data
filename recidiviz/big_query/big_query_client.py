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
import time
from collections import defaultdict
from concurrent import futures
from typing import (
    IO,
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

import __main__
import attr
import google
import pytz
import requests
from google.api_core import exceptions as google_api_exceptions
from google.api_core import retry
from google.api_core.client_options import ClientOptions
from google.api_core.future.polling import PollingFuture
from google.auth.credentials import AnonymousCredentials
from google.cloud import bigquery, exceptions
from google.cloud.bigquery import ExternalConfig
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
from recidiviz.big_query.big_query_job_labels import BigQueryDatasetIdJobLabel
from recidiviz.big_query.big_query_utils import (
    are_bq_schemas_same,
    get_file_destinations_for_bq_export,
)
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.constants import BQ_TABLE_COLUMN_DESCRIPTION_MAX_LENGTH
from recidiviz.big_query.export.export_query_config import ExportQueryConfig
from recidiviz.big_query.row_access_policy_query_builder import (
    RowAccessPolicy,
    RowAccessPolicyQueryBuilder,
    row_access_policy_lists_are_equivalent,
)
from recidiviz.cloud_resources.platform_resource_labels import (
    PlatformEnvironmentResourceLabel,
)
from recidiviz.cloud_resources.resource_label import (
    ResourceLabel,
    coalesce_resource_labels,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.encoding import BIG_QUERY_UTF_8
from recidiviz.common.google_cloud.utils import format_resource_label
from recidiviz.common.retry import default_bq_retry_with_additions
from recidiviz.common.retry_predicate import (
    RATE_LIMIT_INITIAL_DELAY,
    RATE_LIMIT_MAXIMUM_DELAY,
    RATE_LIMIT_TOTAL_TIMEOUT,
    bad_request_retry_predicate,
    ssl_error_retry_predicate,
)
from recidiviz.utils import environment, metadata
from recidiviz.utils.environment import in_test
from recidiviz.utils.size import total_size
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.utils.types import assert_type

_clients_by_project_id_by_region: Dict[str, Dict[str, bigquery.Client]] = defaultdict(
    dict
)

BQ_CLIENT_MAX_POOL_CONNECTIONS = 128
BQ_CLIENT_MAX_POOL_SIZE = 128

DEFAULT_VANTA_DATASET_OWNER = "joshua"

# This is copied from protected variable pandas_gbq.schema._TYPE_ALIASES
_TYPE_ALIASES = {
    "BOOL": "BOOLEAN",
    "FLOAT64": "FLOAT",
    "INT64": "INTEGER",
    "STRUCT": "RECORD",
}


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
            credentials, _default_project = google.auth.default(
                scopes=[
                    # This is the default scope for the bigquery.Client
                    "https://www.googleapis.com/auth/cloud-platform",
                    # Give access to Google Drive so we can read from tables backed by
                    # a Google Sheet.
                    "https://www.googleapis.com/auth/drive",
                ]
            )

            new_client = bigquery.Client(
                project=project_id, location=region, credentials=credentials
            )

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

# Stored in a constant to test
UPDATE_DESCRIPTION_RETRY = default_bq_retry_with_additions(
    google_api_exceptions.PreconditionFailed
)


@attr.define
class BigQueryViewMaterializationResult:
    """Class storing information about the results of a view materialization job."""

    # The address of the view that was materialized
    view_address: BigQueryAddress

    # The table results of view materialization
    materialized_table: bigquery.Table

    # The completed QueryJob for the view materialization.
    completed_materialization_job: bigquery.QueryJob

    @property
    def materialized_table_num_rows(self) -> int:
        return assert_type(self.materialized_table.num_rows, int)

    @property
    def materialized_table_size_bytes(self) -> int:
        return assert_type(self.materialized_table.num_bytes, int)

    @property
    def slot_millis(self) -> int:
        return assert_type(self.completed_materialization_job.slot_millis, int)

    @property
    def total_bytes_processed(self) -> int | None:
        return self.completed_materialization_job.total_bytes_processed

    @property
    def total_bytes_billed(self) -> int | None:
        return self.completed_materialization_job.total_bytes_billed

    @property
    def job_id(self) -> str:
        return assert_type(self.completed_materialization_job.job_id, str)


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
    def create_dataset_if_necessary(
        self,
        dataset_id: str,
        *,
        default_table_expiration_ms: Optional[int] = None,
    ) -> bigquery.Dataset:
        """Create a BigQuery dataset if it does not exist, with the optional dataset
        table expiration if provided.
        """

    @abc.abstractmethod
    def dataset_exists(self, dataset_id: str) -> bool:
        """Check whether or not a BigQuery Dataset exists.
        Args:
            dataset_id: The BigQuery dataset to look for

        Returns:
            True if the dataset exists, False otherwise.
        """

    @abc.abstractmethod
    def dataset_is_empty(self, dataset_id: str) -> bool:
        """Check whether or not a BigQuery Dataset is empty (and could be deleted).
        Args:
            dataset_id: The BigQuery dataset to look for

        Returns:
            True if the dataset is empty, False otherwise.
        """

    @abc.abstractmethod
    def delete_dataset(
        self,
        dataset_id: str,
        *,
        delete_contents: bool = False,
        not_found_ok: bool = False,
    ) -> None:
        """Deletes a BigQuery dataset
        Args:
            dataset_id: The BigQuery dataset to delete
            delete_contents: Whether to delete all tables within the dataset. If set to
                False and the dataset has tables, this method fails.
            not_found_ok: If False, this raises an exception when the dataset_ref is
                not found.
        """

    @abc.abstractmethod
    def get_dataset(self, dataset_id: str) -> bigquery.Dataset:
        """Fetches a BigQuery dataset.
        Args:
            dataset_id: The BigQuery dataset to look for

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
    def table_exists(self, address: BigQueryAddress) -> bool:
        """Check whether a BigQuery table or view exists at the given address.

        Args:
            address: The BigQuery address to look for

        Returns:
            True if the table or view exists, False otherwise.
        """

    @abc.abstractmethod
    def get_table(self, address: BigQueryAddress) -> bigquery.Table:
        """Fetches the bigquery.Table for a BigQuery table or view at the given address.
        Throws if there is no table or view at the given address.

        Args:
            address: The BigQuery address of the table to return

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
    def get_row_counts_for_tables(
        self,
        dataset_id: str,
        *,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> Dict[str, int]:
        """Returns the row counts for each table in a dataset.

        If a table has no rows, it will be omitted from the output. If the dataset does
        not exist, the output will be empty."""

    @abc.abstractmethod
    def create_table(
        self, table: bigquery.Table, *, exists_ok: bool = False
    ) -> bigquery.Table:
        """Creates a new table in big query. If |exists_ok| is False and the table
        already exists, raises an AlreadyExists error. Once the table is created, applies row-level permissions
        to the table if applicable.

        Args:
            table: The Table to create.
            exists_ok: Whether we can overwrite a table if one already exists where the
                table is to be created. If a table exists and exists_ok=False, we will
                throw.

        Returns:
            The Table that was just created.
        """

    @abc.abstractmethod
    def create_or_update_view(
        self, view: BigQueryView, *, might_exist: bool = True
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
    def list_row_level_permissions(
        self, table: bigquery.Table
    ) -> List[RowAccessPolicy]:
        """Lists all row access policies for the table."""

    @abc.abstractmethod
    def apply_row_level_permissions(self, table: bigquery.Table) -> None:
        """Applies the relevant row access policies to the table."""

    @abc.abstractmethod
    def load_table_from_cloud_storage(
        self,
        *,
        source_uris: List[str],
        destination_address: BigQueryAddress,
        destination_table_schema: List[bigquery.SchemaField],
        write_disposition: str,
        skip_leading_rows: int = 0,
        preserve_ascii_control_characters: bool = False,
        encoding: str = BIG_QUERY_UTF_8,
        field_delimiter: str = ",",
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.job.LoadJob:
        """Loads a table from CSV data in GCS to BigQuery.

        Given a desired table name, source data URI(s) and destination schema, loads the
        table into BigQuery.

        The table is created if it does not exist, and overwritten with the contents of
        the new data if they do exist and the write_disposition is WRITE_TRUNCATE. If
        the write_disposition is WRITE_APPEND, data will be added to the table if it
        already exists. Row-level permissions are applied to the table if applicable.

        Args:
            source_uris: The paths in Google Cloud Storage to read contents from (starts with 'gs://').
            destination_address: The BigQuery address to load the table into. Gets
                created if it does not already exist.
            destination_table_schema: Defines a list of field schema information for each expected column in the input
                file.
            write_disposition: Indicates whether BigQuery should overwrite the table
                completely (WRITE_TRUNCATE) or adds to the table with new rows
                (WRITE_APPEND). By default, WRITE_APPEND is used.
            skip_leading_rows: Optional number of leading rows to skip on each input
                file. Defaults to zero
            preserve_ascii_control_characters: Whether to preserve ASCII control characters in the data. Defaults to
                False. When disabled, we will fail the load if we encounter ASCII control characters in the data.
            job_labels: Metadata labels to attach to the BigQuery LoadJob, recorded in the JOBS view.
        Returns:
            The completed LoadJob object containing job details.
        """

    @abc.abstractmethod
    def load_into_table_from_file(
        self,
        *,
        source: IO,
        destination_address: BigQueryAddress,
        schema: List[bigquery.SchemaField],
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.job.LoadJob:
        """Loads a table from a file to BigQuery.

        The destination table will be created with the provided schema if it does not
        already exist. The destination dataset will also be created if it does not
        already exist.

        If the provided schema has additional columns not present in the destination
        table, those columns will be added. However, additional columns will not be
        removed from the destination table if they are not present in the provided
        schema.

        Tables are created if they do not exist. Applies row-level permissions to the
        table if applicable.

        Args:
            source: A file handle opened in binary mode for reading.
            destination_address: The BigQuery address to load the data into. Gets
                created if it does not already exist.
            schema: The desired BigQuery schema of the destination table.
            job_labels: Metadata labels to attach to the BigQuery LoadJob, recorded in the JOBS view.

        Returns:
            The completed LoadJob object containing job details.
        """

    @abc.abstractmethod
    def export_table_to_cloud_storage_async(
        self,
        *,
        source_table_address: BigQueryAddress,
        destination_uri: str,
        destination_format: str,
        print_header: bool,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> Optional[bigquery.ExtractJob]:
        """Exports the table corresponding to the given address to the path in Google
        Cloud Storage denoted by |destination_uri|.

        Extracts the entire table and exports in the specified format to the given
        bucket in Cloud Storage.

        It is the caller's responsibility to wait for the resulting job to complete.

        Args:
            source_table_address: The BigQuery address of the table to export.
            destination_uri: The path in Google Cloud Storage to write the contents of
                the table to (starts with 'gs://').
            destination_format: The format the contents of the table should be outputted
                as (e.g. CSV or NEWLINE_DELIMITED_JSON).
            print_header: Indicates whether to print out a header row in the results.
            job_labels: Metadata labels to attach to the BigQuery ExtractJob, recorded in the JOBS view.

        Returns:
            The ExtractJob object containing job details, or None if the job fails to
            start.
        """

    @abc.abstractmethod
    def export_query_results_to_cloud_storage(
        self,
        *,
        export_configs: List[ExportQueryConfig],
        print_header: bool,
        use_query_cache: bool,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> list[tuple[ExportQueryConfig, list[GcsfsFilePath]]]:
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
            job_labels: Metadata labels to attach to the BigQuery QueryJob, recorded in the JOBS view.
        """

    @abc.abstractmethod
    def run_query_async(
        self,
        *,
        query_str: str,
        use_query_cache: bool,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        http_timeout: Optional[float] = None,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.QueryJob:
        """Runs a query in BigQuery asynchronously.

        It is the caller's responsibility to wait for the resulting job to complete.

        Note: treating the resulting job like an iterator waits implicitly. For example:

            query_job = client.run_query_async(query_str)
            for row in query_job:
                ...

        Args:
            query_str: The query to execute
            use_query_cache: Whether to look for the result in the query cache. See:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery.FIELDS.use_query_cache
            query_parameters: Parameters for the query
            http_timeout: The number of seconds to wait for client's underlying HTTP
                transport before using client.query's ``retry``.
            job_labels: Metadata labels to attach to the BigQuery QueryJob, recorded in the JOBS view.

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def paged_read_and_process(
        self,
        *,
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
    def create_table_from_query_async(
        self,
        *,
        address: BigQueryAddress,
        query: str,
        use_query_cache: bool,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        overwrite: Optional[bool] = False,
        clustering_fields: Optional[List[str]] = None,
        time_partitioning: bigquery.TimePartitioning | None = None,
        job_labels: Optional[list[ResourceLabel]] = None,
        output_schema: Optional[list[bigquery.SchemaField]] = None,
    ) -> bigquery.QueryJob:
        """Creates a table at the given address with the output from the given query.
        If overwrite is False, a 'duplicate' error is returned in the job result if the
        table already exists and contains data. If overwrite is True, overwrites the
        table if it already exists.

        Args:
            address: The address where the table should be created.
            query: The query to run. The result will be loaded into the new table.
            query_parameters: Optional parameters for the query
            overwrite: Whether or not to overwrite an existing table.
            clustering_fields: Columns by which to cluster the table.
            time_partitioning: Configuration for time period partitioning that should be
                applied to the table.
            use_query_cache: Whether to look for the result in the query cache. See:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery.FIELDS.use_query_cache
            job_labels: Metadata labels to attach to the BigQuery QueryJob, recorded in the JOBS view.
            output_schema: The schema for the materialized table if there are any overrides you would like to make.

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def create_table_from_query(
        self,
        *,
        address: BigQueryAddress,
        query: str,
        use_query_cache: bool,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        overwrite: Optional[bool] = False,
        clustering_fields: Optional[List[str]] = None,
        time_partitioning: bigquery.TimePartitioning | None = None,
        job_labels: Optional[list[ResourceLabel]] = None,
        output_schema: Optional[list[bigquery.SchemaField]] = None,
    ) -> bigquery.table.RowIterator:
        """Creates a table at the given address with the output from the given query.
        If overwrite is False, a 'duplicate' error is returned in the job result if the
        table already exists and contains data. If overwrite is True, overwrites the
        table if it already exists. Once the table is created, applies row-level permissions
        to the table if applicable.

        Args:
            address: The address where the table should be created.
            query: The query to run. The result will be loaded into the new table.
            query_parameters: Optional parameters for the query
            overwrite: Whether or not to overwrite an existing table.
            clustering_fields: Columns by which to cluster the table.
            time_partitioning: Configuration for time period partitioning that should be
                applied to the table.
            use_query_cache: Whether to look for the result in the query cache. See:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery.FIELDS.use_query_cache
            job_labels: Metadata labels to attach to the BigQuery QueryJob, recorded in the JOBS view.
            output_schema: The schema for the materialized table if there are any overrides you would like to make.

        Returns:
            The result of the QueryJob."""

    @abc.abstractmethod
    def insert_into_table_from_table_async(
        self,
        *,
        source_address: BigQueryAddress,
        destination_address: BigQueryAddress,
        use_query_cache: bool,
        source_data_filter_clause: Optional[str] = None,
        source_to_destination_column_mapping: Optional[Dict[str, str]] = None,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.QueryJob:
        """Appends data from a source table into a destination table using an INSERT
        INTO statement for columns that exist in both tables, with an optional
        source_data_filter_clause applied. Columns missing from the source table will
        be hydrated with the destination table's default value if set, otherwise with
        NULL.

        Note: Types for shared columns of the two tables must match. If a REQUIRED field
        in the destination table is missing from the source table, the query will likely
        fail unless a non-null default is set. For more info, see big query docs:
        https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement

        Args:
            source_address: The address of the source table from which to query.
            destination_address: The address of the table to insert into.
            source_data_filter_clause: An optional clause to filter the contents of the
                source table that are inserted into the destination table. Must start
                with "WHERE".
            use_query_cache: Whether to look for the result in the query cache. See:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery.FIELDS.use_query_cache
            job_labels: Metadata labels to attach to the BigQuery QueryJob, recorded in the JOBS view.

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def insert_into_table_from_query_async(
        self,
        *,
        destination_address: BigQueryAddress,
        query: str,
        use_query_cache: bool,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        allow_field_additions: bool = False,
        clustering_fields: Optional[List[str]] = None,
        time_partitioning: bigquery.TimePartitioning | None = None,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.QueryJob:
        """Inserts the results of the given query into the table at the given address.
        Creates a table if one does not yet exist. If |allow_field_additions| is set to
        False and the table exists, the schema of the query result must match the schema
        of the destination table. Note that if a table is created as a result of this query,
        the caller is responsible for ensuring that the correct row-level permissions are
        applied to the table.

        Args:
            destination_address: The address of the table where the result should be
                inserted.
            query: The query to run. The result will be loaded into the table.
            query_parameters: Optional parameters for the query.
            allow_field_additions: Whether or not to allow new columns to be created in
                the destination table if the schema in the query result does not exactly
                match the destination table. Defaults to False.
            clustering_fields: Columns by which to cluster the table.
            time_partitioning: Configuration for time period partitioning that should be
                applied to the table.
            use_query_cache: Whether to look for the result in the query cache. See:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery.FIELDS.use_query_cache
            job_labels: Metadata labels to attach to the BigQuery QueryJob, recorded in the JOBS view.

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def insert_into_table_from_query(
        self,
        *,
        destination_address: BigQueryAddress,
        query: str,
        use_query_cache: bool,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        allow_field_additions: bool = False,
        clustering_fields: Optional[List[str]] = None,
        time_partitioning: bigquery.TimePartitioning | None = None,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.table.RowIterator:
        """Inserts the results of the given query into the table at the given address.
        Creates a table if one does not yet exist. If |allow_field_additions| is set to
        False and the table exists, the schema of the query result must match the schema of
        the destination table. If a table is created, applies row-level permissions if applicable.

        Args:
            destination_address: The address of the table where the result should be
                inserted.
            query: The query to run. The result will be loaded into the table.
            query_parameters: Optional parameters for the query.
            allow_field_additions: Whether or not to allow new columns to be created in
                the destination table if the schema in the query result does not exactly
                match the destination table. Defaults to False.
            clustering_fields: Columns by which to cluster the table.
            time_partitioning: Configuration for time period partitioning that should be
                applied to the table.
            use_query_cache: Whether to look for the result in the query cache. See:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery.FIELDS.use_query_cache
            job_labels: Metadata labels to attach to the BigQuery QueryJob, recorded in the JOBS view.

        Returns:
            The result of the QueryJob.
        """

    @abc.abstractmethod
    def stream_into_table(
        self, address: BigQueryAddress, rows: Sequence[Dict[str, Any]]
    ) -> None:
        """Inserts the provided rows into the specified table.

        The table must already exist, and the rows must conform to the existing schema.

        Args:
            address: The address of the table into which the rows should be inserted.
            rows: A sequence of dictionaries representing the rows to insert into the
                table.
        """

    @abc.abstractmethod
    def load_into_table_async(
        self,
        *,
        address: BigQueryAddress,
        rows: Sequence[Dict[str, Any]],
        write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.job.LoadJob:
        """Inserts the provided rows into the specified table.

        The table must already exist, and the rows must conform to the existing schema.

        Args:
            address: The address of the table into which the rows should be inserted.
            rows: A sequence of dictionaries representing the rows to insert into the
                table.
            write_disposition: What to do if the destination table already exists.
                Defaults to WRITE_APPEND, which will append rows to an existing table.
        Returns:
            The LoadJob object containing job details.
        """

    @abc.abstractmethod
    def delete_from_table_async(
        self,
        address: BigQueryAddress,
        *,
        filter_clause: Optional[str] = None,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.QueryJob:
        """Deletes rows from the table at the given address that match the filter
        clause. If no filter is provided, all rows are deleted.

        Args:
            address: The address of the table to delete from.
            filter_clause: An optional clause that filters the contents of the table to
                determine which rows should be deleted. If present, it must start with
                "WHERE".
            job_labels: Metadata labels to attach to the BigQuery QueryJob, recorded in the JOBS view.

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def materialize_view_to_table(
        self,
        view: BigQueryView,
        use_query_cache: bool,
        view_configuration_changed: bool,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> BigQueryViewMaterializationResult:
        """Materializes the result of a view's view_query into a table and applies row-level permissions
        to the resulting table. The view's materialized_address must be set. The resulting table is put
        in the same project as the view, and it overwrites any previous materialization of the view.

        Returns a BigQueryViewMaterializationResult with information about the completed
        materialization job.

        Args:
            view: The BigQueryView to materialize into a table.
            use_query_cache: Whether to look for the result in the query cache. See:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery.FIELDS.use_query_cache
            view_configuration_changed: Whether the view configuration changed since the last materialization.
                If True, the materialized table will be fully deleted and recreated (wiping things like row-level permissions).
                If False, the data will just be dropped and overwritten.
            job_labels: Metadata labels to attach to the BigQuery QueryJob, recorded in the JOBS view.
        """

    @abc.abstractmethod
    def create_table_with_schema(
        self,
        *,
        address: BigQueryAddress,
        schema_fields: List[bigquery.SchemaField],
        clustering_fields: Optional[List[str]] = None,
        time_partitioning: bigquery.TimePartitioning | None = None,
        require_partition_filter: bool | None = None,
    ) -> bigquery.Table:
        """Creates a table in the given dataset with the given schema fields. Raises an
        error if a table with the same table_id already exists in the dataset. Once the table is created, applies row-level permissions
        to the table if applicable.

        Args:
            address: The address of the table to be created
            schema_fields: A list of fields defining the table's schema
            clustering_fields: A list of fields to cluster the table by. The clustering
                columns that are specified are used to co-locate related data. For more:
                https://cloud.google.com/bigquery/docs/clustered-tables.
            time_partitioning: The TimePartitioning configuration for this table,
                see https://cloud.google.com/bigquery/docs/partitioned-tables#partitioning_versus_clustering
                for details on the tradeoffs of clustering vs partitioning. You can also combine partitioning with
                clustering. Data is first partitioned and then data in each partition is
                clustered by the clustering columns.
            require_partition_filter: Whether or not a partition filter is required for
                this table, if partitioning is active.

        Returns:
            The bigquery.Table that is created.
        """

    @abc.abstractmethod
    def create_external_table(
        self,
        *,
        address: BigQueryAddress,
        external_data_config: ExternalConfig,
        allow_auto_detect_schema: bool,
    ) -> bigquery.Table:
        """Creates an external table with the given external configuration. For more
        info about external tables, see:
        https://cloud.google.com/bigquery/docs/external-tables.

        Args:
            address: The address of the table to be created.
            external_data_config: The configuration for the new external table.
            allow_auto_detect_schema: If False, this function throws if
                external_data_config.autodetect is True or external_data_config.schema
                is empty.
        """

    @abc.abstractmethod
    def update_external_table(
        self,
        *,
        address: BigQueryAddress,
        external_data_config: ExternalConfig,
        allow_auto_detect_schema: bool,
    ) -> bigquery.Table:
        """Updates the configuration of an external table. For more info about external
        tables, see: https://cloud.google.com/bigquery/docs/external-tables.

        Args:
            address: The address of the table to be updated.
            external_data_config: The configuration to update to.
            allow_auto_detect_schema: If False, this function throws if
                external_data_config.autodetect is True or external_data_config.schema
                is empty.
        """

    @abc.abstractmethod
    def update_schema(
        self,
        *,
        address: BigQueryAddress,
        desired_schema_fields: List[bigquery.SchemaField],
        allow_field_deletions: bool,
    ) -> None:
        """Updates the schema of the table at the given address to match the
        desired_schema_fields. This may result in both adding and dropping fields from
        the table's schema. Raises an exception if fields in desired_schema_fields
        conflict with existing fields' modes or types.

        Args:
            address: The address of the table to modify.
            desired_schema_fields: A list of fields describing the desired table schema.
            allow_field_deletions: Whether we should delete excess fields or not
        """

    @abc.abstractmethod
    def delete_table(
        self, address: BigQueryAddress, *, not_found_ok: bool = False
    ) -> None:
        """Provided the |dataset_id| and |table_id|, attempts to delete the given table
        from BigQuery.

        Args:
            address: The address of the table to delete.
            not_found_ok: If False, this raises an exception when the table is not found.
        """

    @abc.abstractmethod
    def set_table_expiration(
        self, address: BigQueryAddress, expiration: datetime.datetime
    ) -> None:
        """Set the table expiration to `expiration` for the table at the given address.

        Args:
            address: The address of the table to update.
            expiration: The datetime when the table should expire.
        """

    @abc.abstractmethod
    def copy_dataset_tables_across_regions(
        self,
        *,
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
        *,
        source_dataset_id: str,
        destination_dataset_id: str,
        schema_only: bool = False,
        overwrite_destination_tables: bool = False,
        job_labels: Optional[list[ResourceLabel]] = None,
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
        *,
        source_table_address: BigQueryAddress,
        destination_dataset_id: str,
        schema_only: bool = False,
        overwrite: bool = False,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> Optional[bigquery.job.CopyJob]:
        """Copies the table at the given address to the destination dataset,
        which must be empty if it exists. If the destination dataset does not
        exist, we will create one.

        If `schema_only` is provided, creates a matching table with the same schema
        in the destination dataset but does not copy contents of the table.
        """

    @abc.abstractmethod
    def backup_dataset_tables_if_dataset_exists(
        self,
        dataset_id: str,
        *,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> Optional[str]:
        """Creates a backup of all tables (but NOT views) in |dataset_id| in a new
        dataset with the format `my_dataset_backup_yyyy_mm_dd`. For example, the dataset
        `state` might backup to `state_backup_2021_05_06`.

        Returns the dataset_id the dataset data was backed up to, or None if the
        source dataset does not exist.
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
        self,
        project_id: Optional[str] = None,
        region_override: Optional[str] = None,
        default_job_labels: Optional[list[ResourceLabel]] = None,
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
        self._default_job_labels = self._build_default_labels(default_job_labels)

    @property
    def project_id(self) -> str:
        return self._project_id

    @staticmethod
    def _build_default_labels(
        default_job_labels: Optional[list[ResourceLabel]] = None,
    ) -> list[ResourceLabel]:
        labels = default_job_labels or []
        env_label = PlatformEnvironmentResourceLabel.for_current_env()
        labels.append(env_label)
        return labels

    def _build_labels(
        self,
        job_labels: list[ResourceLabel] | None,
        *,
        address: BigQueryAddress | None = None,
    ) -> dict[str, str]:
        """Builds a dictionary of BigQuery job labels by combining the provided |job_labels|
        with labels that can be derived from |address| and the client's default job labels.
        """
        if not job_labels:
            job_labels = []

        if address:
            job_labels = [*job_labels, *address.bq_job_labels]

        if not job_labels:
            # TODO(#35122) enforce that all jobs either have job or default labels
            return coalesce_resource_labels(
                *self._default_job_labels, should_throw_on_conflict=True
            )

        return coalesce_resource_labels(
            *job_labels, *self._default_job_labels, should_throw_on_conflict=True
        )

    def dataset_ref_for_id(self, dataset_id: str) -> bigquery.DatasetReference:
        return bigquery.DatasetReference.from_string(
            dataset_id, default_project=self._project_id
        )

    def _table_ref_for_address(
        self, address: BigQueryAddress
    ) -> bigquery.TableReference:
        return address.to_project_specific_address(self.project_id).table_reference

    def create_dataset_if_necessary(
        self,
        dataset_id: str,
        *,
        default_table_expiration_ms: Optional[int] = None,
    ) -> bigquery.Dataset:
        dataset_ref = self.dataset_ref_for_id(dataset_id)
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
                owner = format_resource_label(owner)
            else:
                owner = DEFAULT_VANTA_DATASET_OWNER
            description_label = format_resource_label(description)
            dataset_to_update.labels = {
                "vanta-owner": owner,
                "vanta-description": description_label,
            }
            return self.client.update_dataset(
                dataset_to_update, ["description", "labels"]
            )

        return dataset

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

    def dataset_exists(self, dataset_id: str) -> bool:
        dataset_ref = self.dataset_ref_for_id(dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
            return True
        except exceptions.NotFound:
            return False

    def dataset_is_empty(self, dataset_id: str) -> bool:
        dataset_ref = self.dataset_ref_for_id(dataset_id)
        tables = peekable(self.client.list_tables(dataset_ref.dataset_id))
        routines = peekable(self.client.list_routines(dataset_ref.dataset_id))

        return not tables and not routines

    def delete_dataset(
        self,
        dataset_id: str,
        *,
        delete_contents: bool = False,
        not_found_ok: bool = False,
    ) -> None:
        return self.client.delete_dataset(
            dataset_id, delete_contents=delete_contents, not_found_ok=not_found_ok
        )

    def get_dataset(self, dataset_id: str) -> bigquery.Dataset:
        return self.client.get_dataset(self.dataset_ref_for_id(dataset_id))

    def list_datasets(self) -> Iterator[bigquery.dataset.DatasetListItem]:
        return self.client.list_datasets()

    def table_exists(self, address: BigQueryAddress) -> bool:
        table_ref = self._table_ref_for_address(address)

        try:
            self.client.get_table(table_ref)
            return True
        except exceptions.NotFound:
            return False

    def drop_row_level_permissions(self, table: bigquery.Table) -> None:
        query = RowAccessPolicyQueryBuilder.build_query_to_drop_row_access_policy(table)
        job = self.run_query_async(query_str=query, use_query_cache=False)
        job.result()

    def list_row_level_permissions(
        self, table: bigquery.Table
    ) -> List[RowAccessPolicy]:
        # There isn't a client method to list row access policies, so we have to use the REST API endpoint
        url = f"https://bigquery.googleapis.com/bigquery/v2/projects/{self.project_id}/datasets/{table.dataset_id}/tables/{table.table_id}/rowAccessPolicies"

        # pylint: disable=protected-access
        headers = {"Authorization": f"Bearer {self.client._credentials.token}"}
        response = requests.get(url, headers=headers, timeout=15)

        if response.status_code == 200:
            policies = response.json().get("rowAccessPolicies", [])
        else:
            raise ValueError(
                f"Error retrieving row level permissions: {response.status_code} - {response.text}"
            )

        return [RowAccessPolicy.from_api_response(policy) for policy in policies]

    def apply_row_level_permissions(self, table: bigquery.Table) -> None:
        # Row level permissions cannot be applied to views or tables with external data config
        # (eg google sheet backed tables)
        if (
            table.view_query is not None
            or table.external_data_configuration is not None
        ):
            return

        if not (
            access_policies := RowAccessPolicyQueryBuilder.build_row_access_policies(
                table
            )
        ):
            return

        try:
            existing_policies = self.list_row_level_permissions(table)
            if existing_policies and row_access_policy_lists_are_equivalent(
                existing_policies, access_policies
            ):
                logging.info(
                    "No changes to row-level permissions for table [%s.%s]",
                    table.dataset_id,
                    table.table_id,
                )
                return
            if not existing_policies:
                logging.info(
                    "No existing row-level permissions for table [%s.%s]",
                    table.dataset_id,
                    table.table_id,
                )
        except Exception as e:
            logging.warning(
                "Failed to list row level permissions for table [%s.%s]: %s",
                table.dataset_id,
                table.table_id,
                str(e),
            )

        logging.info(
            "Applying row-level permissions to table [%s.%s]",
            table.dataset_id,
            table.table_id,
        )
        policy_queries: List[str] = [
            RowAccessPolicyQueryBuilder.build_query_to_drop_row_access_policy(table),
            *[query.to_create_query() for query in access_policies],
        ]

        job = self.run_query_async(
            query_str="\n".join(policy_queries), use_query_cache=False
        )
        # Applying row level permissions is very flaky and has been periodically failing with
        # various 400 errors.
        retry_policy = retry.Retry(
            initial=RATE_LIMIT_INITIAL_DELAY,
            maximum=RATE_LIMIT_MAXIMUM_DELAY,
            timeout=RATE_LIMIT_TOTAL_TIMEOUT,
            predicate=bad_request_retry_predicate,
        )
        job.result(retry=retry_policy)
        logging.info(
            "Applied row-level permissions for table [%s.%s]",
            table.dataset_id,
            table.table_id,
        )

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

    def get_row_counts_for_tables(
        self,
        dataset_id: str,
        *,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> Dict[str, int]:
        if not self.dataset_exists(dataset_id) or self.dataset_is_empty(dataset_id):
            return {}

        if not job_labels:
            job_labels = []

        job_labels = [*job_labels, BigQueryDatasetIdJobLabel(value=dataset_id.lower())]

        query_jobs = {}
        for table in self.list_tables(dataset_id):
            query_str = f"""
                SELECT COUNT(*) as num_rows
                FROM `{self.project_id}.{dataset_id}.{table.table_id}`
            """
            query_jobs[table.table_id] = self.run_query_async(
                query_str=query_str,
                use_query_cache=False,
                job_labels=job_labels,
            )

        return {table_id: one(job)["num_rows"] for table_id, job in query_jobs.items()}

    def get_table(self, address: BigQueryAddress) -> bigquery.Table:
        table_ref = self._table_ref_for_address(address)
        return self.client.get_table(table_ref)

    def create_table(
        self, table: bigquery.Table, *, exists_ok: bool = False
    ) -> bigquery.Table:
        created_table = self.client.create_table(table, exists_ok=exists_ok)

        try:
            self.apply_row_level_permissions(created_table)
        except Exception:
            logging.error(
                "Failed to apply row-level permissions to table [%s]. "
                "Table was created successfully, but row-level permissions were not applied.",
                created_table.table_id,
            )

        return created_table

    def create_or_update_view(
        self, view: BigQueryView, *, might_exist: bool = True
    ) -> bigquery.Table:
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

    def load_table_from_cloud_storage(
        self,
        *,
        source_uris: List[str],
        destination_address: BigQueryAddress,
        destination_table_schema: List[bigquery.SchemaField],
        write_disposition: str,
        skip_leading_rows: int = 0,
        preserve_ascii_control_characters: bool = False,
        encoding: str = BIG_QUERY_UTF_8,
        field_delimiter: str = ",",
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.job.LoadJob:
        """Triggers a load job, i.e. a job that will copy all of the data from the given
        Cloud Storage source into the given BigQuery destination. Returns once the job
        completes and row-level permissions were applied to the created table, if applicable.
        """
        self._validate_schema(destination_address, destination_table_schema)
        self.create_dataset_if_necessary(destination_address.dataset_id)

        destination_table_ref = self._table_ref_for_address(destination_address)

        job_config = bigquery.LoadJobConfig(
            labels=self._build_labels(job_labels, address=destination_address)
        )
        job_config.schema = destination_table_schema
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.allow_quoted_newlines = True
        job_config.write_disposition = write_disposition
        job_config.skip_leading_rows = skip_leading_rows
        job_config.preserve_ascii_control_characters = preserve_ascii_control_characters
        job_config.encoding = encoding
        job_config.field_delimiter = field_delimiter

        load_job = self.client.load_table_from_uri(
            source_uris, destination_table_ref, job_config=job_config
        )

        logging.info(
            "Started load job [%s] for table [%s]",
            load_job.job_id,
            str(destination_table_ref),
        )

        try:
            load_job.result()
        except Exception as e:
            logging.error(
                "Load job [%s] for table [%s] failed with errors: %s",
                load_job.job_id,
                destination_address.to_str(),
                load_job.errors,
            )
            raise e

        try:
            self.apply_row_level_permissions(self.get_table(destination_address))
        except Exception:
            logging.error(
                "Failed to apply row-level permissions to table [%s]. "
                "Load job was successful, but row-level permissions were not applied.",
                destination_address.to_str(),
            )

        return load_job

    def load_into_table_from_file(
        self,
        *,
        source: IO,
        destination_address: BigQueryAddress,
        schema: List[bigquery.SchemaField],
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.job.LoadJob:
        self._validate_schema(destination_address, schema)
        self.create_dataset_if_necessary(destination_address.dataset_id)

        destination_table_ref = self._table_ref_for_address(destination_address)

        job_config = bigquery.LoadJobConfig(
            labels=self._build_labels(job_labels, address=destination_address)
        )
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
            "Started load job [%s] for table [%s]",
            load_job.job_id,
            str(destination_table_ref),
        )

        try:
            load_job.result()
        except Exception as e:
            logging.error(
                "Load job [%s] for table [%s] failed with errors: %s",
                load_job.job_id,
                destination_address.to_str(),
                load_job.errors,
            )
            raise e

        try:
            self.apply_row_level_permissions(self.get_table(destination_address))
        except Exception:
            logging.error(
                "Failed to apply row-level permissions to table [%s]. "
                "Load job was successful, but row-level permissions were not applied.",
                destination_address.to_str(),
            )

        return load_job

    def export_table_to_cloud_storage_async(
        self,
        *,
        source_table_address: BigQueryAddress,
        destination_uri: str,
        destination_format: str,
        print_header: bool,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> Optional[bigquery.ExtractJob]:
        if not print_header and destination_format != bigquery.DestinationFormat.CSV:
            raise ValueError(
                f"Export called incorrectly with print_header=False and destination_format={destination_format}"
            )

        if not self.table_exists(source_table_address):
            logging.error("Table [%s] does not exist", source_table_address.to_str())
            return None

        source_table_ref = self._table_ref_for_address(source_table_address)

        job_config = bigquery.ExtractJobConfig(
            labels=self._build_labels(job_labels, address=source_table_address)
        )
        job_config.destination_format = destination_format
        job_config.print_header = print_header

        return self.client.extract_table(
            source_table_ref,
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
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> list[tuple[ExportQueryConfig, list[GcsfsFilePath]]]:
        exported_configs_and_paths: list[
            tuple[ExportQueryConfig, list[GcsfsFilePath]]
        ] = []

        try:
            query_jobs = []
            for export_config in export_configs:
                # Because we delete the intermediate table after the export, we don't worry about
                # applying row-level permissions.
                query_job = self.create_table_from_query_async(
                    address=export_config.intermediate_table_address,
                    query=export_config.query,
                    query_parameters=export_config.query_parameters,
                    overwrite=True,
                    use_query_cache=use_query_cache,
                    job_labels=job_labels,
                )
                if query_job is not None:
                    query_jobs.append(query_job)

            logging.info("Waiting on [%d] query jobs to finish", len(query_jobs))
            for query_job in query_jobs:
                query_job.result()

            logging.info("Completed [%d] query jobs.", len(query_jobs))

            extract_jobs_to_config: Dict[bigquery.ExtractJob, ExportQueryConfig] = {}
            for export_config in export_configs:
                extract_job = self.export_table_to_cloud_storage_async(
                    source_table_address=export_config.intermediate_table_address,
                    destination_uri=export_config.output_uri,
                    destination_format=export_config.output_format,
                    print_header=print_header,
                    job_labels=job_labels,
                )
                if extract_job is not None:
                    extract_jobs_to_config[extract_job] = export_config

            logging.info(
                "Waiting on [%d] extract jobs to finish", len(extract_jobs_to_config)
            )

            for export_job, export_config in extract_jobs_to_config.items():
                job_file_paths: List[GcsfsFilePath] = []

                try:
                    # Wait for the job to finish
                    export_job.result()
                    destination_uris: List[str] = export_job.destination_uris
                    file_counts = export_job.destination_uri_file_counts

                    # For each destination uri and its corresponding file count, get
                    # its file paths and append it to the list of exported file paths
                    for destination_uri, file_count in zip(
                        destination_uris, file_counts
                    ):
                        job_file_paths.extend(
                            get_file_destinations_for_bq_export(
                                destination_uri, file_count
                            )
                        )

                        # TODO(#33882): Delete any stale files from a previous export

                except Exception as e:
                    logging.exception(
                        "Extraction failed for table: %s",
                        export_config.intermediate_table_name,
                    )
                    raise e

                exported_configs_and_paths.append((export_config, job_file_paths))

            logging.info("Completed [%d] extract jobs.", len(extract_jobs_to_config))

        finally:
            logging.info(
                "Deleting [%d] temporary intermediate tables.", len(export_configs)
            )
            for export_config in export_configs:
                self.delete_table(
                    address=export_config.intermediate_table_address,
                    not_found_ok=True,
                )
            logging.info("Done deleting temporary intermediate tables.")

        return exported_configs_and_paths

    def delete_table(
        self, address: BigQueryAddress, *, not_found_ok: bool = False
    ) -> None:
        table_ref = self._table_ref_for_address(address)
        logging.info("Deleting table/view [%s]", address.to_str())
        self.client.delete_table(table_ref, not_found_ok=not_found_ok)

    def run_query_async(
        self,
        *,
        query_str: str,
        use_query_cache: bool,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        http_timeout: Optional[float] = None,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.QueryJob:
        # TODO(#35122): should we parse labels from query str?
        job_config = bigquery.QueryJobConfig(
            use_query_cache=use_query_cache, labels=self._build_labels(job_labels)
        )
        job_config.query_parameters = query_parameters or []

        return self.client.query(
            query=query_str,
            location=self.region,
            job_config=job_config,
            timeout=http_timeout,
        )

    def paged_read_and_process(
        self,
        *,
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

    def create_table_from_query_async(
        self,
        *,
        address: BigQueryAddress,
        query: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        overwrite: Optional[bool] = False,
        clustering_fields: Optional[List[str]] = None,
        time_partitioning: bigquery.TimePartitioning | None = None,
        use_query_cache: bool,
        job_labels: Optional[list[ResourceLabel]] = None,
        output_schema: Optional[list[bigquery.SchemaField]] = None,
    ) -> bigquery.QueryJob:
        # If overwrite is False, errors if the table already exists and contains data. Else, overwrites the table if
        # it already exists.
        write_disposition = (
            bigquery.job.WriteDisposition.WRITE_TRUNCATE
            if overwrite
            else bigquery.job.WriteDisposition.WRITE_EMPTY
        )

        return self._insert_into_table_from_query_async(
            destination_address=address,
            query=query,
            query_parameters=query_parameters,
            write_disposition=write_disposition,
            clustering_fields=clustering_fields,
            time_partitioning=time_partitioning,
            use_query_cache=use_query_cache,
            job_labels=job_labels,
            output_schema=output_schema,
        )

    def create_table_from_query(
        self,
        *,
        address: BigQueryAddress,
        query: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        overwrite: Optional[bool] = False,
        clustering_fields: Optional[List[str]] = None,
        time_partitioning: bigquery.TimePartitioning | None = None,
        use_query_cache: bool,
        job_labels: Optional[list[ResourceLabel]] = None,
        output_schema: Optional[list[bigquery.SchemaField]] = None,
    ) -> bigquery.table.RowIterator:
        query_job = self.create_table_from_query_async(
            address=address,
            query=query,
            query_parameters=query_parameters,
            overwrite=overwrite,
            clustering_fields=clustering_fields,
            time_partitioning=time_partitioning,
            use_query_cache=use_query_cache,
            job_labels=job_labels,
            output_schema=output_schema,
        )
        try:
            result = query_job.result()
        except Exception as e:
            logging.error(
                "Create table job [%s] failed for table [%s] with errors: %s",
                query_job.job_id,
                address.to_str(),
                query_job.errors,
            )
            raise e

        try:
            self.apply_row_level_permissions(self.get_table(address))
        except Exception:
            logging.error(
                "Failed to apply row-level permissions to table [%s]. "
                "Table was created successfully, but row-level permissions were not applied.",
                address.to_str(),
            )

        return result

    def _insert_into_table_from_table_async(
        self,
        *,
        source_address: BigQueryAddress,
        destination_address: BigQueryAddress,
        use_query_cache: bool,
        source_data_filter_clause: Optional[str] = None,
        source_to_destination_column_mapping: Optional[Dict[str, str]] = None,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.QueryJob:
        """Appends data from a source table into a destination table using an INSERT INTO
        statement for columns that exist in both tables, with an optional
        source_data_filter_clause applied. Columns missing from the source table will
        be hydrated with the destination table's default value if set, otherwise with
        NULL. If a source_to_destination_column_mapping is provided, the query will additionally
        use the provided mapping to match columns from the source table to the destination
        table.

        Note: Types for shared columns of the two tables must match. If a REQUIRED field
        in the destination table is missing from the source table, the query will likely
        fail unless a non-null default is set. For more info, see big query docs:
        https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax#insert_statement
        """
        insert_job_config = bigquery.job.QueryJobConfig(
            use_query_cache=use_query_cache,
            labels=self._build_labels(job_labels, address=destination_address),
        )

        project_specific_source_address = source_address.to_project_specific_address(
            self.project_id
        )
        project_specific_destination_address = (
            destination_address.to_project_specific_address(self.project_id)
        )

        if not self.table_exists(destination_address):
            raise ValueError(
                f"Destination table [{project_specific_destination_address.to_str()}]"
                f" does not exist!"
            )

        source_table = self.get_table(source_address)
        destination_table = self.get_table(destination_address)

        all_dst_columns = {field.name for field in destination_table.schema}
        all_src_columns = {field.name for field in source_table.schema}

        src_columns_to_select, dst_columns_to_select = [], []
        for col in all_dst_columns & all_src_columns:
            # In case column mapping contains columns mapped to the same name
            # as the source and destination column, we don't want to add them twice
            if col not in (source_to_destination_column_mapping or {}):
                src_columns_to_select.append(col)
                dst_columns_to_select.append(col)

        cols_missing_in_src, cols_missing_in_dst = [], []
        for src_col, dst_col in (source_to_destination_column_mapping or {}).items():
            if src_col in all_src_columns and dst_col in all_dst_columns:
                src_columns_to_select.append(src_col)
                dst_columns_to_select.append(dst_col)
            if src_col not in all_src_columns:
                cols_missing_in_src.append(src_col)
            if dst_col not in all_dst_columns:
                cols_missing_in_dst.append(dst_col)

        if cols_missing_in_src or cols_missing_in_dst:
            error_msg = "Mapping in source_to_destination_column_mapping contains invalid columns:\n"
            if cols_missing_in_src:
                error_msg += f"Source columns missing from source table: [{', '.join(cols_missing_in_src)}]\n"
                error_msg += f"Source columns: [{', '.join(all_src_columns)}]\n"
            if cols_missing_in_dst:
                error_msg += f"Destination columns missing from destination table: [{', '.join(cols_missing_in_dst)}]\n"
                error_msg += f"Destination columns: [{', '.join(all_dst_columns)}]\n"
            raise ValueError(error_msg)

        query = (
            f"INSERT INTO {project_specific_destination_address.format_address_for_query()} "
            f"({', '.join(dst_columns_to_select)}) \n"
            f"SELECT {', '.join(src_columns_to_select)} \n"
            f"FROM {project_specific_source_address.format_address_for_query()}"
        )

        if source_data_filter_clause:
            self._validate_source_data_filter_clause(source_data_filter_clause)
            query = f"{query} \n {source_data_filter_clause}"

        logging.info(
            "Inserting data to: [%s] from [%s]",
            destination_address.to_str(),
            source_address.to_str(),
        )

        return self.client.query(
            query=query, location=self.region, job_config=insert_job_config
        )

    @staticmethod
    def _validate_source_data_filter_clause(filter_clause: str) -> None:
        if not filter_clause.startswith("WHERE"):
            raise ValueError(
                f"Found filter clause [{filter_clause}] that does not begin with WHERE"
            )

    def _insert_into_table_from_query_async(
        self,
        *,
        destination_address: BigQueryAddress,
        query: str,
        write_disposition: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]],
        allow_field_additions: bool = False,
        clustering_fields: Optional[List[str]],
        time_partitioning: bigquery.TimePartitioning | None,
        use_query_cache: bool,
        job_labels: Optional[list[ResourceLabel]],
        output_schema: Optional[list[bigquery.SchemaField]],
    ) -> bigquery.QueryJob:
        """Inserts the results of the given query into the table at the given address.
        Creates a table if one does not yet exist. If |allow_field_additions| is set to
        False and the table exists, the schema of the query result must match the schema
        of the destination table.

        Args:
            destination_address: The address of the table where the result should be
                inserted.
            query: The query to run. The result will be loaded into the table.
            write_disposition: The write disposition for the query. Must be one of:
                bigquery.WriteDisposition.WRITE_TRUNCATE: Overwrite the table.
                bigquery.WriteDisposition.WRITE_APPEND: Append to the table.
                bigquery.WriteDisposition.WRITE_EMPTY: Error if the table exists and is non-empty.
            query_parameters: Optional parameters for the query.
            allow_field_additions: Whether or not to allow new columns to be created in
                the destination table if the schema in the query result does not exactly
                match the destination table. Defaults to False.
            clustering_fields: Columns by which to cluster the table.
            time_partitioning: Configuration for time period partitioning that should be
                applied to the table.
            use_query_cache: Whether to look for the result in the query cache. See:
                https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationQuery.FIELDS.use_query_cache
            job_labels: Metadata labels to attach to the BigQuery QueryJob, recorded in the JOBS view.
            output_schema: The schema for the output table

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

        destination_table_ref = self._table_ref_for_address(destination_address)

        self.create_dataset_if_necessary(destination_address.dataset_id)

        query_job_config = bigquery.job.QueryJobConfig(
            use_query_cache=use_query_cache,
            labels=self._build_labels(job_labels, address=destination_address),
        )
        query_job_config.destination = destination_table_ref

        query_job_config.write_disposition = write_disposition
        query_job_config.query_parameters = query_parameters or []

        if clustering_fields or time_partitioning or output_schema:
            query_job_config.clustering_fields = clustering_fields
            query_job_config.time_partitioning = time_partitioning

            # If we are truncating, go ahead and delete the table (if it exists)
            if write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE:
                self.delete_table(destination_address, not_found_ok=True)
                # pre-create the table with the new schema if there is an output_schema
                # specified (you can't specify the schema as part of the query job)
                if output_schema:
                    self.create_table_with_schema(
                        address=destination_address,
                        schema_fields=output_schema,
                        clustering_fields=clustering_fields,
                        time_partitioning=time_partitioning,
                    )
                    # Make sure to change the job config to APPEND so that the schema
                    # changes are not lost
                    query_job_config.write_disposition = (
                        bigquery.WriteDisposition.WRITE_APPEND
                    )
            # If we are not truncating, check if the existing table is in the same
            # state as the new table would be
            else:
                try:
                    existing_table = self.get_table(destination_address)
                    if existing_table.clustering_fields != clustering_fields:
                        raise ValueError(
                            f"Trying to materialize into a table using different "
                            f"clustering fields than what currently exists requires "
                            f"'WRITE_TRUNCATE' write_disposition."
                            f""
                            f"Current clustering fields: {existing_table.clustering_fields}"
                            f"New clustering fields: {clustering_fields}"
                        )

                    if existing_table.time_partitioning != time_partitioning:
                        raise ValueError(
                            f"Found updated time_partitioning configuration for "
                            f"destination table [{destination_address.to_str()}]. "
                            f"Cannot write to the table with write_disposition "
                            f"[{write_disposition}]."
                            f""
                            f"Current time_partitioning: {existing_table.time_partitioning}"
                            f"New time_partitioning: {time_partitioning}"
                        )

                    if output_schema is not None and not are_bq_schemas_same(
                        existing_table.schema, output_schema
                    ):
                        raise ValueError(
                            f"Found updated table schema for "
                            f"destination table [{destination_address.to_str()}]. "
                            f"Cannot write to the table with write_disposition "
                            f"[{write_disposition}]."
                            f""
                            f"Current schema: {existing_table.schema}"
                            f"New schema: {output_schema}"
                        )
                except exceptions.NotFound:
                    pass

        if allow_field_additions:
            query_job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]

        logging.info(
            "Inserting into table [%s] result of query (%s)",
            destination_address.to_str(),
            write_disposition,
        )

        return self.client.query(
            query=query,
            location=self.region,
            job_config=query_job_config,
        )

    def insert_into_table_from_query_async(
        self,
        *,
        destination_address: BigQueryAddress,
        query: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        allow_field_additions: bool = False,
        clustering_fields: Optional[List[str]] = None,
        time_partitioning: bigquery.TimePartitioning | None = None,
        use_query_cache: bool,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.QueryJob:
        return self._insert_into_table_from_query_async(
            destination_address=destination_address,
            query=query,
            query_parameters=query_parameters,
            allow_field_additions=allow_field_additions,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            clustering_fields=clustering_fields,
            time_partitioning=time_partitioning,
            use_query_cache=use_query_cache,
            job_labels=job_labels,
            output_schema=None,
        )

    def insert_into_table_from_query(
        self,
        *,
        destination_address: BigQueryAddress,
        query: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        allow_field_additions: bool = False,
        clustering_fields: Optional[List[str]] = None,
        time_partitioning: bigquery.TimePartitioning | None = None,
        use_query_cache: bool,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.table.RowIterator:
        query_should_not_create_table = not self.table_exists(destination_address)
        query_job = self._insert_into_table_from_query_async(
            destination_address=destination_address,
            query=query,
            query_parameters=query_parameters,
            allow_field_additions=allow_field_additions,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            clustering_fields=clustering_fields,
            time_partitioning=time_partitioning,
            use_query_cache=use_query_cache,
            job_labels=job_labels,
            output_schema=None,
        )
        try:
            result = query_job.result()
        except Exception as e:
            logging.error(
                "Insert into table job [%s] failed for table [%s] with errors: %s",
                query_job.job_id,
                destination_address.to_str(),
                query_job.errors,
            )
            raise e

        if query_should_not_create_table:
            return result

        try:
            self.apply_row_level_permissions(self.get_table(destination_address))
        except Exception:
            logging.error(
                "Failed to apply row-level permissions to table [%s]. "
                "Insert query was successful, but row-level permissions were not applied.",
                destination_address.to_str(),
            )

        return result

    def insert_into_table_from_table_async(
        self,
        *,
        source_address: BigQueryAddress,
        destination_address: BigQueryAddress,
        use_query_cache: bool,
        source_data_filter_clause: Optional[str] = None,
        source_to_destination_column_mapping: Optional[Dict[str, str]] = None,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.QueryJob:
        return self._insert_into_table_from_table_async(
            source_address=source_address,
            destination_address=destination_address,
            source_data_filter_clause=source_data_filter_clause,
            use_query_cache=use_query_cache,
            source_to_destination_column_mapping=source_to_destination_column_mapping,
            job_labels=job_labels,
        )

    def stream_into_table(self, address: BigQueryAddress, rows: Sequence[Dict]) -> None:
        logging.info("Inserting %d rows into %s", len(rows), address.to_str())

        # Warn on any large rows
        for row in rows:
            estimated_size = total_size(row, include_duplicates=True)
            if estimated_size > (100 * 2**10):  # 100 KiB
                logging.warning("Row is larger than 100 KiB: %s", repr(row)[:1000])

        errors = self.client.insert_rows(self.get_table(address), rows)
        if errors:
            raise RuntimeError(
                f"Failed to insert rows into {address.to_str()}:\n"
                + "\n".join(str(error) for error in errors)
            )

    def load_into_table_async(
        self,
        *,
        address: BigQueryAddress,
        rows: Sequence[Dict],
        write_disposition: str = bigquery.WriteDisposition.WRITE_APPEND,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.job.LoadJob:
        logging.info("Inserting %d rows into %s", len(rows), address.to_str())

        # Warn on any large rows
        for row in rows:
            json_row = json.dumps(row)
            estimated_size = len(row)
            if estimated_size > (100 * 2**10):  # 100 KiB
                logging.warning("Row is larger than 100 KiB: %s", json_row[:1000])

        job_config = bigquery.LoadJobConfig(
            labels=self._build_labels(job_labels, address=address)
        )
        job_config.write_disposition = write_disposition

        return self.client.load_table_from_json(
            rows, self.get_table(address), job_config=job_config
        )

    def delete_from_table_async(
        self,
        address: BigQueryAddress,
        *,
        filter_clause: Optional[str] = None,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> bigquery.QueryJob:
        if filter_clause and not filter_clause.startswith("WHERE"):
            raise ValueError(
                "Cannot delete from a table without a valid filter clause starting with WHERE."
            )

        # BQ requires a WHERE clause for all DELETE statements. The `WHERE true` filter deletes all rows.
        filter_str = filter_clause if filter_clause else "WHERE true"
        project_specific_address = address.to_project_specific_address(self.project_id)
        delete_query = f"DELETE FROM {project_specific_address.format_address_for_query()} {filter_str}"

        logging.info(
            "Deleting data from %s matching this filter: %s", address, filter_str
        )

        if not job_labels:
            job_labels = []
        job_labels = [*job_labels, *address.bq_job_labels]

        return self.run_query_async(
            query_str=delete_query, use_query_cache=False, job_labels=job_labels
        )

    def materialize_view_to_table(
        self,
        view: BigQueryView,
        use_query_cache: bool,
        view_configuration_changed: bool,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> BigQueryViewMaterializationResult:
        if view.materialized_address is None:
            raise ValueError(
                "Trying to materialize a view that does not have a set "
                "materialized_address."
            )

        destination_address = view.materialized_address
        logging.info(
            "Materializing %s into a table with address: %s",
            view.address.to_str(),
            destination_address.to_str(),
        )

        write_disposition = (
            bigquery.job.WriteDisposition.WRITE_TRUNCATE
            if view_configuration_changed
            else bigquery.job.WriteDisposition.WRITE_TRUNCATE_DATA
        )
        materialize_job = self._insert_into_table_from_query_async(
            destination_address=view.materialized_address,
            query=view.direct_select_query,
            query_parameters=None,
            write_disposition=write_disposition,
            clustering_fields=view.clustering_fields,
            time_partitioning=view.time_partitioning,
            use_query_cache=use_query_cache,
            job_labels=job_labels,
            output_schema=view.materialized_table_schema,
        )
        materialize_job.result()

        table = self.get_table(destination_address)
        description = view.materialized_table_bq_description
        if description == table.description:
            return BigQueryViewMaterializationResult(
                view_address=view.address,
                materialized_table=table,
                completed_materialization_job=materialize_job,
            )

        table.description = description
        updated_table = self.client.update_table(
            table, ["description"], retry=UPDATE_DESCRIPTION_RETRY
        )
        return BigQueryViewMaterializationResult(
            view_address=view.address,
            materialized_table=updated_table,
            completed_materialization_job=materialize_job,
        )

    def create_table_with_schema(
        self,
        *,
        address: BigQueryAddress,
        schema_fields: List[bigquery.SchemaField],
        clustering_fields: Optional[List[str]] = None,
        time_partitioning: bigquery.TimePartitioning | None = None,
        require_partition_filter: bool | None = None,
    ) -> bigquery.Table:

        if require_partition_filter is not None and time_partitioning is None:
            raise ValueError(
                "Cannot require a partition filter on a table that is not partitioned."
            )

        self._validate_schema(address, schema_fields)

        if self.table_exists(address):
            raise ValueError(
                f"Trying to create a table that already exists: {address.to_str()}."
            )

        table_ref = self._table_ref_for_address(address)
        table = bigquery.Table(table_ref, schema_fields)

        if clustering_fields:
            table.clustering_fields = clustering_fields

        if time_partitioning:
            table.time_partitioning = time_partitioning

        if require_partition_filter is not None:
            table.require_partition_filter = require_partition_filter

        logging.info("Creating table %s", address.to_str())
        return self.create_table(table)

    def create_external_table(
        self,
        *,
        address: BigQueryAddress,
        external_data_config: ExternalConfig,
        allow_auto_detect_schema: bool,
    ) -> bigquery.Table:
        if not allow_auto_detect_schema:
            if external_data_config.autodetect:
                raise ValueError(
                    f"Cannot set autodetect=True for external table "
                    f"[{address.to_str()}] when allow_auto_detect_schema=False."
                )
            if not external_data_config.schema:
                raise ValueError(
                    f"Must provide a non-empty schema for [{address.to_str()}] when "
                    f"allow_auto_detect_schema=False."
                )

        table_ref = self._table_ref_for_address(address)
        table = bigquery.Table(table_ref)
        table.external_data_configuration = external_data_config
        return self.create_table(table, exists_ok=False)

    def update_external_table(
        self,
        *,
        address: BigQueryAddress,
        external_data_config: ExternalConfig,
        allow_auto_detect_schema: bool,
    ) -> bigquery.Table:
        if not allow_auto_detect_schema:
            if external_data_config.autodetect:
                raise ValueError(
                    f"Cannot set autodetect=True for external table "
                    f"[{address.to_str()}] when allow_auto_detect_schema=False."
                )
            if not external_data_config.schema:
                raise ValueError(
                    f"Must provide a non-empty schema for [{address.to_str()}] when "
                    f"allow_auto_detect_schema=False."
                )

        table = self.get_table(address)
        if not table.external_data_configuration:
            raise ValueError(
                f"Cannot convert a normal table [{address.to_str()}] to an external "
                f"table."
            )

        table.external_data_configuration = external_data_config
        if external_data_config.schema:
            # The schema on the ExternalConfig is only really used at table creation
            # time, but the schema is stored in the Table schema field and subsequent
            # updates will require that the schema is set on the table.
            table.schema = external_data_config.schema

        updated_table = self.client.update_table(
            table,
            [
                # Applies all non-schema updates to the external table config
                "external_data_configuration",
                # Applies schema updates
                "schema",
            ],
        )
        return updated_table

    def set_table_expiration(
        self, address: BigQueryAddress, expiration: datetime.datetime
    ) -> None:
        table = self.get_table(address)
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
        address: BigQueryAddress,
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
            "Schema for table %s has [%s] excess fields to drop.",
            address.to_str(),
            len(deprecated_fields),
        )

        if not allow_field_deletions:
            raise ValueError(
                f"Found deprecated fields [{deprecated_fields}] for table:"
                f" {address.to_str()} but field deletions is not allowed."
            )

        drop_statements = ", ".join(
            [f"DROP COLUMN {field.name}" for field in deprecated_fields]
        )

        project_specific_address = address.to_project_specific_address(self.project_id)
        drop_query = f"""
            ALTER TABLE {project_specific_address.format_address_for_query()} {drop_statements}
        """

        return self.run_query_async(
            query_str=drop_query,
            use_query_cache=False,
        )

    @staticmethod
    def _valid_field_types(schema_field: bigquery.SchemaField) -> Set[str]:
        """Returns the field type and an alias for that type if applicable."""
        if schema_field.field_type in _TYPE_ALIASES:
            return {schema_field.field_type, _TYPE_ALIASES[schema_field.field_type]}
        return {schema_field.field_type}

    def _assert_is_valid_schema_field_update(
        self,
        table_address: BigQueryAddress,
        old_schema_field: bigquery.SchemaField,
        new_schema_field: bigquery.SchemaField,
    ) -> None:
        if old_schema_field.field_type not in self._valid_field_types(new_schema_field):
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
        *,
        address: BigQueryAddress,
        desired_schema_fields: List[bigquery.SchemaField],
        allow_field_deletions: bool,
    ) -> None:
        self._validate_schema(address, desired_schema_fields)
        table_ref = self._table_ref_for_address(address)
        try:
            table = self.client.get_table(table_ref)
        except exceptions.NotFound as e:
            raise ValueError(
                f"Cannot update schema fields for a table that does not exist: "
                f"{address.to_str()}"
            ) from e

        existing_schema = table.schema
        desired_schema_map = {field.name: field for field in desired_schema_fields}

        for field in existing_schema:
            if field.name in desired_schema_map:
                self._assert_is_valid_schema_field_update(
                    address, field, desired_schema_map[field.name]
                )

        # Remove any deprecated fields first as it involves copying the entire view
        removal_job = self._remove_unused_fields_from_schema(
            address=address,
            existing_schema=existing_schema,
            desired_schema_fields=desired_schema_fields,
            allow_field_deletions=allow_field_deletions,
        )

        if removal_job:
            # Wait for the removal job to complete before running the job to add fields
            removal_job.result()

            # If we removed fields, we need to query for the table again to get the
            # updated schema.
            table = self.client.get_table(table_ref)

        self._add_or_update_existing_schema_fields(
            table=table,
            desired_schema_fields=desired_schema_fields,
        )

    def copy_table(
        self,
        *,
        source_table_address: BigQueryAddress,
        destination_dataset_id: str,
        schema_only: bool = False,
        overwrite: bool = False,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> Optional[bigquery.job.CopyJob]:
        source_table = self.get_table(source_table_address)
        destination_table_address = BigQueryAddress(
            dataset_id=destination_dataset_id, table_id=source_table_address.table_id
        )

        # If we are copying the contents then we can only copy actual tables.
        if not schema_only and source_table.table_type != "TABLE":
            logging.warning(
                "Skipping copy of item with type [%s]: [%s]",
                source_table.table_type,
                source_table.table_id,
            )

            return None

        source_table_ref = self._table_ref_for_address(source_table_address)
        destination_table_ref = self._table_ref_for_address(destination_table_address)

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
                exists_ok=overwrite,
            )
        else:
            job_config = bigquery.CopyJobConfig(
                labels=self._build_labels(job_labels, address=destination_table_address)
            )
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
        *,
        source_dataset_id: str,
        destination_dataset_id: str,
        schema_only: bool = False,
        overwrite_destination_tables: bool = False,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> None:
        # Get source tables
        source_table_addresses = [
            BigQueryAddress(dataset_id=table.dataset_id, table_id=table.table_id)
            for table in self.list_tables(source_dataset_id)
        ]

        # Check existing destination tables
        initial_destination_addresses = [
            BigQueryAddress(dataset_id=table.dataset_id, table_id=table.table_id)
            for table in self.list_tables(destination_dataset_id)
        ]

        if overwrite_destination_tables:
            for address in initial_destination_addresses:
                if address not in source_table_addresses:
                    self.delete_table(address)
        else:
            if initial_destination_addresses:
                raise ValueError(
                    f"Destination dataset [{destination_dataset_id}] for copy is not empty."
                )

        logging.info(
            "Copying tables in dataset [%s] to dataset [%s]",
            source_dataset_id,
            destination_dataset_id,
        )
        copy_jobs: List[bigquery.job.CopyJob] = []
        for source_table_address in source_table_addresses:
            copy_job = self.copy_table(
                source_table_address=source_table_address,
                destination_dataset_id=destination_dataset_id,
                schema_only=schema_only,
                overwrite=overwrite_destination_tables,
                job_labels=job_labels,
            )

            if copy_job:
                copy_jobs.append(copy_job)

        if copy_jobs:
            self.wait_for_big_query_jobs(jobs=copy_jobs)

    def backup_dataset_tables_if_dataset_exists(
        self,
        dataset_id: str,
        *,
        job_labels: Optional[list[ResourceLabel]] = None,
    ) -> Optional[str]:
        backup_dataset_id = self.add_timestamp_suffix_to_dataset_id(
            f"{dataset_id}_backup"
        )

        if not self.dataset_exists(dataset_id):
            return None

        self.create_dataset_if_necessary(
            backup_dataset_id,
            default_table_expiration_ms=DATASET_BACKUP_TABLE_EXPIRATION_MS,
        )
        self.copy_dataset_tables(
            source_dataset_id=dataset_id,
            destination_dataset_id=backup_dataset_id,
            job_labels=job_labels,
        )
        return backup_dataset_id

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
        *,
        source_dataset_id: str,
        destination_dataset_id: str,
        overwrite_destination_tables: bool = False,
        timeout_sec: float = DEFAULT_CROSS_REGION_COPY_TIMEOUT_SEC,
    ) -> None:
        # Get source tables
        source_table_ids = {
            table.table_id
            for table in self.list_tables_excluding_views(source_dataset_id)
        }
        source_tables_by_id = {
            table_id: self.get_table(
                BigQueryAddress(dataset_id=source_dataset_id, table_id=table_id)
            )
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
                    self.delete_table(
                        BigQueryAddress(
                            dataset_id=destination_dataset_id, table_id=table_id
                        )
                    )
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

        if not self.dataset_exists(destination_dataset_id):
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
                    destination_address = BigQueryAddress(
                        dataset_id=destination_dataset_id, table_id=destination_table_id
                    )
                    destination_table = self.get_table(destination_address)
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
