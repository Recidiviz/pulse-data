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
import logging
import time
from collections import defaultdict
from concurrent import futures
from typing import List, Optional, Iterator, Callable, Dict, Any, Sequence

import pytz
from google.api_core.future.polling import PollingFuture
from google.cloud import bigquery, exceptions
from google.cloud.bigquery_datatransfer import (
    StartManualTransferRunsRequest,
    ScheduleOptions,
    TransferState,
    TransferConfig,
    DataTransferServiceClient,
)
from google.protobuf import timestamp_pb2
from more_itertools import one

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.export.export_query_config import ExportQueryConfig
from recidiviz.utils import metadata

_clients_by_project_id_by_region: Dict[str, Dict[str, bigquery.Client]] = defaultdict(
    dict
)


def client(project_id: str, region: str) -> bigquery.Client:
    global _clients_by_project_id_by_region
    if (
        project_id not in _clients_by_project_id_by_region
        or region not in _clients_by_project_id_by_region[project_id]
    ):
        _clients_by_project_id_by_region[project_id][region] = bigquery.Client(
            project=project_id, location=region
        )
    return _clients_by_project_id_by_region[project_id][region]


# The urllib3 library (used by the Google BigQuery client) has a default limit of 10 connections and when we do
# concurrent operations with the same client we see "urllib3.connectionpool:Connection pool is full, discarding
# connection" errors when this number increased.
# In the future, we could increase this number by playing around with increasing the pool size per this post:
# https://github.com/googleapis/python-storage/issues/253
BIG_QUERY_CLIENT_MAX_CONNECTIONS = 10

DATASET_BACKUP_TABLE_EXPIRATION_MS = 7 * 24 * 60 * 60 * 1000  # 7 days

CROSS_REGION_COPY_STATUS_ATTEMPT_SLEEP_TIME_SEC = 10
DEFAULT_CROSS_REGION_COPY_TIMEOUT_SEC = 10 * 60
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
    ) -> None:
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
        """Returns a list of tables in the dataset with the given dataset id."""

    @abc.abstractmethod
    def create_table(self, table: bigquery.Table) -> bigquery.Table:
        """Creates a new table in big query if it does not already exist, otherwise raises an AlreadyExists error.

        Args:
            table: The Table to create.

        Returns:
            The Table that was just created.
        """

    @abc.abstractmethod
    def create_or_update_view(self, view: BigQueryView) -> bigquery.Table:
        """Create a View if it does not exist, or update its query if it does.

        This runs synchronously and waits for the job to complete.

        Args:
            view: The View to create or update.

        Returns:
            The Table that was just created.
        """

    @abc.abstractmethod
    def load_table_from_table_async(
        self,
        source_dataset_id: str,
        source_table_id: str,
        destination_dataset_id: str,
        destination_table_id: str,
        allow_field_additions: bool = False,
    ) -> bigquery.QueryJob:
        """Loads data from a source table and writes to a destination table.

        This starts the job, but does not wait until it completes. It will raise an error if the destination table
        does not exist.

        Because we are using bigquery.WriteDisposition.WRITE_TRUNCATE, the destination table's
        data will be completely wiped and overwritten with the contents of the source table.

        Args:
            source_dataset_id: The name of the source dataset.
            source_table_id: The name of the source table from which to query.
            destination_dataset_id: The name of the destination dataset.
            destination_table_id: The name of the table to insert into.
            allow_field_additions: Whether or not to allow new columns to be created in the destination table if the
                schema in the source table does not exactly match the destination table. Defaults to False.

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def load_table_from_cloud_storage_async(
        self,
        source_uri: str,
        destination_dataset_ref: bigquery.DatasetReference,
        destination_table_id: str,
        destination_table_schema: List[bigquery.SchemaField],
        skip_leading_rows: int = 0,
    ) -> bigquery.job.LoadJob:
        """Loads a table from CSV data in GCS to BigQuery.

        Given a desired table name, source data URI and destination schema, loads the table into BigQuery.

        This starts the job, but does not wait until it completes.

        Tables are created if they do not exist, and overwritten if they do exist.

        Because we are using bigquery.WriteDisposition.WRITE_TRUNCATE, the table's
        data will be completely wiped and overwritten with the contents of the CSV.

        Args:
            source_uri: The path in Google Cloud Storage to read contents from (starts with 'gs://').
            destination_dataset_ref: The BigQuery dataset to load the table into. Gets created
                if it does not already exist.
            destination_table_id: String name of the table to import.
            destination_table_schema: Defines a list of field schema information for each expected column in the input
                file.
            skip_leading_rows: Optional number of leading rows to skip. Defaults to zero
        Returns:
            The LoadJob object containing job details.
        """

    @abc.abstractmethod
    def export_table_to_cloud_storage_async(
        self,
        source_table_dataset_ref: bigquery.dataset.DatasetReference,
        source_table_id: str,
        destination_uri: str,
        destination_format: bigquery.DestinationFormat,
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
        self, export_configs: List[ExportQueryConfig], print_header: bool
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
        """

    @abc.abstractmethod
    def run_query_async(
        self,
        query_str: str,
        query_parameters: List[bigquery.ScalarQueryParameter] = None,
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

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def paged_read_and_process(
        self,
        query_job: bigquery.QueryJob,
        page_size: int,
        process_fn: Callable[[bigquery.table.Row], None],
    ) -> None:
        """Reads the given result set from the given query job in pages to limit how many rows are read into memory at
        any given time, processing the results of each row with the given callable.

        Args:
            query_job: the query job from which to process results.
            page_size: the maximum number of rows to read in at a time.
            process_fn: a callable function which takes in one row from the result set and performs some operation.
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
        dataset_id: str,
        table_id: str,
        query: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        overwrite: Optional[bool] = False,
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

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def insert_into_table_from_table_async(
        self,
        source_dataset_id: str,
        source_table_id: str,
        destination_dataset_id: str,
        destination_table_id: str,
        source_data_filter_clause: Optional[str] = None,
        hydrate_missing_columns_with_null: bool = False,
        allow_field_additions: bool = False,
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

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def insert_into_table_from_cloud_storage_async(
        self,
        source_uri: str,
        destination_dataset_ref: bigquery.DatasetReference,
        destination_table_id: str,
        destination_table_schema: List[bigquery.SchemaField],
    ) -> bigquery.job.LoadJob:
        """Inserts rows from CSV data in GCS into a table in BigQuery.

        Given a desired table name, source data URI and destination schema, inserts the data into the BigQuery table.

        This starts the job, but does not wait until it completes.

        Tables are created if they do not exist, and rows are merely appended if they do exist.

        Args:
            source_uri: The path in Google Cloud Storage to read contents from (starts with 'gs://').
            destination_dataset_ref: The BigQuery dataset to load the table into. Gets created
                if it does not already exist.
            destination_table_id: String name of the table to import.
            destination_table_schema: Defines a list of field schema information for each expected column in the input
                file.
        Returns:
            The LoadJob object containing job details.
        """

    @abc.abstractmethod
    def insert_into_table_from_query(
        self,
        *,
        destination_dataset_id: str,
        destination_table_id: str,
        query: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        allow_field_additions: bool = False,
        write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_APPEND,
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

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def delete_from_table_async(
        self, dataset_id: str, table_id: str, filter_clause: str
    ) -> bigquery.QueryJob:
        """Deletes rows from the given table that match the filter clause.

        Args:
            dataset_id: The name of the dataset where the table lives.
            table_id: The name of the table to delete from.
            filter_clause: A clause that filters the contents of the table to determine which rows should be deleted.
                Must start with "WHERE".

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def materialize_view_to_table(self, view: BigQueryView) -> bigquery.Table:
        """Materializes the result of a view's view_query into a table. The view's
        materialized_address must be set. The resulting table is put in the same
        project as the view, and it overwrites any previous materialization of the view.

        Args:
            view: The BigQueryView to materialize into a table.
        """

    @abc.abstractmethod
    def create_table_with_schema(
        self, dataset_id: str, table_id: str, schema_fields: List[bigquery.SchemaField]
    ) -> bigquery.Table:
        """Creates a table in the given dataset with the given schema fields. Raises an error if a table with the same
        table_id already exists in the dataset.

        Args:
            dataset_id: The name of the dataset where the table should be created
            table_id: The name of the table to be created
            schema_fields: A list of fields defining the table's schema

        Returns:
            The bigquery.Table that is created.
        """

    @abc.abstractmethod
    def update_description(
        self, dataset_id: str, table_id: str, description: str
    ) -> bigquery.Table:
        """Updates the description for a given table / view.

        Args:
            dataset_id: The name of the dataset where the table/view lives
            table_id: The name of the table/view to update
            description: The new description string.

        Returns:
            The bigquery.Table result of the update.
        """

    @abc.abstractmethod
    def add_missing_fields_to_schema(
        self,
        dataset_id: str,
        table_id: str,
        desired_schema_fields: List[bigquery.SchemaField],
    ) -> None:
        """Updates the schema of the table to include the schema_fields if they are not already present in the
        Table's schema. Does not update the type or mode of any existing schema fields, and does not delete existing
        schema fields.

        Args:
            dataset_id: The name of the dataset where the table lives.
            table_id: The name of the table to add fields to.
            desired_schema_fields: A list of fields to add to the table
        """

    @abc.abstractmethod
    def remove_unused_fields_from_schema(
        self,
        dataset_id: str,
        table_id: str,
        desired_schema_fields: List[bigquery.SchemaField],
    ) -> Optional[bigquery.QueryJob]:
        """Updates the schema of the table to drop any columns not in desired_schema_fields. This will not add any
        fields to the table's schema.

        Args:
            dataset_id: The name of the dataset where the table lives.
            table_id: The name of the table to drop fields from.
            desired_schema_fields: A list of fields to keep in the table. Any field not in this list will be dropped.

        Returns:
            If there are fields to be removed, returns a QueryJob which will contain the results once the query is
            complete.
        """

    @abc.abstractmethod
    def update_schema(
        self,
        dataset_id: str,
        table_id: str,
        desired_schema_fields: List[bigquery.SchemaField],
    ) -> None:
        """Updates the schema of the table to match the desired_schema_fields. This may result in both adding and
        dropping fields from the table's schema. Raises an exception if fields in desired_schema_fields conflict with
        existing fields' modes or types.

        Args:
            dataset_id: The name of the dataset where the table lives.
            table_id: The name of the table to modify.
            desired_schema_fields: A list of fields describing the desired table schema.
        """

    @abc.abstractmethod
    def delete_table(self, dataset_id: str, table_id: str) -> None:
        """Provided the |dataset_id| and |table_id|, attempts to delete the given table from BigQuery.

        Args:
            dataset_id: The name of the dataset where the table lives.
            table_id: The name of the table to delete.
        """

    @abc.abstractmethod
    def copy_dataset_tables_across_regions(
        self,
        source_dataset_id: str,
        destination_dataset_id: str,
        timeout_sec: float = DEFAULT_CROSS_REGION_COPY_TIMEOUT_SEC,
    ) -> None:
        """Copies tables (but NOT views) from |source_dataset_id| to
        |destination_dataset_id|. This only works if the datasets live in different
        regions (e.g. 'us' vs 'us-east1'), because the API for doing an inter-region
        dataset copy is inexplicably different than for copies between regions.
        """

    def copy_dataset_tables(
        self,
        source_dataset_id: str,
        destination_dataset_id: str,
    ) -> None:
        """Copies all tables (but NOT views) in the source dataset to the destination
        dataset, which must be empty if it exists. If the destination dataset does not
        exist, we will create one.
        """

    def backup_dataset_tables_if_dataset_exists(
        self, dataset_id: str
    ) -> Optional[bigquery.DatasetReference]:
        """Creates a backup of all tables (but NOT views) in |dataset_id| in a new
        dataset with the format `my_dataset_backup_yyyy_mm_dd`. For example, the dataset
        `state` might backup to `state_backup_2021_05_06`.
        """

    def wait_for_big_query_jobs(self, jobs: Sequence[PollingFuture]) -> List[Any]:
        """Waits for a list of jobs to complete. These can by any of the async job types
        the BQ client returns, e.g. QueryJob, CopyJob, LoadJob, etc.

        If any job throws this function will throw with the first encountered exception.
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
            dataset = bigquery.Dataset(dataset_ref)

            if default_table_expiration_ms:
                logging.info(
                    "Setting default table expiration to %d milliseconds for dataset [%s].",
                    default_table_expiration_ms,
                    str(dataset_ref),
                )
                dataset.default_table_expiration_ms = default_table_expiration_ms
            return self.client.create_dataset(dataset)

        return dataset

    def dataset_exists(self, dataset_ref: bigquery.DatasetReference) -> bool:
        try:
            self.client.get_dataset(dataset_ref)
            return True
        except exceptions.NotFound:
            logging.warning("Dataset [%s] does not exist", str(dataset_ref))
            return False

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
            logging.warning(
                "Table [%s] does not exist in dataset [%s]", table_id, str(dataset_ref)
            )
            return False

    def list_tables(self, dataset_id: str) -> Iterator[bigquery.table.TableListItem]:
        return self.client.list_tables(dataset_id)

    def get_table(
        self, dataset_ref: bigquery.DatasetReference, table_id: str
    ) -> bigquery.Table:
        table_ref = dataset_ref.table(table_id)
        return self.client.get_table(table_ref)

    def create_table(self, table: bigquery.Table) -> bigquery.Table:
        return self.client.create_table(table, exists_ok=False)

    def create_or_update_view(self, view: BigQueryView) -> bigquery.Table:
        bq_view = bigquery.Table(view)
        bq_view.view_query = view.view_query
        bq_view.description = view.description

        try:
            table = self.get_table(
                self.dataset_ref_for_id(view.dataset_id), view.view_id
            )
        except exceptions.NotFound:
            logging.info("Creating view %s", str(bq_view))
            return self.client.create_table(bq_view)

        if table.table_type == "TABLE":
            raise ValueError(
                f"Cannot call create_or_update_view on table {view.view_id} in dataset {view.dataset_id}."
            )

        logging.info("Updating existing view [%s]", str(bq_view))
        return self.client.update_table(bq_view, ["view_query", "description"])

    def load_table_from_cloud_storage_async(
        self,
        source_uri: str,
        destination_dataset_ref: bigquery.DatasetReference,
        destination_table_id: str,
        destination_table_schema: List[bigquery.SchemaField],
        skip_leading_rows: int = 0,
    ) -> bigquery.job.LoadJob:

        return self._load_table_from_cloud_storage_async(
            source_uri=source_uri,
            destination_dataset_ref=destination_dataset_ref,
            destination_table_id=destination_table_id,
            destination_table_schema=destination_table_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            skip_leading_rows=skip_leading_rows,
        )

    def _load_table_from_cloud_storage_async(
        self,
        source_uri: str,
        destination_dataset_ref: bigquery.DatasetReference,
        destination_table_id: str,
        destination_table_schema: List[bigquery.SchemaField],
        write_disposition: bigquery.WriteDisposition,
        skip_leading_rows: int = 0,
    ) -> bigquery.job.LoadJob:
        """Triggers a load job, i.e. a job that will copy all of the data from the given Cloud Storage source into
        the given BigQuery destination. Returns once the job has been started."""

        self.create_dataset_if_necessary(destination_dataset_ref)

        destination_table_ref = destination_dataset_ref.table(destination_table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.schema = destination_table_schema
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.allow_quoted_newlines = True
        job_config.write_disposition = write_disposition
        job_config.skip_leading_rows = skip_leading_rows

        load_job = self.client.load_table_from_uri(
            source_uri, destination_table_ref, job_config=job_config
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
        destination_format: bigquery.DestinationFormat,
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
        self, export_configs: List[ExportQueryConfig], print_header: bool
    ) -> None:
        query_jobs = []
        for export_config in export_configs:
            query_job = self.create_table_from_query_async(
                dataset_id=export_config.intermediate_dataset_id,
                table_id=export_config.intermediate_table_name,
                query=export_config.query,
                query_parameters=export_config.query_parameters,
                overwrite=True,
            )
            if query_job is not None:
                query_jobs.append(query_job)

        logging.info("Waiting on [%d] query jobs to finish", len(query_jobs))
        for job in query_jobs:
            job.result()

        logging.info("Completed [%d] query jobs.", len(query_jobs))

        extract_jobs = []
        for export_config in export_configs:
            extract_job = self.export_table_to_cloud_storage_async(
                self.dataset_ref_for_id(export_config.intermediate_dataset_id),
                export_config.intermediate_table_name,
                export_config.output_uri,
                export_config.output_format,
                print_header,
            )
            if extract_job is not None:
                extract_jobs.append(extract_job)

        logging.info("Waiting on [%d] extract jobs to finish", len(extract_jobs))
        for job in extract_jobs:
            job.result()
        logging.info("Completed [%d] extract jobs.", len(extract_jobs))

        logging.info(
            "Deleting [%d] temporary intermediate tables.", len(export_configs)
        )
        for export_config in export_configs:
            self.delete_table(
                dataset_id=export_config.intermediate_dataset_id,
                table_id=export_config.intermediate_table_name,
            )
        logging.info("Done deleting temporary intermediate tables.")

    def delete_table(self, dataset_id: str, table_id: str) -> None:
        dataset_ref = self.dataset_ref_for_id(dataset_id)
        table_ref = dataset_ref.table(table_id)
        logging.info(
            "Deleting table/view [%s] from dataset [%s].", table_id, dataset_id
        )
        self.client.delete_table(table_ref)

    def run_query_async(
        self,
        query_str: str,
        query_parameters: List[bigquery.ScalarQueryParameter] = None,
    ) -> bigquery.QueryJob:
        job_config = bigquery.QueryJobConfig()
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
        process_fn: Callable[[bigquery.table.Row], None],
    ) -> None:
        logging.debug(
            "Querying for first page of results to perform %s...", process_fn.__name__
        )

        start_index = 0

        while True:
            page_rows: bigquery.table.RowIterator = query_job.result(
                max_results=page_size, start_index=start_index
            )
            logging.debug(
                "Retrieved result set from query page of size [%d] starting at index [%d]",
                page_size,
                start_index,
            )

            rows_processed = 0
            for row in page_rows:
                process_fn(row)
                rows_processed += 1

            logging.debug(
                "Processed [%d] rows from query page starting at index [%d]",
                rows_processed,
                start_index,
            )
            if rows_processed == 0:
                break

            start_index += rows_processed
            logging.debug("Processed [%d] rows...", start_index)

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
        new_view.view_query = view.view_query.format(
            destination_client.project_id,
            destination_dataset_ref.dataset_id,
            view.view_id,
        )
        table = destination_client.create_table(new_view)
        logging.info("Created %s", new_view_ref)
        return table

    def create_table_from_query_async(
        self,
        dataset_id: str,
        table_id: str,
        query: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        overwrite: Optional[bool] = False,
    ) -> bigquery.QueryJob:
        # If overwrite is False, errors if the table already exists and contains data. Else, overwrites the table if
        # it already exists.
        write_disposition = (
            bigquery.job.WriteDisposition.WRITE_TRUNCATE
            if overwrite
            else bigquery.job.WriteDisposition.WRITE_EMPTY
        )

        logging.info("Creating table: %s with query: %s", table_id, query)

        return self.insert_into_table_from_query(
            destination_dataset_id=dataset_id,
            destination_table_id=table_id,
            query=query,
            query_parameters=query_parameters,
            write_disposition=write_disposition,
        )

    def load_table_from_table_async(
        self,
        source_dataset_id: str,
        source_table_id: str,
        destination_dataset_id: str,
        destination_table_id: str,
        allow_field_additions: bool = False,
    ) -> bigquery.QueryJob:
        return self._load_table_from_table_async(
            source_dataset_id=source_dataset_id,
            source_table_id=source_table_id,
            destination_dataset_id=destination_dataset_id,
            destination_table_id=destination_table_id,
            allow_field_additions=allow_field_additions,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

    def _load_table_from_table_async(
        self,
        source_dataset_id: str,
        source_table_id: str,
        destination_dataset_id: str,
        destination_table_id: str,
        source_data_filter_clause: Optional[str] = None,
        allow_field_additions: bool = False,
        hydrate_missing_columns_with_null: bool = False,
        write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_APPEND,
    ) -> bigquery.QueryJob:
        """Loads data from a source table to a destination table, depending on the write_disposition passed in
        it can either append or overwrite the destination table. Defaults to WRITE_APPEND.
        """
        dataset_ref = self.dataset_ref_for_id(destination_dataset_id)

        if not self.table_exists(dataset_ref, destination_table_id):
            raise ValueError(
                f"Destination table [{self.project_id}.{destination_dataset_id}.{destination_table_id}]"
                f" does not exist!"
            )

        source_table = self.get_table(dataset_ref, source_table_id)
        destination_table = self.get_table(dataset_ref, destination_table_id)

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
                select_columns += ", {null_columns}".format(
                    null_columns=", ".join(missing_columns)
                )

        query = "SELECT {columns} FROM `{project_id}.{source_dataset_id}.{source_table_id}`".format(
            columns=select_columns,
            project_id=self.project_id,
            source_dataset_id=source_dataset_id,
            source_table_id=source_table_id,
        )

        if source_data_filter_clause:
            self.validate_source_data_filter_clause(source_data_filter_clause)
            query = f"{query} {source_data_filter_clause}"

        logging.info(
            "Copying data from: %s.%s to: %s.%s",
            source_dataset_id,
            source_table_id,
            destination_dataset_id,
            destination_table_id,
        )

        return self.insert_into_table_from_query(
            destination_dataset_id=destination_dataset_id,
            destination_table_id=destination_table_id,
            query=query,
            allow_field_additions=allow_field_additions,
            write_disposition=write_disposition,
        )

    @staticmethod
    def validate_source_data_filter_clause(filter_clause: str) -> None:
        if not filter_clause.startswith("WHERE"):
            raise ValueError(
                f"Found filter clause [{filter_clause}] that does not begin with WHERE"
            )

    def insert_into_table_from_query(
        self,
        *,
        destination_dataset_id: str,
        destination_table_id: str,
        query: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        allow_field_additions: bool = False,
        write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_APPEND,
    ) -> bigquery.QueryJob:
        destination_dataset_ref = self.dataset_ref_for_id(destination_dataset_id)

        self.create_dataset_if_necessary(destination_dataset_ref)

        job_config = bigquery.job.QueryJobConfig()
        job_config.destination = destination_dataset_ref.table(destination_table_id)

        job_config.write_disposition = write_disposition
        job_config.query_parameters = query_parameters or []

        if allow_field_additions:
            job_config.schema_update_options = [
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
            job_config=job_config,
        )

    def insert_into_table_from_table_async(
        self,
        source_dataset_id: str,
        source_table_id: str,
        destination_dataset_id: str,
        destination_table_id: str,
        source_data_filter_clause: Optional[str] = None,
        hydrate_missing_columns_with_null: bool = False,
        allow_field_additions: bool = False,
    ) -> bigquery.QueryJob:

        return self._load_table_from_table_async(
            source_dataset_id=source_dataset_id,
            source_table_id=source_table_id,
            destination_dataset_id=destination_dataset_id,
            destination_table_id=destination_table_id,
            source_data_filter_clause=source_data_filter_clause,
            hydrate_missing_columns_with_null=hydrate_missing_columns_with_null,
            allow_field_additions=allow_field_additions,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

    def insert_into_table_from_cloud_storage_async(
        self,
        source_uri: str,
        destination_dataset_ref: bigquery.DatasetReference,
        destination_table_id: str,
        destination_table_schema: List[bigquery.SchemaField],
    ) -> bigquery.job.LoadJob:
        return self._load_table_from_cloud_storage_async(
            source_uri=source_uri,
            destination_dataset_ref=destination_dataset_ref,
            destination_table_id=destination_table_id,
            destination_table_schema=destination_table_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

    def delete_from_table_async(
        self, dataset_id: str, table_id: str, filter_clause: str
    ) -> bigquery.QueryJob:
        if not filter_clause.startswith("WHERE"):
            raise ValueError(
                "Cannot delete from a table without a valid filter clause starting with WHERE."
            )

        delete_query = (
            f"DELETE FROM `{self.project_id}.{dataset_id}.{table_id}` {filter_clause}"
        )

        logging.info(
            "Deleting data from %s.%s matching this filter: %s",
            dataset_id,
            table_id,
            filter_clause,
        )

        return self.client.query(delete_query)

    def materialize_view_to_table(self, view: BigQueryView) -> bigquery.Table:
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
            dst_dataset_id,
            dst_table_id,
            view.direct_select_query,
            overwrite=True,
        )
        create_job.result()
        table_description = (
            f"Materialized data from view [{view.dataset_id}.{view.view_id}]. "
            f"View description:\n{view.description}"
        )
        return self.update_description(
            dst_dataset_id,
            dst_table_id,
            table_description,
        )

    def create_table_with_schema(
        self, dataset_id: str, table_id: str, schema_fields: List[bigquery.SchemaField]
    ) -> bigquery.Table:
        dataset_ref = self.dataset_ref_for_id(dataset_id)

        if self.table_exists(dataset_ref, table_id):
            raise ValueError(
                f"Trying to create a table that already exists: {dataset_id}.{table_id}."
            )

        table_ref = bigquery.TableReference(dataset_ref, table_id)
        table = bigquery.Table(table_ref, schema_fields)

        logging.info("Creating table %s.%s", dataset_id, table_id)
        return self.client.create_table(table)

    def update_description(
        self, dataset_id: str, table_id: str, description: str
    ) -> bigquery.Table:
        table = self.get_table(self.dataset_ref_for_id(dataset_id), table_id)
        if description == table.description:
            return table

        table.description = description
        return self.client.update_table(table, ["description"])

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

    def add_missing_fields_to_schema(
        self,
        dataset_id: str,
        table_id: str,
        desired_schema_fields: List[bigquery.SchemaField],
    ) -> None:
        dataset_ref = self.dataset_ref_for_id(dataset_id)

        if not self.table_exists(dataset_ref, table_id):
            raise ValueError(
                f"Cannot add schema fields to a table that does not exist: {dataset_id}.{table_id}"
            )

        table = self.get_table(dataset_ref, table_id)
        existing_table_schema = table.schema

        missing_fields = self._get_excess_schema_fields(
            table.schema, desired_schema_fields
        )

        updated_table_schema = existing_table_schema.copy()

        for field in desired_schema_fields:
            if field in missing_fields:
                updated_table_schema.append(field)
            else:
                # A field with this name should already be in the existing schema. Assert they are of the same
                # field_type and mode.
                existing_field_with_name = next(
                    existing_field
                    for existing_field in existing_table_schema
                    if existing_field.name == field.name
                )

                if not existing_field_with_name:
                    raise ValueError(
                        "Set comparison of field names is not working. This should be in the"
                        " missing_field_names set."
                    )

                if field.field_type != existing_field_with_name.field_type:
                    raise ValueError(
                        "Trying to change the field type of an existing field. Existing field "
                        f"{existing_field_with_name.name} has type {existing_field_with_name.field_type}. "
                        f"Cannot change this type to {field.field_type}."
                    )

                if field.mode != existing_field_with_name.mode:
                    raise ValueError(
                        f"Cannot change the mode of field {existing_field_with_name} to {field.mode}."
                    )

        if updated_table_schema == existing_table_schema:
            logging.info(
                "Schema for table %s.%s already contains all of the desired fields.",
                dataset_id,
                table_id,
            )
            return

        # Update the table schema with the missing fields
        logging.info(
            "Updating schema of table %s to: %s", table_id, updated_table_schema
        )
        table.schema = updated_table_schema
        self.client.update_table(table, ["schema"])

    def remove_unused_fields_from_schema(
        self,
        dataset_id: str,
        table_id: str,
        desired_schema_fields: List[bigquery.SchemaField],
    ) -> Optional[bigquery.QueryJob]:
        """Compares the schema of the given table to the desired schema fields and drops any unused columns."""
        dataset_ref = self.dataset_ref_for_id(dataset_id)

        if not self.table_exists(dataset_ref, table_id):
            raise ValueError(
                f"Cannot remove schema fields from a table that does not exist: {dataset_id}.{table_id}"
            )

        table = self.get_table(dataset_ref, table_id)

        deprecated_fields = self._get_excess_schema_fields(
            desired_schema_fields, table.schema
        )

        if not deprecated_fields:
            logging.info(
                "Schema for table %s.%s has no excess fields to drop.",
                dataset_id,
                table_id,
            )
            return None

        columns_to_drop = ", ".join([field.name for field in deprecated_fields])

        rebuild_query = f"""
            SELECT * EXCEPT({columns_to_drop})
            FROM `{dataset_id}.{table_id}`
        """

        return self.insert_into_table_from_query(
            destination_table_id=table_id,
            destination_dataset_id=dataset_id,
            query=rebuild_query,
            allow_field_additions=False,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

    def update_schema(
        self,
        dataset_id: str,
        table_id: str,
        desired_schema_fields: List[bigquery.SchemaField],
    ) -> None:
        dataset_ref = self.dataset_ref_for_id(dataset_id)

        if not self.table_exists(dataset_ref, table_id):
            raise ValueError(
                f"Cannot update schema fields for a table that does not exist: {dataset_id}.{table_id}"
            )

        table = self.get_table(dataset_ref, table_id)

        existing_schema = table.schema

        desired_schema_map = {field.name: field for field in desired_schema_fields}

        for field in existing_schema:
            if field.name in desired_schema_map:
                desired_field = desired_schema_map[field.name]
                if field.field_type != desired_field.field_type:
                    raise ValueError(
                        "Trying to change the field type of an existing field. Existing field "
                        f"{desired_field.name} has type {desired_field.field_type}. "
                        f"Cannot change this type to {field.field_type}."
                    )

                if field.mode != desired_field.mode:
                    raise ValueError(
                        f"Cannot change the mode of field {desired_field} to {field.mode}."
                    )

        # Remove any deprecated fields first as it involves copying the entire view
        removal_job = self.remove_unused_fields_from_schema(
            dataset_id, table_id, desired_schema_fields
        )

        if removal_job:
            # Wait for the removal job to complete before running the job to add fields
            removal_job.result()

        self.add_missing_fields_to_schema(dataset_id, table_id, desired_schema_fields)

    def copy_dataset_tables(
        self,
        source_dataset_id: str,
        destination_dataset_id: str,
    ) -> None:
        if self.dataset_exists(self.dataset_ref_for_id(destination_dataset_id)):
            if any(self.list_tables(destination_dataset_id)):
                raise ValueError(
                    f"Destination dataset [{destination_dataset_id}] for copy is not empty."
                )

        logging.info(
            "Copying tables in dataset [%s] to empty dataset [%s]",
            source_dataset_id,
            destination_dataset_id,
        )
        copy_jobs = []
        for table in self.list_tables(source_dataset_id):
            if table.table_type != "TABLE":
                logging.warning(
                    "Skipping copy of item with type [%s]: [%s]",
                    table.table_type,
                    table.table_id,
                )
                continue

            source_table_ref = bigquery.TableReference(
                self.dataset_ref_for_id(source_dataset_id), table.table_id
            )
            destination_table_ref = bigquery.TableReference(
                self.dataset_ref_for_id(destination_dataset_id), table.table_id
            )

            copy_jobs.append(
                self.client.copy_table(source_table_ref, destination_table_ref)
            )
        self.wait_for_big_query_jobs(jobs=copy_jobs)

    def backup_dataset_tables_if_dataset_exists(
        self, dataset_id: str
    ) -> Optional[bigquery.DatasetReference]:
        backup_dataset_id = (
            f"{dataset_id}_backup_{datetime.date.today().isoformat().replace('-', '_')}"
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
        with futures.ThreadPoolExecutor(
            max_workers=BIG_QUERY_CLIENT_MAX_CONNECTIONS
        ) as executor:
            job_futures = [executor.submit(job.result) for job in jobs]
            for f in futures.as_completed(job_futures):
                results.append(f.result())
        return results

    def copy_dataset_tables_across_regions(
        self,
        source_dataset_id: str,
        destination_dataset_id: str,
        timeout_sec: float = DEFAULT_CROSS_REGION_COPY_TIMEOUT_SEC,
    ) -> None:
        transfer_client = DataTransferServiceClient()
        if not self.dataset_exists(self.dataset_ref_for_id(destination_dataset_id)):
            raise ValueError(
                f"Cannot copy data to dataset [{destination_dataset_id}] which does not exist"
            )

        display_name = CROSS_REGION_COPY_DISPLAY_NAME_TEMPLATE.format(
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
            },
            schedule_options=ScheduleOptions(disable_auto_scheduling=True),
        )
        transfer_config = transfer_client.create_transfer_config(
            parent=f"projects/{self.project_id}",
            transfer_config=transfer_config,
        )
        logging.info("Created transfer config [%s]", transfer_config.name)
        try:
            requested_run_time = timestamp_pb2.Timestamp()
            requested_run_time.FromDatetime(datetime.datetime.now(tz=pytz.UTC))
            response = transfer_client.start_manual_transfer_runs(
                StartManualTransferRunsRequest(
                    parent=transfer_config.name, requested_run_time=requested_run_time
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
                run = transfer_client.get_transfer_run(name=run.name)
                if run.state == TransferState.SUCCEEDED:
                    logging.info("Transfer run succeeded")
                    break

                if run.state == TransferState.FAILED:
                    raise ValueError(
                        f"Transfer run [{run.name}] FAILED with status: {run.error_status}"
                    )

                if timeout_time < datetime.datetime.now():
                    raise TimeoutError(
                        f"Did not complete dataset copy before timeout of "
                        f"[{timeout_sec}] seconds expired."
                    )

                logging.info(
                    "Transfer run has status [%s] - sleeping for %s seconds",
                    run.state.name,
                    CROSS_REGION_COPY_STATUS_ATTEMPT_SLEEP_TIME_SEC,
                )
                time.sleep(CROSS_REGION_COPY_STATUS_ATTEMPT_SLEEP_TIME_SEC)
        finally:
            logging.info("Deleting transfer config [%s]", transfer_config.name)
            transfer_client.delete_transfer_config(name=transfer_config.name)
            logging.info("Finished deleting transfer config [%s]", transfer_config.name)
