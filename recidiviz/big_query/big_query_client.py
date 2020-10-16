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
import logging
from typing import List, Optional, Iterator, Dict, Callable

from google.cloud import bigquery, exceptions

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.export.export_query_config import ExportQueryConfig
from recidiviz.utils import metadata

_clients_by_project_id = {}


def client(project_id: str) -> bigquery.Client:
    global _clients_by_project_id
    if project_id not in _clients_by_project_id:
        _clients_by_project_id[project_id] = bigquery.Client(project=project_id)
    return _clients_by_project_id[project_id]


class BigQueryClient:
    """Interface for a wrapper around the bigquery.Client with convenience functions for querying, creating, copying and
     exporting BigqQuery tables and views.
    """

    @property
    @abc.abstractmethod
    def project_id(self) -> str:
        """The Google Cloud project id for this client."""

    @abc.abstractmethod
    def dataset_ref_for_id(self, dataset_id: str) -> bigquery.DatasetReference:
        """Returns a BigQuery DatasetReference for the dataset with the given dataset name."""

    @abc.abstractmethod
    def create_dataset_if_necessary(self,
                                    dataset_ref: bigquery.DatasetReference) -> None:
        """Create a BigQuery dataset if it does not exist."""

    @abc.abstractmethod
    def dataset_exists(self, dataset_ref: bigquery.DatasetReference) -> bool:
        """Check whether or not a BigQuery Dataset exists.
        Args:
            dataset_ref: The BigQuery dataset to look for

        Returns:
            True if the dataset exists, False otherwise.
        """

    @abc.abstractmethod
    def table_exists(
            self,
            dataset_ref: bigquery.DatasetReference,
            table_id: str) -> bool:
        """Check whether or not a BigQuery Table or View exists in a Dataset.

        Args:
            dataset_ref: The BigQuery dataset to search
            table_id: The string table name to look for

        Returns:
            True if the table or view exists, False otherwise.
        """

    @abc.abstractmethod
    def get_table(
            self,
            dataset_ref: bigquery.DatasetReference,
            table_id: str) -> bigquery.Table:
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
    def create_or_update_view(self,
                              # TODO(#3020): BigQueryView now encodes dataset information, remove this parameter.
                              dataset_ref: bigquery.DatasetReference,
                              view: BigQueryView) -> bigquery.Table:
        """Create a View if it does not exist, or update its query if it does.

        This runs synchronously and waits for the job to complete.

        Args:
            dataset_ref: The BigQuery dataset to store the view in.
            view: The View to create or update.

        Returns:
            The Table that was just created.
        """

    @abc.abstractmethod
    def load_table_from_cloud_storage_async(
            self,
            source_uri: str,
            destination_dataset_ref: bigquery.DatasetReference,
            destination_table_id: str,
            destination_table_schema: List[bigquery.SchemaField]) -> bigquery.job.LoadJob:
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
        Returns:
            The LoadJob object containing job details.
        """

    @abc.abstractmethod
    def export_table_to_cloud_storage_async(
            self,
            source_table_dataset_ref: bigquery.dataset.DatasetReference,
            source_table_id: str,
            destination_uri: str,
            destination_format: bigquery.DestinationFormat) -> Optional[bigquery.ExtractJob]:
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

        Returns:
            The ExtractJob object containing job details, or None if the job fails to start.
        """

    @abc.abstractmethod
    def export_query_results_to_cloud_storage(self, export_configs: List[ExportQueryConfig]) -> None:
        """Exports the queries to cloud storage according to the given configs.

        This is a three-step process. First, each query is executed and the entire result is loaded into a temporary
        table in BigQuery. Then, for each table, the contents are exported to the cloud storage bucket in the format
        specified in the config. Finally, once all exports are complete, the temporary tables are deleted.

        The query output must be materialized in a table first because BigQuery doesn't support exporting a view or
        query directly.

        This runs synchronously and waits for the jobs to complete.

        Args:
            export_configs: List of queries along with how to export their results.
        """

    @abc.abstractmethod
    def run_query_async(self, query_str: str, query_parameters: List[bigquery.ScalarQueryParameter] = None) \
            -> bigquery.QueryJob:
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
    def paged_read_and_process(self,
                               query_job: bigquery.QueryJob,
                               page_size: int,
                               process_fn: Callable[[bigquery.table.Row], None]) -> None:
        """Reads the given result set from the given query job in pages to limit how many rows are read into memory at
        any given time, processing the results of each row with the given callable.

        Args:
            query_job: the query job from which to process results.
            page_size: the maximum number of rows to read in at a time.
            process_fn: a callable function which takes in one row from the result set and performs some operation.
        """

    @abc.abstractmethod
    def copy_view(self,
                  view: BigQueryView,
                  destination_client: 'BigQueryClient',
                  destination_dataset_ref: bigquery.DatasetReference) -> bigquery.Table:
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
    def create_table_from_query_async(self,
                                      dataset_id: str,
                                      table_id: str,
                                      query: str,
                                      query_parameters: List[bigquery.ScalarQueryParameter],
                                      overwrite: Optional[bool] = False) -> bigquery.QueryJob:
        """Creates a table at the given location with the output from the given query. If overwrite is False, a
        'duplicate' error is returned in the job result if the table already exists and contains data. If overwrite is
        True, overwrites the table if it already exists.

        Args:
            dataset_id: The name of the dataset where the table should be created.
            table_id: The name of the table to be created.
            query: The query to run. The result will be loaded into the new table.
            query_parameters: Parameters for the query
            overwrite: Whether or not to overwrite an existing table.

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def insert_into_table_from_table_async(self,
                                           source_dataset_id: str,
                                           source_table_id: str,
                                           destination_dataset_id: str,
                                           destination_table_id: str,
                                           source_data_filter_clause: Optional[str] = None,
                                           allow_field_additions: bool = False) -> bigquery.QueryJob:
        """Inserts rows from the source table into the destination table, with an optional filter clause to only insert
        a subset of rows into the destination table.

        Args:
            source_dataset_id: The name of the source dataset.
            source_table_id: The name of the source table from which to query.
            destination_dataset_id: The name of the destination dataset.
            destination_table_id: The name of the table to insert into.
            source_data_filter_clause: An optional clause to filter the contents of the source table that are inserted
                into the destination table. Must start with "WHERE".
            allow_field_additions: Whether or not to allow new columns to be created in the destination table if the
                schema in the source table does not exactly match the destination table. Defaults to False.

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def insert_into_table_from_cloud_storage_async(
            self,
            source_uri: str,
            destination_dataset_ref: bigquery.DatasetReference,
            destination_table_id: str,
            destination_table_schema: List[bigquery.SchemaField]) -> bigquery.job.LoadJob:
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
    def delete_from_table_async(self, dataset_id: str, table_id: str, filter_clause: str) -> bigquery.QueryJob:
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
    def materialize_view_to_table(self, view: BigQueryView) -> None:
        """Materializes the result of a view's view_query into a table. The view's materialized_view_table_id must be
        set. The resulting table is put in the same project and dataset as the view, and it overwrites any previous
        materialization of the view.

        Args:
            view: The BigQueryView to materialize into a table.
        """

    @abc.abstractmethod
    def create_table_with_schema(self, dataset_id: str, table_id: str, schema_fields: List[bigquery.SchemaField]) -> \
            bigquery.Table:
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
    def add_missing_fields_to_schema(self, dataset_id: str, table_id: str, schema_fields: List[bigquery.SchemaField]) \
            -> None:
        """Updates the schema of the table to include the schema_fields if they are not already present in the
        Table's schema. Does not update the type or mode of any existing schema fields, and does not delete existing
        schema fields.

        Args:
            dataset_id: The name of the dataset where the table lives.
            table_id: The name of the table to add fields to.
            schema_fields: A list of fields to add to the table
        """

    @abc.abstractmethod
    def delete_table(self, dataset_id: str, table_id: str) -> None:
        """Provied the |dataset_id| and |table_id|, attempts to delete the given table from BigQuery.

        Args:
            dataset_id: The name of the dataset where the table lives.
            table_id: The name of the table to delete.
        """


class BigQueryClientImpl(BigQueryClient):
    """Wrapper around the bigquery.Client with convenience functions for querying, creating, copying and exporting
    BigQuery tables and views.
    """

    # Location of the GCP project that must be the same for bigquery.Client calls
    LOCATION = 'US'

    def __init__(self, project_id: Optional[str] = None):
        if not project_id:
            project_id = metadata.project_id()

        if not project_id:
            raise ValueError('Must provide a project_id if metadata.project_id() returns None')

        self._project_id = project_id
        self.client = client(self._project_id)

    @property
    def project_id(self) -> str:
        return self._project_id

    def dataset_ref_for_id(self, dataset_id: str) -> bigquery.DatasetReference:
        return bigquery.DatasetReference.from_string(dataset_id, default_project=self._project_id)

    def create_dataset_if_necessary(self, dataset_ref: bigquery.DatasetReference) -> bigquery.Dataset:
        try:
            dataset = self.client.get_dataset(dataset_ref)
        except exceptions.NotFound:
            logging.info("Dataset [%s] does not exist. Creating...", str(dataset_ref))
            dataset = bigquery.Dataset(dataset_ref)
            return self.client.create_dataset(dataset)

        return dataset

    def dataset_exists(self, dataset_ref: bigquery.DatasetReference) -> bool:
        try:
            self.client.get_dataset(dataset_ref)
            return True
        except exceptions.NotFound:
            logging.warning("Dataset [%s] does not exist", str(dataset_ref))
            return False

    def table_exists(self, dataset_ref: bigquery.DatasetReference, table_id: str) -> bool:
        table_ref = dataset_ref.table(table_id)

        try:
            self.client.get_table(table_ref)
            return True
        except exceptions.NotFound:
            logging.warning("Table [%s] does not exist in dataset [%s]", table_id, str(dataset_ref))
            return False

    def list_tables(self, dataset_id: str) -> Iterator[bigquery.table.TableListItem]:
        return self.client.list_tables(dataset_id)

    def get_table(
            self,
            dataset_ref: bigquery.DatasetReference,
            table_id: str) -> bigquery.Table:
        table_ref = dataset_ref.table(table_id)
        return self.client.get_table(table_ref)

    def create_table(self, table: bigquery.Table) -> bigquery.Table:
        return self.client.create_table(table, exists_ok=False)

    def create_or_update_view(self, dataset_ref: bigquery.DatasetReference, view: BigQueryView) -> bigquery.Table:
        bq_view = bigquery.Table(view)
        bq_view.view_query = view.view_query

        if self.table_exists(dataset_ref, view.view_id):
            logging.info("Updating existing view [%s]", str(bq_view))
            return self.client.update_table(bq_view, ['view_query'])

        logging.info("Creating view %s", str(bq_view))
        return self.client.create_table(bq_view)

    def load_table_from_cloud_storage_async(
            self,
            source_uri: str,
            destination_dataset_ref: bigquery.DatasetReference,
            destination_table_id: str,
            destination_table_schema: List[bigquery.SchemaField]) -> bigquery.job.LoadJob:

        return self._load_table_from_cloud_storage_async(source_uri=source_uri,
                                                         destination_dataset_ref=destination_dataset_ref,
                                                         destination_table_id=destination_table_id,
                                                         destination_table_schema=destination_table_schema,
                                                         write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    def _load_table_from_cloud_storage_async(
            self,
            source_uri: str,
            destination_dataset_ref: bigquery.DatasetReference,
            destination_table_id: str,
            destination_table_schema: List[bigquery.SchemaField],
            write_disposition: bigquery.WriteDisposition) -> bigquery.job.LoadJob:

        self.create_dataset_if_necessary(destination_dataset_ref)

        destination_table_ref = destination_dataset_ref.table(destination_table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.schema = destination_table_schema
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.allow_quoted_newlines = True
        job_config.write_disposition = write_disposition

        load_job = self.client.load_table_from_uri(
            source_uri,
            destination_table_ref,
            job_config=job_config
        )

        logging.info("Started load job [%s] for table [%s.%s.%s]",
                     load_job.job_id,
                     destination_table_ref.project, destination_table_ref.dataset_id, destination_table_ref.table_id)

        return load_job

    def export_table_to_cloud_storage_async(
            self,
            source_table_dataset_ref: bigquery.DatasetReference,
            source_table_id: str,
            destination_uri: str,
            destination_format: bigquery.DestinationFormat) -> Optional[bigquery.ExtractJob]:
        if not self.table_exists(source_table_dataset_ref, source_table_id):
            logging.error("Table [%s] does not exist in dataset [%s]", source_table_id, str(source_table_dataset_ref))
            return None

        table_ref = source_table_dataset_ref.table(source_table_id)

        job_config = bigquery.ExtractJobConfig()
        job_config.destination_format = destination_format

        return self.client.extract_table(
            table_ref,
            destination_uri,
            # Location must match that of the source table.
            location=self.LOCATION,
            job_config=job_config
        )

    def export_query_results_to_cloud_storage(self,
                                              export_configs: List[ExportQueryConfig]) -> None:
        query_jobs = []
        for export_config in export_configs:
            query_job = self.create_table_from_query_async(
                dataset_id=export_config.intermediate_dataset_id,
                table_id=export_config.intermediate_table_name,
                query=export_config.query,
                query_parameters=export_config.query_parameters,
                overwrite=True
            )
            if query_job is not None:
                query_jobs.append(query_job)

        logging.info('Waiting on [%d] query jobs to finish', len(query_jobs))
        for job in query_jobs:
            job.result()

        logging.info('Completed [%d] query jobs.', len(query_jobs))

        extract_jobs = []
        for export_config in export_configs:
            extract_job = self.export_table_to_cloud_storage_async(
                self.dataset_ref_for_id(export_config.intermediate_dataset_id),
                export_config.intermediate_table_name,
                export_config.output_uri,
                export_config.output_format
            )
            if extract_job is not None:
                extract_jobs.append(extract_job)

        logging.info('Waiting on [%d] extract jobs to finish', len(extract_jobs))
        for job in extract_jobs:
            job.result()
        logging.info('Completed [%d] extract jobs.', len(extract_jobs))

        logging.info('Deleting [%d] temporary intermediate tables.', len(export_configs))
        for export_config in export_configs:
            self.delete_table(dataset_id=export_config.intermediate_dataset_id,
                              table_id=export_config.intermediate_table_name)
        logging.info('Done deleting temporary intermediate tables.')

    def delete_table(self, dataset_id: str, table_id: str) -> None:
        dataset_ref = self.dataset_ref_for_id(dataset_id)
        table_ref = dataset_ref.table(table_id)
        logging.info('Deleting temporary table [%s] from dataset [%s].', table_id, dataset_id)
        self.client.delete_table(table_ref)

    def run_query_async(self, query_str: str, query_parameters: List[bigquery.ScalarQueryParameter] = None) \
            -> bigquery.QueryJob:
        job_config = bigquery.QueryJobConfig()
        job_config.query_parameters = query_parameters or []

        return self.client.query(
            query=query_str,
            location=self.LOCATION,
            job_config=job_config,
        )

    def paged_read_and_process(self,
                               query_job: bigquery.QueryJob,
                               page_size: int,
                               process_fn: Callable[[bigquery.table.Row], None]) -> None:
        logging.info("Querying for first page of results to perform %s...", process_fn.__name__)

        start_index = 0

        while True:
            page_rows: bigquery.table.RowIterator = query_job.result(max_results=page_size, start_index=start_index)
            logging.debug("Retrieved result set from query page of size [%d] starting at index [%d]",
                          page_size, start_index)

            rows_processed = 0
            for row in page_rows:
                process_fn(row)
                rows_processed += 1

            logging.debug("Processed [%d] rows from query page starting at index [%d]", rows_processed, start_index)
            if rows_processed == 0:
                break

            start_index += rows_processed

    def copy_view(self,
                  view: BigQueryView,
                  destination_client: BigQueryClient,
                  destination_dataset_ref: bigquery.DatasetReference) -> bigquery.Table:

        if destination_client.table_exists(destination_dataset_ref, view.view_id):
            raise ValueError(f"Table [{view.view_id}] already exists in dataset!")

        # Create the destination dataset if it doesn't yet exist
        destination_client.create_dataset_if_necessary(destination_dataset_ref)

        new_view_ref = destination_dataset_ref.table(view.view_id)
        new_view = bigquery.Table(new_view_ref)
        new_view.view_query = view.view_query.format(destination_client.project_id,
                                                     destination_dataset_ref.dataset_id,
                                                     view.view_id)
        table = destination_client.create_table(new_view)
        logging.info("Created %s", new_view_ref)
        return table

    def create_table_from_query_async(self,
                                      dataset_id: str,
                                      table_id: str,
                                      query: str,
                                      query_parameters: List[bigquery.ScalarQueryParameter],
                                      overwrite: Optional[bool] = False) -> bigquery.QueryJob:
        dataset_ref = self.dataset_ref_for_id(dataset_id)

        self.create_dataset_if_necessary(dataset_ref)

        job_config = bigquery.QueryJobConfig()
        job_config.destination = dataset_ref.table(table_id)

        # If overwrite is False, errors if the table already exists and contains data. Else, overwrites the table if
        # it already exists.
        job_config.write_disposition = (bigquery.job.WriteDisposition.WRITE_TRUNCATE if overwrite
                                        else bigquery.job.WriteDisposition.WRITE_EMPTY)
        job_config.query_parameters = query_parameters

        logging.info("Creating table: %s with query: %s", table_id, query)

        return self.client.query(
            query=query,
            location=self.LOCATION,
            job_config=job_config,
        )

    def insert_into_table_from_table_async(self,
                                           source_dataset_id: str,
                                           source_table_id: str,
                                           destination_dataset_id: str,
                                           destination_table_id: str,
                                           source_data_filter_clause: Optional[str] = None,
                                           allow_field_additions: bool = False) -> bigquery.QueryJob:

        destination_dataset_ref = self.dataset_ref_for_id(destination_dataset_id)

        if not self.table_exists(destination_dataset_ref, destination_table_id):
            raise ValueError(f"Destination table [{self.project_id}.{destination_dataset_id}.{destination_table_id}]"
                             f" does not exist!")

        select_query = \
            """SELECT * FROM `{project_id}.{source_dataset_id}.{source_table_id}`""".format(
                project_id=self.project_id,
                source_dataset_id=source_dataset_id,
                source_table_id=source_table_id
            )

        if source_data_filter_clause:
            if not source_data_filter_clause.startswith('WHERE'):
                raise ValueError("Cannot filter a SELECT without a valid filter clause starting with WHERE.")

            select_query = f"{select_query} {source_data_filter_clause}"

        job_config = bigquery.job.QueryJobConfig()
        job_config.destination = destination_dataset_ref.table(destination_table_id)
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        if allow_field_additions:
            job_config.schema_update_options = [bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]

        logging.info("Copying data from: %s.%s to: %s.%s", source_dataset_id, source_table_id,
                     destination_dataset_id, destination_table_id)

        return self.client.query(select_query, job_config=job_config)

    def insert_into_table_from_cloud_storage_async(
            self,
            source_uri: str,
            destination_dataset_ref: bigquery.DatasetReference,
            destination_table_id: str,
            destination_table_schema: List[bigquery.SchemaField]) -> bigquery.job.LoadJob:
        return self._load_table_from_cloud_storage_async(source_uri=source_uri,
                                                         destination_dataset_ref=destination_dataset_ref,
                                                         destination_table_id=destination_table_id,
                                                         destination_table_schema=destination_table_schema,
                                                         write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

    def delete_from_table_async(self, dataset_id: str, table_id: str, filter_clause: str) -> bigquery.QueryJob:
        if not filter_clause.startswith('WHERE'):
            raise ValueError("Cannot delete from a table without a valid filter clause starting with WHERE.")

        delete_query = f"DELETE FROM `{self.project_id}.{dataset_id}.{table_id}` {filter_clause}"

        logging.info("Deleting data from %s.%s matching this filter: %s", dataset_id, table_id, filter_clause)

        return self.client.query(delete_query)

    def materialize_view_to_table(self, view: BigQueryView) -> None:
        if view.materialized_view_table_id is None:
            raise ValueError("Trying to materialize a view that does not have a set materialized_view_table_id.")

        logging.info("Materializing %s into a table with the table_id: %s",
                     view.view_id, view.materialized_view_table_id)

        create_job = self.create_table_from_query_async(
            view.dataset_id, view.materialized_view_table_id, view.select_query, query_parameters=[], overwrite=True)
        create_job.result()

    def create_table_with_schema(self, dataset_id: str, table_id: str, schema_fields: List[bigquery.SchemaField]) -> \
            bigquery.Table:
        dataset_ref = self.dataset_ref_for_id(dataset_id)

        if self.table_exists(dataset_ref, table_id):
            raise ValueError(f"Trying to create a table that already exists: {dataset_id}.{table_id}.")

        table_ref = bigquery.TableReference(dataset_ref, table_id)
        table = bigquery.Table(table_ref, schema_fields)

        logging.info("Creating table %s.%s", dataset_id, table_id)
        return self.client.create_table(table)

    def add_missing_fields_to_schema(self, dataset_id: str, table_id: str, schema_fields: List[bigquery.SchemaField]) \
            -> None:
        dataset_ref = self.dataset_ref_for_id(dataset_id)

        if not self.table_exists(dataset_ref, table_id):
            raise ValueError(f"Cannot add schema fields to a table that does not exist: {dataset_id}.{table_id}")

        table = self.get_table(dataset_ref, table_id)
        existing_table_schema = table.schema

        existing_table_schema_fields_by_name: Dict[str, bigquery.SchemaField] = {
            field.name: field for field in existing_table_schema
        }

        schema_fields_by_name: Dict[str, bigquery.SchemaField] = {
            field.name: field for field in schema_fields
        }

        missing_field_names = \
            set(schema_fields_by_name.keys()).difference(set(existing_table_schema_fields_by_name.keys()))

        updated_table_schema = existing_table_schema.copy()

        for field in schema_fields:
            if field.name in missing_field_names:
                updated_table_schema.append(field)
            else:
                # A field with this name should already be in the existing schema. Assert they are of the same
                # field_type and mode.
                existing_field_with_name = existing_table_schema_fields_by_name.get(field.name)

                if not existing_field_with_name:
                    raise ValueError("Set comparison of field names is not working. This should be in the"
                                     " missing_field_names set.")

                if field.field_type != existing_field_with_name.field_type:
                    raise ValueError("Trying to change the field type of an existing field. Existing field "
                                     f"{existing_field_with_name.name} has type {existing_field_with_name.field_type}. "
                                     f"Cannot change this type to {field.field_type}.")

                if field.mode != existing_field_with_name.mode:
                    raise ValueError(f"Cannot change the mode of field {existing_field_with_name} to {field.mode}.")

        if updated_table_schema == existing_table_schema:
            logging.info("Schema for table %s.%s already contains all of the desired fields.",
                         dataset_id, table_id)
            return

        # Update the table schema with the missing fields
        logging.info("Updating schema of table %s to: %s", table_id, updated_table_schema)
        table.schema = updated_table_schema
        self.client.update_table(table, ['schema'])
