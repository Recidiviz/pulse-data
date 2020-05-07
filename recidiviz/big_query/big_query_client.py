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
from typing import List, Optional, Iterator

import attr
from google.cloud import bigquery, exceptions

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.utils import metadata

_clients_by_project_id = {}


def client(project_id: str) -> bigquery.Client:
    global _clients_by_project_id
    if project_id not in _clients_by_project_id:
        _clients_by_project_id[project_id] = bigquery.Client(project=project_id)
    return _clients_by_project_id[project_id]


@attr.s(frozen=True)
class ExportViewConfig:
    """Specification for how to export a particular view."""

    # The view to export.
    view: BigQueryView = attr.ib()

    # A WHERE clause to filter what gets exported from the view
    view_filter_clause: str = attr.ib()

    # The name of the intermediate table to create/update (will live in the same dataset as the view).
    intermediate_table_name: str = attr.ib()

    # The desired path of the output file (starts with 'gs://').
    output_uri: str = attr.ib()


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
                              # TODO(3020): BigQueryView now encodes dataset information, remove this parameter.
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
    def export_view_to_table_async(self,
                                   # TODO(3020): BigQueryView now encodes dataset information, remove this parameter.
                                   view_dataset_ref: bigquery.DatasetReference,
                                   view: BigQueryView,
                                   view_filter_clause: str,
                                   output_table_dataset_ref: bigquery.DatasetReference,
                                   output_table_id: str) -> Optional[bigquery.QueryJob]:
        """Queries data in a view filtered by the provided |view_filter_clause| and loads it into a table.

        If the table exists, overwrites existing data. Creates the table if it does not exist.

        It is the caller's responsibility to wait for the resulting job to complete.

        Args:
            view_dataset_ref: The BigQuery dataset where the view is.
            view: The View to query.
            view_filter_clause: A string WHERE clause to filter the results of the view
            output_table_dataset_ref: The BigQuery dataset where the output table should go.
            output_table_id: The string table name

        Returns:
            The QueryJob containing job details, or None if the job fails to start.
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
    def export_table_to_cloud_storage_async(self,
                                            source_table_dataset_ref: bigquery.dataset.DatasetReference,
                                            source_table_id: str,
                                            destination_uri: str) -> Optional[bigquery.ExtractJob]:
        """Exports the table corresponding to the given view to the path in Google Cloud Storage denoted by
        |destination_uri|.

        Extracts the entire table and exports in JSON format to the given bucket in
        Cloud Storage.

        It is the caller's responsibility to wait for the resulting job to complete.

        Args:
            source_table_dataset_ref: The BigQuery dataset where the table exists.
            source_table_id: The string table name to export to cloud storage.
            destination_uri: The path in Google Cloud Storage to write the contents of the table to (starts with
                'gs://').

        Returns:
            The ExtractJob object containing job details, or None if the job fails to start.
        """

    @abc.abstractmethod
    def export_views_to_cloud_storage(self,
                                      dataset_ref: bigquery.dataset.DatasetReference,
                                      export_configs: List[ExportViewConfig]) -> None:
        """Exports the views to cloud storage according to the given configs.

        This is a two-step process. First, for each view, the view query is executed
        and the entire result is loaded into a table in BigQuery. Then, for each
        table, the contents are exported to the cloud storage bucket in JSON Lines format.
        This has to be a two-step process because BigQuery doesn't support exporting
        a view directly, it must be materialized in a table first.

        This runs synchronously and waits for the jobs to complete.

        Args:
            dataset_ref: The dataset where the views exist.
            export_configs: List of views along with how to export them.
        """

    @abc.abstractmethod
    def run_query_async(self, query_str: str) -> bigquery.QueryJob:
        """Runs a query in BigQuery asynchronously.

        It is the caller's responsibility to wait for the resulting job to complete.

        Note: treating the resulting job like an iterator waits implicitly. For example:

            query_job = client.run_query_async(query_str)
            for row in query_job:
                ...

        Args:
            query_str: The query to execute

        Returns:
            A QueryJob which will contain the results once the query is complete.
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
    def create_table_from_query_async(self, dataset_id: str, table_id: str, query: str) -> bigquery.QueryJob:
        """Creates a table at the given location with the output from the given query. If the table already exists and
        contains data, a 'duplicate' error is returned in the job result.

        Args:
            dataset_id: The name of the dataset where the table should be created.
            table_id: The name of the table to be created.
            query: The query to run. The result will be loaded into the new table.

        Returns:
            A QueryJob which will contain the results once the query is complete.
        """

    @abc.abstractmethod
    def insert_into_table_from_table_async(self,
                                           source_dataset_id: str,
                                           source_table_id: str,
                                           destination_dataset_id: str,
                                           destination_table_id: str,
                                           source_data_filter_clause: Optional[str]) -> bigquery.QueryJob:
        """Inserts rows from the source table into the destination table, with an optional filter clause to only insert
        a subset of rows into the destination table.

        Args:
            source_dataset_id: The name of the source dataset.
            source_table_id: The name of the source table from which to query.
            destination_dataset_id: The name of the destination dataset.
            destination_table_id: The name of the table to insert into.
            source_data_filter_clause: An optional clause to filter the contents of the source table that are inserted
                into the destination table. Must start with "WHERE".

        Returns:
            A QueryJob which will contain the results once the query is complete.
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
        return bigquery.DatasetReference.from_string(dataset_id,
                                                     default_project=self._project_id)

    def create_dataset_if_necessary(self, dataset_ref: bigquery.DatasetReference) -> bigquery.Dataset:
        try:
            dataset = self.client.get_dataset(dataset_ref)
        except exceptions.NotFound:
            logging.info("Dataset [%s] does not exist. Creating...", str(dataset_ref))
            dataset = bigquery.Dataset(dataset_ref)
            return self.client.create_dataset(dataset)

        return dataset

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

    def export_view_to_table_async(self,
                                   view_dataset_ref: bigquery.DatasetReference,
                                   view: BigQueryView,
                                   view_filter_clause: str,
                                   output_table_dataset_ref: bigquery.DatasetReference,
                                   output_table_id: str) -> Optional[bigquery.QueryJob]:
        if not self.table_exists(view_dataset_ref, view.view_id):
            logging.error("View [%s] does not exist in dataset [%s]", view.view_id, str(view_dataset_ref))
            return None

        job_config = bigquery.QueryJobConfig()
        job_config.destination = output_table_dataset_ref.table(output_table_id)
        job_config.write_disposition = \
            bigquery.job.WriteDisposition.WRITE_TRUNCATE
        query = "{select_query} {filter_clause}".format(select_query=view.select_query,
                                                        filter_clause=view_filter_clause)

        logging.info("Querying table: %s with query: %s", view.view_id, query)

        return self.client.query(
            query=query,
            location=self.LOCATION,
            job_config=job_config,
        )

    def load_table_from_cloud_storage_async(
            self,
            source_uri: str,
            destination_dataset_ref: bigquery.DatasetReference,
            destination_table_id: str,
            destination_table_schema: List[bigquery.SchemaField]) -> bigquery.job.LoadJob:

        self.create_dataset_if_necessary(destination_dataset_ref)

        destination_table_ref = destination_dataset_ref.table(destination_table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.schema = destination_table_schema
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        load_job = self.client.load_table_from_uri(
            source_uri,
            destination_table_ref,
            job_config=job_config
        )

        logging.info("Started load job %s for table %s.%s.%s",
                     load_job.job_id,
                     destination_table_ref.project, destination_table_ref.dataset_id, destination_table_ref.table_id)

        return load_job

    def export_table_to_cloud_storage_async(self,
                                            source_table_dataset_ref: bigquery.DatasetReference,
                                            source_table_id: str,
                                            destination_uri: str) -> Optional[bigquery.ExtractJob]:
        if not self.table_exists(source_table_dataset_ref, source_table_id):
            logging.error("Table [%s] does not exist in dataset [%s]", source_table_id, str(source_table_dataset_ref))
            return None

        table_ref = source_table_dataset_ref.table(source_table_id)

        job_config = bigquery.ExtractJobConfig()
        job_config.destination_format = bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON

        return self.client.extract_table(
            table_ref,
            destination_uri,
            # Location must match that of the source table.
            location=self.LOCATION,
            job_config=job_config
        )

    def export_views_to_cloud_storage(self,
                                      dataset_ref: bigquery.dataset.DatasetReference,
                                      export_configs: List[ExportViewConfig]) -> None:
        query_jobs = []
        for export_config in export_configs:
            query_job = self.export_view_to_table_async(
                dataset_ref,
                export_config.view,
                export_config.view_filter_clause,
                dataset_ref,
                export_config.intermediate_table_name)
            if query_job is not None:
                query_jobs.append(query_job)

        logging.info('Waiting on [%d] query jobs to finish', len(query_jobs))
        for job in query_jobs:
            job.result()

        logging.info('Completed [%d] query jobs.', len(query_jobs))

        extract_jobs = []
        for export_config in export_configs:
            extract_job = self.export_table_to_cloud_storage_async(
                dataset_ref,
                export_config.intermediate_table_name,
                export_config.output_uri)
            if extract_job is not None:
                extract_jobs.append(extract_job)

        logging.info('Waiting on [%d] extract jobs to finish', len(extract_jobs))
        for job in extract_jobs:
            job.result()
        logging.info('Completed [%d] extract jobs.', len(extract_jobs))

    def run_query_async(self, query_str: str) -> bigquery.QueryJob:
        return self.client.query(query_str)

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

    def create_table_from_query_async(self, dataset_id: str, table_id: str, query: str) -> bigquery.QueryJob:
        dataset_ref = self.dataset_ref_for_id(dataset_id)

        self.create_dataset_if_necessary(dataset_ref)

        job_config = bigquery.QueryJobConfig()
        job_config.destination = dataset_ref.table(table_id)
        # Errors if the table already exists and contains data
        job_config.write_disposition = bigquery.job.WriteDisposition.WRITE_EMPTY

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
                                           source_data_filter_clause: Optional[str] = None) -> bigquery.QueryJob:

        destination_dataset_ref = self.dataset_ref_for_id(destination_dataset_id)

        if not self.table_exists(destination_dataset_ref, destination_table_id):
            raise ValueError(f"Destination table [{self.project_id}.{destination_dataset_id}.{destination_table_id}]"
                             f" does not exist!")

        insert_query = \
            """INSERT INTO `{project_id}.{destination_dataset_id}.{destination_table_id}`
            SELECT * FROM `{project_id}.{source_dataset_id}.{source_table_id}`""".format(
                project_id=self.project_id,
                destination_dataset_id=destination_dataset_id,
                destination_table_id=destination_table_id,
                source_dataset_id=source_dataset_id,
                source_table_id=source_table_id
            )

        if source_data_filter_clause:
            if not source_data_filter_clause.startswith('WHERE'):
                raise ValueError("Cannot filter a SELECT without a valid filter clause starting with WHERE.")

            insert_query = f"{insert_query} {source_data_filter_clause}"

        logging.info("Copying data from: %s.%s to: %s.%s", source_dataset_id, source_table_id,
                     destination_dataset_id, destination_table_id)

        return self.client.query(insert_query)

    def delete_from_table_async(self, dataset_id: str, table_id: str, filter_clause: str) -> bigquery.QueryJob:
        if not filter_clause.startswith('WHERE'):
            raise ValueError("Cannot delete from a table without a valid filter clause starting with WHERE.")

        delete_query = f"DELETE FROM `{self.project_id}.{dataset_id}.{table_id}` {filter_clause}"

        logging.info("Deleting data from %s.%s matching this filter: %s", dataset_id, table_id, filter_clause)

        return self.client.query(delete_query)
