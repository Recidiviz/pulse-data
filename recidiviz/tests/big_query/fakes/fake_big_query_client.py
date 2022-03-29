# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""A fake implementation of BigQueryClient for use in tests.
TODO(#9717): Implement functionality in this class more fully by using a
  FakeBigQueryDatabase that talks to Postgres.
"""
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence

import pandas as pd
import sqlalchemy
from google.api_core.future.polling import PollingFuture
from google.cloud import bigquery
from pandas import DataFrame

from recidiviz.big_query.big_query_client import (
    DEFAULT_CROSS_REGION_COPY_TIMEOUT_SEC,
    BigQueryClient,
)
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.export.export_query_config import ExportQueryConfig
from recidiviz.tests.big_query.fakes.fake_big_query_database import FakeBigQueryDatabase


class _ResultsIter:
    def __init__(self, results_df: DataFrame) -> None:
        field_to_index = {col_name: i for i, col_name in enumerate(results_df.columns)}

        self.results: List[bigquery.table.Row] = [
            bigquery.table.Row(row_values, field_to_index)
            for row_values in results_df.values
        ]

    def __iter__(self) -> Iterator[bigquery.table.Row]:
        return self

    def __next__(self) -> bigquery.table.Row:
        if not self.results:
            raise StopIteration
        return self.results.pop(0)


class FakeQueryJob:
    """A fake implementation of bigquery.QueryJob for use in tests."""

    def __init__(self, run_query_fn: Callable[[], DataFrame]) -> None:
        self.run_query_fn = run_query_fn

    def __iter__(self) -> Iterator[bigquery.table.Row]:
        return self.result()

    def result(
        self,
        page_size: int = None,
        max_results: int = None,
        retry: int = None,
        timeout: int = None,
    ) -> Iterator[bigquery.table.Row]:
        if page_size is not None:
            raise NotImplementedError(
                f"No test support for paging fake results. Found page_size "
                f"[{page_size}]."
            )
        if max_results is not None:
            raise NotImplementedError(
                f"No test support for limiting result size. Found max_results "
                f"[{max_results}]."
            )
        if retry is not None:
            raise NotImplementedError(
                f"No test support for retrying queries. Found retry [{retry}]."
            )
        if timeout is not None:
            raise NotImplementedError(
                f"No test support for timeouts in test queries. Found timeout "
                f"[{timeout}]."
            )
        return _ResultsIter(self.run_query_fn())


class FakeBigQueryClient(BigQueryClient):
    """A fake implementation of BigQueryClient for use in tests.
    TODO(#9717): Implement functionality in this class more fully by using a
      FakeBigQueryDatabase that talks to Postgres.
    """

    def __init__(
        self,
        project_id: str,
        database: FakeBigQueryDatabase,
    ):
        self._project_id = project_id
        self.database = database

    @property
    def project_id(self) -> str:
        return self._project_id

    def dataset_ref_for_id(self, dataset_id: str) -> bigquery.DatasetReference:
        return bigquery.DatasetReference(self._project_id, dataset_id)

    def create_dataset_if_necessary(
        self,
        dataset_ref: bigquery.DatasetReference,
        default_table_expiration_ms: Optional[int] = None,
    ) -> None:
        pass

    def dataset_exists(self, dataset_ref: bigquery.DatasetReference) -> bool:
        raise ValueError("Must be implemented for use in tests.")

    def delete_dataset(
        self,
        dataset_ref: bigquery.DatasetReference,
        delete_contents: bool = False,
        not_found_ok: bool = False,
    ) -> None:
        raise ValueError("Must be implemented for use in tests.")

    def get_dataset(self, dataset_ref: bigquery.DatasetReference) -> bigquery.Dataset:
        raise ValueError("Must be implemented for use in tests.")

    def list_datasets(self) -> Iterator[bigquery.dataset.DatasetListItem]:
        raise ValueError("Must be implemented for use in tests.")

    def table_exists(
        self, dataset_ref: bigquery.DatasetReference, table_id: str
    ) -> bool:
        raise ValueError("Must be implemented for use in tests.")

    def get_table(
        self, dataset_ref: bigquery.DatasetReference, table_id: str
    ) -> bigquery.Table:
        raise ValueError("Must be implemented for use in tests.")

    def get_table_if_exists(
        self, dataset_ref: bigquery.DatasetReference, table_id: str
    ) -> Optional[bigquery.Table]:
        raise ValueError("Must be implemented for use in tests.")

    def list_tables(self, dataset_id: str) -> Iterator[bigquery.table.TableListItem]:
        raise ValueError("Must be implemented for use in tests.")

    def list_tables_excluding_views(
        self, dataset_id: str
    ) -> Iterator[bigquery.table.TableListItem]:
        raise ValueError("Must be implemented for use in tests.")

    def create_table(self, table: bigquery.Table) -> bigquery.Table:
        raise ValueError("Must be implemented for use in tests.")

    def create_or_update_view(self, view: BigQueryView) -> bigquery.Table:
        raise ValueError("Must be implemented for use in tests.")

    def load_table_from_cloud_storage_async(
        self,
        source_uris: List[str],
        destination_dataset_ref: bigquery.DatasetReference,
        destination_table_id: str,
        destination_table_schema: List[bigquery.SchemaField],
        write_disposition: bigquery.WriteDisposition,
        skip_leading_rows: int = 0,
    ) -> bigquery.job.LoadJob:
        raise ValueError("Must be implemented for use in tests.")

    def load_into_table_from_dataframe_async(
        self,
        source: pd.DataFrame,
        destination_dataset_ref: bigquery.DatasetReference,
        destination_table_id: str,
    ) -> bigquery.job.LoadJob:
        raise ValueError("Must be implemented for use in tests.")

    def export_table_to_cloud_storage_async(
        self,
        source_table_dataset_ref: bigquery.dataset.DatasetReference,
        source_table_id: str,
        destination_uri: str,
        destination_format: bigquery.DestinationFormat,
        print_header: bool,
    ) -> Optional[bigquery.ExtractJob]:
        raise ValueError("Must be implemented for use in tests.")

    def export_query_results_to_cloud_storage(
        self, export_configs: List[ExportQueryConfig], print_header: bool
    ) -> None:
        raise ValueError("Must be implemented for use in tests.")

    def run_query_async(
        self,
        query_str: str,
        query_parameters: List[bigquery.ScalarQueryParameter] = None,
    ) -> bigquery.QueryJob:
        def run_query_fn() -> DataFrame:
            return self.database.run_query(
                query_str=query_str, data_types=None, dimensions=None
            )

        return FakeQueryJob(run_query_fn=run_query_fn)

    def paged_read_and_process(
        self,
        query_job: bigquery.QueryJob,
        page_size: int,
        process_page_fn: Callable[[List[bigquery.table.Row]], None],
    ) -> None:
        raise ValueError("Must be implemented for use in tests.")

    def copy_view(
        self,
        view: BigQueryView,
        destination_client: "BigQueryClient",
        destination_dataset_ref: bigquery.DatasetReference,
    ) -> bigquery.Table:
        raise ValueError("Must be implemented for use in tests.")

    def create_table_from_query_async(
        self,
        dataset_id: str,
        table_id: str,
        query: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        overwrite: Optional[bool] = False,
        clustering_fields: Optional[List[str]] = None,
    ) -> bigquery.QueryJob:
        raise ValueError("Must be implemented for use in tests.")

    def insert_into_table_from_table_async(
        self,
        source_dataset_id: str,
        source_table_id: str,
        destination_dataset_id: str,
        destination_table_id: str,
        source_data_filter_clause: Optional[str] = None,
        hydrate_missing_columns_with_null: bool = False,
        allow_field_additions: bool = False,
        write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_APPEND,
    ) -> bigquery.QueryJob:
        raise ValueError("Must be implemented for use in tests.")

    def insert_into_table_from_query_async(
        self,
        *,
        destination_dataset_id: str,
        destination_table_id: str,
        query: str,
        query_parameters: Optional[List[bigquery.ScalarQueryParameter]] = None,
        allow_field_additions: bool = False,
        write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_APPEND,
        clustering_fields: Optional[List[str]] = None,
    ) -> bigquery.QueryJob:
        raise ValueError("Must be implemented for use in tests.")

    def stream_into_table(
        self,
        dataset_ref: bigquery.DatasetReference,
        table_id: str,
        rows: Sequence[Dict[str, Any]],
    ) -> None:
        raise ValueError("Must be implemented for use in tests.")

    def load_into_table_async(
        self,
        dataset_ref: bigquery.DatasetReference,
        table_id: str,
        rows: Sequence[Dict[str, Any]],
        *,
        write_disposition: bigquery.WriteDisposition = bigquery.WriteDisposition.WRITE_APPEND,
    ) -> bigquery.job.LoadJob:
        raise ValueError("Must be implemented for use in tests.")

    def delete_from_table_async(
        self, dataset_id: str, table_id: str, filter_clause: str
    ) -> bigquery.QueryJob:
        raise ValueError("Must be implemented for use in tests.")

    def materialize_view_to_table(self, view: BigQueryView) -> bigquery.Table:
        raise ValueError("Must be implemented for use in tests.")

    def add_missing_fields_to_schema(
        self,
        dataset_id: str,
        table_id: str,
        desired_schema_fields: List[bigquery.SchemaField],
    ) -> None:
        raise ValueError("Must be implemented for use in tests.")

    def remove_unused_fields_from_schema(
        self,
        dataset_id: str,
        table_id: str,
        desired_schema_fields: List[bigquery.SchemaField],
    ) -> Optional[bigquery.QueryJob]:
        raise ValueError("Must be implemented for use in tests.")

    def update_schema(
        self,
        dataset_id: str,
        table_id: str,
        desired_schema_fields: List[bigquery.SchemaField],
    ) -> None:
        raise ValueError("Must be implemented for use in tests.")

    def create_table_with_schema(
        self,
        dataset_id: str,
        table_id: str,
        schema_fields: List[bigquery.SchemaField],
        clustering_fields: List[bigquery.SchemaField] = None,
        date_partition_field: Optional[str] = None,
    ) -> bigquery.Table:
        raise ValueError("Must be implemented for use in tests.")

    def delete_table(self, dataset_id: str, table_id: str) -> None:
        raise ValueError("Must be implemented for use in tests.")

    def update_description(
        self, dataset_id: str, table_id: str, description: str
    ) -> bigquery.Table:
        raise ValueError("Must be implemented for use in tests.")

    def copy_dataset_tables_across_regions(
        self,
        source_dataset_id: str,
        destination_dataset_id: str,
        overwrite_destination_tables: bool = True,
        timeout_sec: float = DEFAULT_CROSS_REGION_COPY_TIMEOUT_SEC,
    ) -> None:
        raise ValueError("Must be implemented for use in tests.")

    def backup_dataset_tables_if_dataset_exists(
        self, dataset_id: str
    ) -> Optional[bigquery.DatasetReference]:
        raise ValueError("Must be implemented for use in tests.")

    def update_datasets_to_match_reference_schema(
        self, reference_dataset_id: str, stale_schema_dataset_ids: List[str]
    ) -> None:
        raise ValueError("Must be implemented for use in tests.")

    @staticmethod
    def schema_for_sqlalchemy_table(
        table: sqlalchemy.Table, add_state_code_field: bool = False
    ) -> List[bigquery.SchemaField]:
        raise ValueError("Must be implemented for use in tests.")

    def wait_for_big_query_jobs(self, jobs: Sequence[PollingFuture]) -> List[Any]:
        raise ValueError("Must be implemented for use in tests.")

    def copy_dataset_tables(
        self,
        source_dataset_id: str,
        destination_dataset_id: str,
        schema_only: bool = False,
    ) -> None:
        raise ValueError("Must be implemented for use in tests.")
