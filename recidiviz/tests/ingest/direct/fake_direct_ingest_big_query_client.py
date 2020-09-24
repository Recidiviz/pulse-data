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
"""A fake implementation of BigQueryClient for use in direct ingest tests."""
from typing import List, Optional, Iterator, Callable

from google.cloud import bigquery

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.big_query.export.export_query_config import ExportQueryConfig
from recidiviz.ingest.direct.controllers.gcsfs_direct_ingest_utils import filename_parts_from_path
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.tests.cloud_storage.fake_gcs_file_system import FakeGCSFileSystem


class FakeQueryJob:
    """A fake implementation of bigquery.QueryJob for use in direct ingest tests."""
    def result(self, _page_size=None, _max_results=None, _retry=None, _timeout=None):
        return


class FakeDirectIngestBigQueryClient(BigQueryClient):
    """A fake implementation of BigQueryClient for use in direct ingest tests."""
    def __init__(self,
                 project_id: str,
                 fs: FakeGCSFileSystem):
        self._project_id = project_id
        self.fs = fs
        self.exported_file_tags: List[str] = []

    @property
    def project_id(self) -> str:
        return self._project_id

    def dataset_ref_for_id(self, dataset_id: str) -> bigquery.DatasetReference:
        raise ValueError('Must be implemented for use in tests.')

    def create_dataset_if_necessary(self, dataset_ref: bigquery.DatasetReference) -> None:
        raise ValueError('Must be implemented for use in tests.')

    def dataset_exists(self, dataset_ref: bigquery.DatasetReference) -> bool:
        raise ValueError('Must be implemented for use in tests.')

    def table_exists(self, dataset_ref: bigquery.DatasetReference, table_id: str) -> bool:
        raise ValueError('Must be implemented for use in tests.')

    def get_table(self, dataset_ref: bigquery.DatasetReference, table_id: str) -> bigquery.Table:
        raise ValueError('Must be implemented for use in tests.')

    def get_table_if_exists(self, dataset_ref: bigquery.DatasetReference, table_id: str) -> Optional[bigquery.Table]:
        raise ValueError('Must be implemented for use in tests.')

    def list_tables(self, dataset_id: str) -> Iterator[bigquery.table.TableListItem]:
        raise ValueError('Must be implemented for use in tests.')

    def create_table(self, table: bigquery.Table) -> bigquery.Table:
        raise ValueError('Must be implemented for use in tests.')

    def create_or_update_view(self, dataset_ref: bigquery.DatasetReference, view: BigQueryView) -> bigquery.Table:
        raise ValueError('Must be implemented for use in tests.')

    def load_table_from_cloud_storage_async(
            self, source_uri: str,
            destination_dataset_ref: bigquery.DatasetReference,
            destination_table_id: str,
            destination_table_schema: List[bigquery.SchemaField]) -> bigquery.job.LoadJob:
        raise ValueError('Must be implemented for use in tests.')

    def export_table_to_cloud_storage_async(
            self,
            source_table_dataset_ref: bigquery.dataset.DatasetReference,
            source_table_id: str, destination_uri: str,
            destination_format: bigquery.DestinationFormat) -> Optional[bigquery.ExtractJob]:
        raise ValueError('Must be implemented for use in tests.')

    def export_query_results_to_cloud_storage(self,
                                              export_configs: List[ExportQueryConfig]) -> None:
        for export_config in export_configs:
            export_path = GcsfsFilePath.from_absolute_path(export_config.output_uri)
            self.fs.test_add_path(export_path)
            self.exported_file_tags.append(filename_parts_from_path(export_path).file_tag)

    def run_query_async(self, query_str: str, query_parameters: List[bigquery.ScalarQueryParameter] = None) \
            -> bigquery.QueryJob:
        raise ValueError('Must be implemented for use in tests.')

    def paged_read_and_process(self,
                               query_job: bigquery.QueryJob,
                               page_size: int,
                               process_fn: Callable[[bigquery.table.Row], None]) -> None:
        raise ValueError('Must be implemented for use in tests.')

    def copy_view(self, view: BigQueryView, destination_client: 'BigQueryClient',
                  destination_dataset_ref: bigquery.DatasetReference) -> bigquery.Table:
        raise ValueError('Must be implemented for use in tests.')

    def create_table_from_query_async(self, dataset_id: str, table_id: str, query: str,
                                      query_parameters: List[bigquery.ScalarQueryParameter],
                                      overwrite: Optional[bool] = False) -> bigquery.QueryJob:
        return FakeQueryJob()

    def insert_into_table_from_table_async(self, source_dataset_id: str, source_table_id: str,
                                           destination_dataset_id: str, destination_table_id: str,
                                           source_data_filter_clause: Optional[str] = None,
                                           allow_field_additions: bool = False) -> bigquery.QueryJob:
        raise ValueError('Must be implemented for use in tests.')

    def insert_into_table_from_cloud_storage_async(
            self, source_uri: str,
            destination_dataset_ref: bigquery.DatasetReference,
            destination_table_id: str, destination_table_schema: List[bigquery.SchemaField]) -> bigquery.job.LoadJob:
        raise ValueError('Must be implemented for use in tests.')

    def delete_from_table_async(self, dataset_id: str, table_id: str, filter_clause: str) -> bigquery.QueryJob:
        raise ValueError('Must be implemented for use in tests.')

    def materialize_view_to_table(self, view: BigQueryView) -> None:
        raise ValueError('Must be implemented for use in tests.')

    def add_missing_fields_to_schema(self, dataset_id: str, table_id: str, schema_fields: List[bigquery.SchemaField]) \
            -> None:
        raise ValueError('Must be implemented for use in tests.')

    def create_table_with_schema(self, dataset_id, table_id, schema_fields: List[bigquery.SchemaField]) -> \
            bigquery.Table:
        raise ValueError('Must be implemented for use in tests.')

    def delete_table(self, dataset_id: str, table_id: str) -> None:
        return
