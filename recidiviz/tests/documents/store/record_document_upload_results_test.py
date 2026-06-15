# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for record_document_upload_results.py."""

import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock

from google.api_core.exceptions import NotFound

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    collect_document_collection_configs,
)
from recidiviz.documents.store.document_store_types import (
    SingleCollectionDocumentDiscoveryResult,
)
from recidiviz.documents.store.record_document_upload_results import (
    DocumentUploadResultRecorder,
)
from recidiviz.tests.documents.store import config as fake_config_module


class TestDocumentUploadResultRecorder(unittest.TestCase):
    """Tests for DocumentUploadResultRecorder."""

    def setUp(self) -> None:
        self.project_id = "recidiviz-testing"
        self.state_code = StateCode.US_XX
        self.run_id = "test_run_123"
        self.row_create_datetime = datetime(2026, 4, 21, 12, 0, 0, tzinfo=timezone.utc)
        self.bq_client = MagicMock()
        self.recorder = DocumentUploadResultRecorder(
            state_code=self.state_code,
            project_id=self.project_id,
            big_query_client=self.bq_client,
            run_id=self.run_id,
            metadata_row_create_datetime=self.row_create_datetime,
        )

        self.config = next(
            iter(
                collect_document_collection_configs(
                    self.state_code, config_module=fake_config_module
                ).values()
            )
        )
        self.discovery_result = self._make_discovery_result(
            self.config,
            num_new_document_contents_rows=5,
            num_document_metadata_updates_rows=10,
        )

        self.query_job_all_docs_uploaded = MagicMock()
        result_all = MagicMock()
        # If all documents were uploaded successfully, the number of rows inserted into the metadata table should equal the number of rows in the temp metadata updates table (num_document_metadata_updates_rows).
        result_all.num_dml_affected_rows = 10
        self.query_job_all_docs_uploaded.result.return_value = result_all

        self.query_job_doc_upload_failures = MagicMock()
        result_failures = MagicMock()
        # If some documents failed to upload, the number of rows inserted into the metadata table will be less than the number of rows in the temp metadata updates table.
        result_failures.num_dml_affected_rows = 7
        self.query_job_doc_upload_failures.result.return_value = result_failures

    def _make_discovery_result(
        self,
        config: DocumentCollectionConfig,
        num_new_document_contents_rows: int,
        num_document_metadata_updates_rows: int,
    ) -> SingleCollectionDocumentDiscoveryResult:
        return SingleCollectionDocumentDiscoveryResult(
            state_code=config.state_code,
            collection_name=config.name,
            temp_document_metadata_updates_address=config.temp_document_metadata_updates_table_address(
                self.project_id, self.run_id
            ),
            temp_new_document_contents_address=config.temp_new_document_contents_table_address(
                self.project_id, self.run_id
            ),
            num_new_document_contents_rows=num_new_document_contents_rows,
            num_document_metadata_updates_rows=num_document_metadata_updates_rows,
        )

    def test_all_uploads_succeeded_deletes_temp_tables(self) -> None:
        self.bq_client.run_query_async.return_value = self.query_job_all_docs_uploaded

        self.recorder.run(self.discovery_result)

        self.bq_client.load_table_from_cloud_storage.assert_called_once()
        self.bq_client.run_query_async.assert_called_once()
        self.assertEqual(self.bq_client.delete_table.call_count, 2)
        deleted_addresses = {
            c.args[0] for c in self.bq_client.delete_table.call_args_list
        }
        self.assertEqual(
            deleted_addresses,
            {
                self.discovery_result.temp_document_metadata_updates_address.to_project_agnostic_address(),
                self.discovery_result.temp_new_document_contents_address.to_project_agnostic_address(),
            },
        )

    def test_some_uploads_failed_retains_temp_tables(self) -> None:
        self.bq_client.run_query_async.return_value = self.query_job_doc_upload_failures

        self.recorder.run(self.discovery_result)

        self.bq_client.load_table_from_cloud_storage.assert_called_once()
        self.bq_client.run_query_async.assert_called_once()
        self.bq_client.delete_table.assert_not_called()

    def test_no_new_document_contents_skips_csv_load(self) -> None:
        metadata_only_result = self._make_discovery_result(
            self.config,
            num_new_document_contents_rows=0,
            num_document_metadata_updates_rows=10,
        )
        self.bq_client.run_query_async.return_value = self.query_job_all_docs_uploaded

        self.recorder.run(metadata_only_result)

        # don't upload CSVs if there are no new document contents
        self.bq_client.load_table_from_cloud_storage.assert_not_called()
        # but still attempt to insert metadata rows
        self.bq_client.run_query_async.assert_called_once()

    def test_load_upload_status_not_found_raises(self) -> None:
        load_job_mock = MagicMock()
        load_job_mock.result.side_effect = NotFound("Not found: Uris gs://...")
        self.bq_client.load_table_from_cloud_storage.return_value = load_job_mock

        with self.assertRaises(NotFound):
            self.recorder.run(self.discovery_result)

        self.bq_client.load_table_from_cloud_storage.assert_called_once()
        self.bq_client.run_query_async.assert_not_called()
        self.bq_client.delete_table.assert_not_called()

    def test_load_upload_status_fails(self) -> None:
        self.bq_client.load_table_from_cloud_storage.side_effect = ValueError(
            "BQ load failed"
        )

        with self.assertRaisesRegex(ValueError, "BQ load failed"):
            self.recorder.run(self.discovery_result)

        self.bq_client.load_table_from_cloud_storage.assert_called_once()
        self.bq_client.run_query_async.assert_not_called()
        self.bq_client.delete_table.assert_not_called()
