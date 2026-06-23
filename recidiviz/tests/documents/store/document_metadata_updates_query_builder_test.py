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
"""Tests for DocumentMetadataUpdatesQueryBuilder using the BQ emulator."""

from datetime import datetime
from pathlib import Path

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    get_document_collection_config,
)
from recidiviz.documents.store.document_metadata_updates_query_builder import (
    DocumentMetadataUpdatesQueryBuilder,
)
from recidiviz.documents.store.document_upload_status_table import (
    DocumentUploadStatusTable,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.documents.store import config as fake_config_module
from recidiviz.tests.documents.store.fixtures import metadata_updates


class TestDocumentMetadataUpdatesQueryBuilder(BigQueryEmulatorTestCase):
    """Tests for DocumentMetadataUpdatesQueryBuilder."""

    def setUp(self) -> None:
        super().setUp()
        self.config = get_document_collection_config(
            StateCode.US_XX, "FAKE_CASE_NOTES", fake_config_module
        )
        self.temp_metadata_address = (
            self.config.temp_document_metadata_updates_table_address(
                self.project_id, "test_run_id"
            )
        )
        self.metadata_table_address = self.config.metadata_table_address(
            self.project_id
        )
        self.document_contents_table_address = (
            self.config.document_contents_table_address(self.project_id)
        )
        self.upload_status_address = DocumentUploadStatusTable.get_table_address(
            project_id=self.project_id, state_code=StateCode.US_XX
        )
        self.query_builder = DocumentMetadataUpdatesQueryBuilder(
            project_id=self.project_id,
            state_code=StateCode.US_XX,
        )
        self.fixture_base_dir = Path(metadata_updates.__file__).parent

    def _fixture_path(self, subdir: str, fixture_name: str) -> Path:
        return self.fixture_base_dir / subdir / f"{fixture_name}.csv"

    def _load_temp_metadata(self, subdir: str) -> None:
        self.load_fixture_into_table(
            address=self.temp_metadata_address.to_project_agnostic_address(),
            schema=self.config.build_bq_temp_document_metadata_updates_schema(),
            fixture_path=self._fixture_path(subdir, "temp_metadata_input"),
            fixture_columns=None,
            allow_comments=False,
        )

    def _load_upload_status(self, subdir: str) -> None:
        self.load_fixture_into_table(
            address=self.upload_status_address.to_project_agnostic_address(),
            schema=DocumentUploadStatusTable.schema(),
            fixture_path=self._fixture_path(subdir, "upload_status_input"),
            fixture_columns=None,
            allow_comments=False,
        )

    def _load_document_contents(self, subdir: str) -> None:
        self.load_fixture_into_table(
            address=self.document_contents_table_address.to_project_agnostic_address(),
            schema=self.config.build_bq_document_contents_schema(),
            fixture_path=self._fixture_path(subdir, "document_contents_input"),
            fixture_columns=None,
            allow_comments=False,
        )

    def _create_empty_temp_metadata(self) -> None:
        self.create_mock_table(
            self.temp_metadata_address.to_project_agnostic_address(),
            schema=self.config.build_bq_temp_document_metadata_updates_schema(),
        )

    def _create_empty_upload_status(self) -> None:
        self.create_mock_table(
            self.upload_status_address.to_project_agnostic_address(),
            schema=DocumentUploadStatusTable.schema(),
        )

    def _create_empty_document_contents(self) -> None:
        self.create_mock_table(
            self.document_contents_table_address.to_project_agnostic_address(),
            schema=self.config.build_bq_document_contents_schema(),
        )

    def test_new_documents_query(self) -> None:
        """Fixture covers:
        - NOTE_1 (already_uploaded_aaa): already in this collection's
          document_contents, excluded
        - NOTE_2 + NOTE_5 (new_contents_bbb): same document_contents_id,
          deduplicated to one row; not in this collection's document_contents,
          included
        - NOTE_3 (new_contents_ccc): genuinely new document, included
        - NOTE_4: deletion (NULL document_contents_id), excluded
        - NOTE_6 (failed_upload_ddd): not in document_contents, included

        With batch_bytes=20 and documents ordered by document_contents_id:
        - failed_upload_ddd (18 bytes): preceding sum = 0  -> batch 0
        - new_contents_bbb  (10 bytes): preceding sum = 18 -> batch 0
        - new_contents_ccc  (10 bytes): preceding sum = 28 -> batch 1
        """
        subdir = "new_documents"
        self._load_temp_metadata(subdir)
        self._load_document_contents(subdir)

        query = self.query_builder.build_new_documents_query(
            temp_document_metadata_updates_address=self.temp_metadata_address,
            document_contents_table_address=self.document_contents_table_address,
            target_batch_bytes=20,
        )
        results = self.query(query)

        self.compare_results_to_fixture(
            results=results,
            expected_output_fixture_path=self._fixture_path(
                subdir, "new_documents_output"
            ),
            expect_missing_fixtures_on_empty_results=False,
            create_expected=False,
            expect_unique_output_rows=False,
        )

    def test_new_documents_query_batching(self) -> None:
        """Tests batch assignment with batch_bytes=20.
        Documents ordered by document_contents_id:
        - doc_aaa ( 5 bytes): preceding sum =  0 -> batch 0
        - doc_bbb (15 bytes): preceding sum =  5 -> batch 0
        - doc_ccc (30 bytes): preceding sum = 20 -> batch 1 (oversized doc, gets own batch)
        - doc_ddd ( 8 bytes): preceding sum = 50 -> batch 2
        - doc_eee (10 bytes): preceding sum = 58 -> batch 2
        """
        subdir = "new_documents_batching"
        self._load_temp_metadata(subdir)
        self._create_empty_document_contents()

        query = self.query_builder.build_new_documents_query(
            temp_document_metadata_updates_address=self.temp_metadata_address,
            document_contents_table_address=self.document_contents_table_address,
            target_batch_bytes=20,
        )
        results = self.query(query)

        self.compare_results_to_fixture(
            results=results,
            expected_output_fixture_path=self._fixture_path(
                subdir, "new_documents_output"
            ),
            expect_missing_fixtures_on_empty_results=False,
            create_expected=False,
            expect_unique_output_rows=False,
        )

    def test_new_documents_query_empty_temp_metadata(self) -> None:
        self._create_empty_temp_metadata()
        self._create_empty_document_contents()

        query = self.query_builder.build_new_documents_query(
            temp_document_metadata_updates_address=self.temp_metadata_address,
            document_contents_table_address=self.document_contents_table_address,
            target_batch_bytes=1_000_000_000,
        )
        results = self.query(query)
        self.assertEqual(len(results), 0)

    def _create_metadata_table(self) -> None:
        self.create_mock_table(
            self.metadata_table_address.to_project_agnostic_address(),
            schema=self.config.build_bq_metadata_schema(),
        )

    def _query_metadata_table(self) -> list[dict[str, str]]:
        return self.query(
            f"SELECT * FROM {self.metadata_table_address.format_address_for_query()}"
        )

    def test_successful_uploads_metadata_insert(self) -> None:
        """Fixture covers:
        - NOTE_1: successfully uploaded, included in results
        - NOTE_2: failed upload, excluded
        - NOTE_3: deleted (NULL document_contents_id), included
        - NOTE_4: no upload status entry, excluded
        - NOTE_5: successfully uploaded in previous job run, included in results
        """
        subdir = "successful_uploads"
        self._load_temp_metadata(subdir)
        self._load_upload_status(subdir)
        self._create_metadata_table()

        query = self.query_builder.build_successful_uploads_metadata_insert_query(
            config=self.config,
            metadata_table_address=self.metadata_table_address,
            temp_document_metadata_updates_address=self.temp_metadata_address,
            row_create_datetime=datetime(2026, 3, 15, 12, 0, 0),
        )
        self.query(query)
        results = self._query_metadata_table()

        self.compare_results_to_fixture(
            results=results,
            expected_output_fixture_path=self._fixture_path(subdir, "expected_output"),
            expect_missing_fixtures_on_empty_results=False,
            create_expected=False,
            expect_unique_output_rows=False,
        )

    def test_successful_uploads_metadata_insert_empty_temp_metadata(self) -> None:
        self._create_empty_temp_metadata()
        self._create_empty_upload_status()
        self._create_metadata_table()

        query = self.query_builder.build_successful_uploads_metadata_insert_query(
            config=self.config,
            metadata_table_address=self.metadata_table_address,
            temp_document_metadata_updates_address=self.temp_metadata_address,
            row_create_datetime=datetime(2026, 3, 15, 12, 0, 0),
        )
        self.query(query)
        results = self._query_metadata_table()
        self.assertEqual(len(results), 0)
