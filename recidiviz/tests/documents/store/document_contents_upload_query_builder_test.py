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
"""Tests for DocumentContentsUploadQueryBuilder using the BQ emulator."""

from datetime import datetime
from pathlib import Path

from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    get_document_collection_config,
)
from recidiviz.documents.store.document_contents_upload_query_builder import (
    DocumentContentsUploadQueryBuilder,
)
from recidiviz.documents.store.document_store_columns import (
    DOCUMENT_CONTENTS_ID_COLUMN_NAME,
    DOCUMENT_LENGTH_BYTES_COLUMN_NAME,
    DOCUMENT_TEXT_COLUMN_NAME,
    DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME,
    get_document_store_column_schema,
)
from recidiviz.documents.store.document_upload_status_table import (
    DocumentUploadStatusTable,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.documents.store import config as fake_config_module
from recidiviz.tests.documents.store.fixtures import document_contents_upload


class TestDocumentContentsUploadQueryBuilder(BigQueryEmulatorTestCase):
    """Tests for DocumentContentsUploadQueryBuilder."""

    def setUp(self) -> None:
        super().setUp()
        self.config = get_document_collection_config(
            StateCode.US_XX, "FAKE_CASE_NOTES", fake_config_module
        )
        self.temp_new_document_contents_address = (
            self.config.temp_new_document_contents_table_address(
                self.project_id, "test_run_id"
            )
        )
        self.document_contents_table_address = (
            self.config.document_contents_table_address(self.project_id)
        )
        self.upload_status_address = DocumentUploadStatusTable.get_table_address(
            project_id=self.project_id, state_code=StateCode.US_XX
        )
        self.query_builder = DocumentContentsUploadQueryBuilder(
            project_id=self.project_id,
            state_code=StateCode.US_XX,
            collection_name=self.config.name,
        )
        self.fixture_base_dir = Path(document_contents_upload.__file__).parent

    def _fixture_path(self, subdir: str, fixture_name: str) -> Path:
        return self.fixture_base_dir / subdir / f"{fixture_name}.csv"

    @staticmethod
    def _temp_new_document_contents_schema() -> list[bigquery.SchemaField]:
        return [
            get_document_store_column_schema(DOCUMENT_CONTENTS_ID_COLUMN_NAME),
            get_document_store_column_schema(DOCUMENT_TEXT_COLUMN_NAME),
            get_document_store_column_schema(DOCUMENT_LENGTH_BYTES_COLUMN_NAME),
            get_document_store_column_schema(DOCUMENT_UPLOAD_BATCH_NUM_COLUMN_NAME),
        ]

    def _load_temp_new_document_contents(self, subdir: str) -> None:
        self.load_fixture_into_table(
            address=self.temp_new_document_contents_address.to_project_agnostic_address(),
            schema=self._temp_new_document_contents_schema(),
            fixture_path=self._fixture_path(subdir, "temp_new_document_contents_input"),
            fixture_columns=None,
            allow_comments=False,
        )
        self.load_fixture_into_table(
            address=self.upload_status_address.to_project_agnostic_address(),
            schema=DocumentUploadStatusTable.schema(),
            fixture_path=self._fixture_path(subdir, "upload_status_input"),
            fixture_columns=None,
            allow_comments=False,
        )

    def _query_document_contents_table(self) -> list[dict[str, str]]:
        return self.query(
            f"SELECT * FROM "
            f"{self.document_contents_table_address.format_address_for_query()} "
            f"ORDER BY document_contents_id"
        )

    def test_document_contents_insert(self) -> None:
        """Fixture covers:
        - just_uploaded_aaa: SUCCESS upload_status row for this collection
          this run, inserted
        - uploaded_other_collection_bbb: SUCCESS upload_status row exists for
          a different collection only, excluded (this collection didn't
          upload it in this run)
        - failed_upload_ccc: FAILURE upload_status row for this collection, excluded
        - existing_in_table_ddd: SUCCESS upload_status row for this collection
          this run, but already present in document_contents — excluded by the
          dedup guard so it's not re-inserted.
        """
        subdir = "document_contents_insert"
        self._load_temp_new_document_contents(subdir)
        self.load_fixture_into_table(
            address=self.document_contents_table_address.to_project_agnostic_address(),
            schema=self.config.build_bq_document_contents_schema(),
            fixture_path=self._fixture_path(subdir, "document_contents_existing"),
            fixture_columns=None,
            allow_comments=False,
        )

        query = self.query_builder.build_document_contents_insert_query(
            document_contents_table_address=self.document_contents_table_address,
            temp_new_document_contents_address=self.temp_new_document_contents_address,
            row_create_datetime=datetime(2026, 5, 15, 12, 0, 0),
        )
        self.query(query)

        self.compare_results_to_fixture(
            results=self._query_document_contents_table(),
            expected_output_fixture_path=self._fixture_path(subdir, "expected_output"),
            expect_missing_fixtures_on_empty_results=False,
            create_expected=False,
            expect_unique_output_rows=True,
        )

    def test_document_contents_insert_empty_temp(self) -> None:
        self.create_mock_table(
            self.temp_new_document_contents_address.to_project_agnostic_address(),
            schema=self._temp_new_document_contents_schema(),
        )
        self.create_mock_table(
            self.upload_status_address.to_project_agnostic_address(),
            schema=DocumentUploadStatusTable.schema(),
        )
        self.create_mock_table(
            self.document_contents_table_address.to_project_agnostic_address(),
            schema=self.config.build_bq_document_contents_schema(),
        )

        query = self.query_builder.build_document_contents_insert_query(
            document_contents_table_address=self.document_contents_table_address,
            temp_new_document_contents_address=self.temp_new_document_contents_address,
            row_create_datetime=datetime(2026, 5, 15, 12, 0, 0),
        )
        self.query(query)

        results = self._query_document_contents_table()
        self.assertEqual(len(results), 0)
