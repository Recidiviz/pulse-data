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
"""Tests for document_collection_query_builder.py."""
import unittest
from pathlib import Path

from freezegun import freeze_time
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.enums import SqlTypeNames

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    collect_document_collection_configs,
    get_document_collection_config,
)
from recidiviz.documents.store.document_collection_query_builder import (
    DocumentCollectionDiffQueryBuilder,
)
from recidiviz.documents.store.document_upload_status_table import (
    DocumentUploadStatusTable,
)
from recidiviz.ingest.direct.dataset_config import (
    document_store_metadata_dataset_for_region,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.big_query.sqlglot_helpers import check_query_selects_output_columns
from recidiviz.tests.documents.store.fixtures import document_diff, new_documents
from recidiviz.tests.ingest.direct import fake_regions


class TestBuildDocumentGenerationQuery(unittest.TestCase):
    def test_generation_query_output_matches_temp_table_schema(self) -> None:
        for state_code in StateCode:
            configs = collect_document_collection_configs(state_code)
            for config in configs.values():
                expected_columns = {
                    field.name for field in config.build_bq_temp_table_schema()
                }
                query = DocumentCollectionDiffQueryBuilder(
                    project_id="test-project",
                ).build_document_generation_query(
                    config=config,
                )
                try:
                    check_query_selects_output_columns(
                        query=query,
                        expected_output_columns=expected_columns,
                    )
                except ValueError as e:
                    raise ValueError(
                        f"Query output column mismatch for "
                        f"[{state_code.value}/{config.name}]"
                    ) from e


class TestBuildDocumentDiffQuery(BigQueryEmulatorTestCase):
    """Tests for build_document_diff_query."""

    def setUp(self) -> None:
        super().setUp()
        self.config = get_document_collection_config(
            StateCode.US_XX, "fake_case_notes", fake_regions
        )
        self.metadata_address = BigQueryAddress(
            dataset_id=document_store_metadata_dataset_for_region(StateCode.US_XX),
            table_id=self.config.metadata_table_id,
        )
        self.raw_table_address = BigQueryAddress(
            dataset_id="us_xx_raw_data",
            table_id="fake_notes",
        )
        self.query_builder = DocumentCollectionDiffQueryBuilder(
            project_id=self.project_id,
        )
        self.fixture_dir = Path(document_diff.__file__).parent

    def _fixture_path(self, fixture_name: str) -> Path:
        return self.fixture_dir / f"{fixture_name}.csv"

    def _load_raw_table(self, fixture_path: Path) -> None:
        self.load_fixture_into_table(
            address=self.raw_table_address,
            schema=[
                SchemaField("person_id", SqlTypeNames.STRING),
                SchemaField("note_id", SqlTypeNames.STRING),
                SchemaField("note_type", SqlTypeNames.STRING),
                SchemaField("note_body", SqlTypeNames.STRING),
                SchemaField("created_at", SqlTypeNames.STRING),
            ],
            fixture_path=fixture_path,
            fixture_columns=None,
            allow_comments=False,
        )

    def _load_metadata(
        self,
        config: DocumentCollectionConfig,
        fixture_path: Path,
    ) -> None:
        self.load_fixture_into_table(
            address=self.metadata_address,
            schema=config.build_bq_metadata_schema(),
            fixture_path=fixture_path,
            fixture_columns=None,
            allow_comments=False,
        )

    def test_document_diff(self) -> None:
        """Fixture covers:
        - NOTE_1: same content and metadata in both, excluded from diff
        - NOTE_2: content differs between new and current (updated)
        - NOTE_3: exists in new but not current (added)
        - NOTE_5: exists in current but not new (deleted, NULL content)
        - NOTE_6: same content but note_type changed (metadata-only update)
        - NOTE_8: added with NULL document_text, NULL document_contents_id
        """
        self._load_raw_table(self._fixture_path("fake_notes_latest"))
        self._load_metadata(self.config, self._fixture_path("fake_case_notes_metadata"))

        with freeze_time("2026-03-15 12:00:00", tz_offset=0):
            query = self.query_builder.build_document_diff_query(config=self.config)
        results = self.query(query)

        self.compare_results_to_fixture(
            results=results,
            expected_output_fixture_path=self._fixture_path(
                "fake_case_notes_diff_output"
            ),
            expect_missing_fixtures_on_empty_results=False,
            create_expected=False,
            expect_unique_output_rows=False,
        )


class TestBuildNewDocumentsQuery(BigQueryEmulatorTestCase):
    """Tests for build_new_documents_query."""

    def setUp(self) -> None:
        super().setUp()
        self.config = get_document_collection_config(
            StateCode.US_XX, "fake_case_notes", fake_regions
        )
        dataset_id = document_store_metadata_dataset_for_region(StateCode.US_XX)
        self.temp_metadata_address = BigQueryAddress(
            dataset_id=dataset_id,
            table_id=f"{self.config.name}_temp_metadata",
        )
        self.upload_status_address = BigQueryAddress(
            dataset_id=dataset_id,
            table_id=DocumentUploadStatusTable.table_id,
        )
        self.fixture_dir = Path(new_documents.__file__).parent

    def _fixture_path(self, fixture_name: str) -> Path:
        return self.fixture_dir / f"{fixture_name}.csv"

    def test_new_documents_query(self) -> None:
        """Fixture covers:
        - NOTE_1: already successfully uploaded, excluded
        - NOTE_2 + NOTE_5: same document_contents_id, deduplicated to one row
        - NOTE_3: genuinely new document, included
        - NOTE_4: deletion (NULL document_contents_id), excluded
        - NOTE_6: previously failed upload, included
        """
        self.load_fixture_into_table(
            address=self.temp_metadata_address,
            schema=self.config.build_bq_temp_table_schema(),
            fixture_path=self._fixture_path("temp_metadata_input"),
            fixture_columns=None,
            allow_comments=False,
        )
        self.load_fixture_into_table(
            address=self.upload_status_address,
            schema=DocumentUploadStatusTable.schema(),
            fixture_path=self._fixture_path("upload_status_input"),
            fixture_columns=None,
            allow_comments=False,
        )

        query = DocumentCollectionDiffQueryBuilder.build_new_documents_query(
            temp_metadata_address=self.temp_metadata_address.to_project_specific_address(
                self.project_id
            ),
            upload_status_address=self.upload_status_address.to_project_specific_address(
                self.project_id
            ),
        )
        results = self.query(query)

        self.compare_results_to_fixture(
            results=results,
            expected_output_fixture_path=self._fixture_path("new_documents_output"),
            expect_missing_fixtures_on_empty_results=False,
            create_expected=False,
            expect_unique_output_rows=False,
        )

    def test_empty_temp_metadata(self) -> None:
        self.create_mock_table(
            self.temp_metadata_address,
            schema=self.config.build_bq_temp_table_schema(),
        )
        self.create_mock_table(
            self.upload_status_address,
            schema=DocumentUploadStatusTable.schema(),
        )

        query = DocumentCollectionDiffQueryBuilder.build_new_documents_query(
            temp_metadata_address=self.temp_metadata_address.to_project_specific_address(
                self.project_id
            ),
            upload_status_address=self.upload_status_address.to_project_specific_address(
                self.project_id
            ),
        )
        results = self.query(query)
        self.assertEqual(len(results), 0)
