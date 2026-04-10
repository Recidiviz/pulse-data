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
"""Tests for DocumentCollectionMetadataTableQueryBuilder using the BQ emulator."""
from pathlib import Path

import pandas as pd

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    get_document_collection_config,
)
from recidiviz.documents.store.document_metadata_table_query_builder import (
    DocumentCollectionMetadataTableQueryBuilder,
)
from recidiviz.ingest.direct.dataset_config import (
    document_store_metadata_dataset_for_region,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.documents.store.fixtures import document_metadata
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.ingest.direct.fixture_util import load_dataframe_from_path


class TestDocumentCollectionMetadataTableQueryBuilder(BigQueryEmulatorTestCase):
    """Tests for DocumentCollectionMetadataTableQueryBuilder."""

    def setUp(self) -> None:
        super().setUp()
        self.config = get_document_collection_config(
            StateCode.US_XX, "fake_case_notes", fake_regions
        )
        self.metadata_address = BigQueryAddress(
            dataset_id=document_store_metadata_dataset_for_region(StateCode.US_XX),
            table_id=self.config.metadata_table_id,
        )
        self.query_builder = DocumentCollectionMetadataTableQueryBuilder(
            project_id=self.project_id,
        )

    def _fixture_path(self, fixture_name: str) -> Path:
        return Path(document_metadata.__file__).parent / fixture_name

    def _load_metadata(
        self, config: DocumentCollectionConfig, data: pd.DataFrame
    ) -> None:
        self.create_mock_table(
            self.metadata_address,
            schema=config.build_bq_metadata_schema(),
        )
        self.load_rows_into_table(self.metadata_address, data.to_dict("records"))

    def test_latest_documents(self) -> None:
        """Tests that the latest documents query correctly deduplicates to one
        row per primary key. Fixture covers:
        - NOTE_1: two versions with different timestamps, only the latest is
          returned
        - NOTE_2: two versions with same document_contents_id but different
          note_type, returns the latest metadata
        - NOTE_5: single version, returned as-is
        - NOTE_6: single version with NULL note_type (non-PK metadata column)
        - NOTE_7: latest version has NULL document_contents_id (deleted),
          excluded from results
        """
        input_df = load_dataframe_from_path(
            self._fixture_path("fake_case_notes_input.csv"),
            fixture_columns=None,
            allow_comments=False,
        )
        self._load_metadata(self.config, input_df)

        query = self.query_builder.build_latest_documents_query(
            config=self.config,
        )
        results = self.query(query)

        expected = load_dataframe_from_path(
            self._fixture_path("fake_case_notes_output.csv"),
            fixture_columns=None,
            allow_comments=False,
        )
        self.compare_expected_and_result_dfs(expected=expected, results=results)
