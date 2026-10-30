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
"""Tests for golden_eval_entity_resolution_tables."""
from unittest import TestCase

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.eval.golden_eval_entity_resolution_tables import (
    GoldenEvalDocumentsBQTable,
)


class GoldenEvalDocumentsBQTableTest(TestCase):
    """Tests for GoldenEvalDocumentsBQTable."""

    def test_address_is_sandbox_prefixed(self) -> None:
        self.assertEqual(
            BigQueryAddress(
                dataset_id="my_prefix_document_extraction_golden_eval_results",
                table_id="fake_extractor_collection__documents",
            ),
            GoldenEvalDocumentsBQTable.address(
                collection_name="FAKE_EXTRACTOR_COLLECTION",
                sandbox_prefix="my_prefix",
            ),
        )

    def test_unprefixed_address(self) -> None:
        self.assertEqual(
            BigQueryAddress(
                dataset_id="document_extraction_golden_eval_results",
                table_id="fake_extractor_collection__documents",
            ),
            GoldenEvalDocumentsBQTable.address(
                collection_name="FAKE_EXTRACTOR_COLLECTION", sandbox_prefix=None
            ),
        )

    def test_to_row_matches_schema(self) -> None:
        row = GoldenEvalDocumentsBQTable.to_row(
            state_code=StateCode.US_XX,
            golden_document_id="unit_1",
            document_text="<p>Assigned to dish duty.</p>",
        )

        self.assertEqual(
            {
                "state_code": "US_XX",
                "golden_document_id": "unit_1",
                "document_text": "<p>Assigned to dish duty.</p>",
            },
            row,
        )
        self.assertEqual(
            [field.name for field in GoldenEvalDocumentsBQTable.schema()],
            list(row),
        )
