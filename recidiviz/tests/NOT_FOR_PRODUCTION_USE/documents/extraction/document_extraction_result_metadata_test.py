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
"""Tests for DocumentExtractionResultMetadata raw table address methods."""
import unittest

from recidiviz.common.constants.states import StateCode
from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extraction_result_metadata import (
    DocumentExtractionResultMetadata,
)


class TestRawTableAddress(unittest.TestCase):
    """Tests for DocumentExtractionResultMetadata table addressing."""

    def test_raw_dataset_id(self) -> None:
        self.assertEqual(
            DocumentExtractionResultMetadata.RAW_DATASET_ID,
            "document_extraction_results__raw",
        )

    def test_raw_table_id(self) -> None:
        self.assertEqual(
            DocumentExtractionResultMetadata.raw_table_id(
                StateCode.US_IX, "CASE_NOTE_EMPLOYMENT_INFO"
            ),
            "us_ix_case_note_employment_info",
        )

    def test_raw_table_address_default_dataset(self) -> None:
        address = DocumentExtractionResultMetadata.raw_table_address(
            state_code=StateCode.US_IX,
            collection_name="CASE_NOTE_EMPLOYMENT_INFO",
            sandbox_dataset_prefix=None,
        )
        self.assertEqual(
            address.dataset_id,
            "document_extraction_results__raw",
        )
        self.assertEqual(
            address.table_id,
            "us_ix_case_note_employment_info",
        )

    def test_raw_table_address_with_sandbox_prefix(self) -> None:
        address = DocumentExtractionResultMetadata.raw_table_address(
            state_code=StateCode.US_IX,
            collection_name="CASE_NOTE_EMPLOYMENT_INFO",
            sandbox_dataset_prefix="my_sandbox",
        )
        self.assertEqual(
            address.dataset_id,
            "my_sandbox_document_extraction_results__raw",
        )
        self.assertEqual(
            address.table_id,
            "us_ix_case_note_employment_info",
        )
