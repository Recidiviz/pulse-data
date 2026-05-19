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
"""Tests for document_store_types.py."""
import unittest

from recidiviz.big_query.big_query_address import ProjectSpecificBigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_store_types import (
    DocumentUploadBatch,
    SingleCollectionDocumentDiscoveryResult,
)


class TestDocumentUploadBatchSerialization(unittest.TestCase):
    def test_to_dict_from_dict_roundtrip(self) -> None:
        batch = DocumentUploadBatch(
            collection_name="test_collection",
            temp_new_document_contents_table_address=ProjectSpecificBigQueryAddress(
                project_id="recidiviz-testing",
                dataset_id="ds",
                table_id="contents",
            ),
            batch_number=4,
        )
        self.assertEqual(batch, DocumentUploadBatch.from_dict(batch.to_dict()))


class TestSingleCollectionDocumentDiscoveryResultSerialization(unittest.TestCase):
    def test_to_dict_from_dict_roundtrip(self) -> None:
        result = SingleCollectionDocumentDiscoveryResult(
            state_code=StateCode.US_XX,
            collection_name="test_collection",
            temp_document_metadata_updates_address=ProjectSpecificBigQueryAddress(
                project_id="recidiviz-testing",
                dataset_id="ds",
                table_id="metadata",
            ),
            temp_new_document_contents_address=ProjectSpecificBigQueryAddress(
                project_id="recidiviz-testing",
                dataset_id="ds",
                table_id="contents",
            ),
            num_new_document_contents_rows=42,
            num_document_metadata_updates_rows=99,
        )
        self.assertEqual(
            result,
            SingleCollectionDocumentDiscoveryResult.from_dict(result.to_dict()),
        )
