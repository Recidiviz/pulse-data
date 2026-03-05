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
"""Tests for document_extraction_raw_source_table_collector.py."""
import unittest

from recidiviz.NOT_FOR_PRODUCTION_USE.documents.extraction.persisted_models.document_extraction_result_metadata import (
    DocumentExtractionResultMetadata,
)
from recidiviz.NOT_FOR_PRODUCTION_USE.source_tables.document_extraction_raw_source_table_collector import (
    collect_document_extraction_raw_source_table_collection,
)


class TestDocumentExtractionRawSourceTableCollector(unittest.TestCase):
    """Tests for the raw extraction result source table collector."""

    def test_collection_is_generated(self) -> None:
        collection = collect_document_extraction_raw_source_table_collection()
        self.assertGreater(len(collection.source_tables), 0)

    def test_dataset_naming(self) -> None:
        collection = collect_document_extraction_raw_source_table_collection()
        self.assertEqual(
            collection.dataset_id,
            DocumentExtractionResultMetadata.RAW_DATASET_ID,
        )

    def test_tables_have_expected_schema_columns(self) -> None:
        collection = collect_document_extraction_raw_source_table_collection()
        expected_columns = {
            "job_id",
            "document_id",
            "extractor_id",
            "extractor_version_id",
            "extraction_datetime",
            "status",
            "result_json",
            "error_message",
            "error_type",
        }
        for table in collection.source_tables:
            actual_columns = {f.name for f in table.schema_fields}
            self.assertEqual(
                actual_columns,
                expected_columns,
                f"Table {table.address.to_str()} has unexpected schema columns",
            )

    def test_protected_update_config(self) -> None:
        collection = collect_document_extraction_raw_source_table_collection()
        self.assertTrue(collection.update_config.attempt_to_manage)
        self.assertFalse(collection.update_config.allow_field_deletions)
