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
"""Tests for document_collection_config.py."""
import unittest

from google.cloud import bigquery

from recidiviz.common.constants.states import StateCode
from recidiviz.documents.store.document_collection_config import (
    DocumentCollectionConfig,
    collect_document_collection_configs,
    get_document_collection_config,
)
from recidiviz.tests.ingest.direct import fake_regions


class TestDocumentCollectionConfig(unittest.TestCase):
    """Tests for DocumentCollectionConfig."""

    def test_load_all_configs(self) -> None:
        for state_code in StateCode:
            _ = collect_document_collection_configs(state_code)

    def test_get_document_collection_config_doesnt_exist(self) -> None:
        with self.assertRaisesRegex(ValueError, "No config file found"):
            get_document_collection_config(
                StateCode.US_XX, "non_existent_collection", fake_regions
            )

    def test_get_document_collection_config(self) -> None:
        config = get_document_collection_config(
            StateCode.US_XX, "fake_case_notes", fake_regions
        )

        self.assertEqual(config.state_code, StateCode.US_XX)
        self.assertEqual(config.name, "fake_case_notes")

        pk_col_names = [col.name for col in config.primary_key_columns]
        self.assertEqual(
            pk_col_names,
            ["person_external_id", "person_external_id_type", "note_id"],
        )

        self.assertEqual(len(config.other_metadata_columns), 1)
        self.assertEqual(config.other_metadata_columns[0].name, "note_type")
        self.assertEqual(config.other_metadata_columns[0].field_type, "STRING")

    def test_collect_configs(self) -> None:
        configs = collect_document_collection_configs(StateCode.US_XX, fake_regions)
        self.assertEqual(
            configs.keys(),
            {
                "fake_case_notes",
                "fake_person_id_notes",
                "fake_staff_id_reports",
                "fake_staff_reports",
            },
        )

    def test_get_staff_external_id_config(self) -> None:
        config = get_document_collection_config(
            StateCode.US_XX, "fake_staff_reports", fake_regions
        )
        pk_col_names = [col.name for col in config.primary_key_columns]
        self.assertEqual(
            pk_col_names,
            ["staff_external_id", "staff_external_id_type", "report_id"],
        )

    def test_get_person_id_config(self) -> None:
        config = get_document_collection_config(
            StateCode.US_XX, "fake_person_id_notes", fake_regions
        )
        pk_col_names = [col.name for col in config.primary_key_columns]
        self.assertEqual(pk_col_names, ["person_id", "note_id"])

    def test_get_staff_id_config(self) -> None:
        config = get_document_collection_config(
            StateCode.US_XX, "fake_staff_id_reports", fake_regions
        )
        pk_col_names = [col.name for col in config.primary_key_columns]
        self.assertEqual(pk_col_names, ["staff_id"])

    def test_collect_configs_nonexistent_state(self) -> None:
        configs = collect_document_collection_configs(StateCode.US_LL, fake_regions)
        self.assertEqual(configs, {})

    def test_validation_duplicate_columns(self) -> None:
        with self.assertRaisesRegex(ValueError, "has duplicate column names"):
            DocumentCollectionConfig(
                state_code=StateCode.US_XX,
                name="test_collection",
                description="test collection for validation",
                primary_key_columns=[
                    bigquery.SchemaField("duplicate_column", "STRING"),
                ],
                other_metadata_columns=[
                    bigquery.SchemaField("duplicate_column", "INTEGER"),
                ],
                document_generation_query_template="SELECT 1",
            )

    def test_invalid_collection_name(self) -> None:
        with self.assertRaisesRegex(ValueError, "contains invalid characters"):
            DocumentCollectionConfig(
                state_code=StateCode.US_XX,
                name="bad-name",
                description="test collection with bad name",
                primary_key_columns=[
                    bigquery.SchemaField("pk_col", "STRING"),
                ],
                other_metadata_columns=[],
                document_generation_query_template="SELECT 1",
            )
