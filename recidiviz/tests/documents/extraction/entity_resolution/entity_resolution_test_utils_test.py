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
"""Tests for the composite generation-output row builders in
entity_resolution_test_utils.py, pinning the fakes to the real generation-output
schema so a schema change breaks them here rather than silently in a consumer."""

import datetime
import unittest

from google.cloud import bigquery

from recidiviz.documents.extraction.entity_resolution.entity_resolution_composite_document_query_builder import (
    ENTRY_SOURCE_MAP_COLUMN_NAME,
    entry_source_map_schema_field,
)
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    build_fake_composite_generation_output_row,
    build_fake_entry_source_map_entry,
    fake_entity_resolution_document_collection_config,
    patch_fake_entity_resolution_model_config_name,
)


def _field_names(schema: list[bigquery.SchemaField]) -> set[str]:
    return {field.name for field in schema}


class EntityResolutionCompositeGenerationRowBuilderTest(unittest.TestCase):
    """Tests that the fake composite generation-output row builders emit exactly
    the columns the real ER generation query produces."""

    def test_row_keys_match_generation_output_schema(self) -> None:
        # The array-sourced `assignment` group exercises a non-null source_array_index.
        with patch_fake_entity_resolution_model_config_name():
            config = fake_entity_resolution_document_collection_config("assignment")
        schema = config.build_bq_document_generation_output_schema()

        row = build_fake_composite_generation_output_row(
            root_entity_id_column="person_id",
            root_entity_id=1001,
            document_contents_id="comp_1001",
            document_update_datetime=datetime.datetime(
                2026, 1, 15, tzinfo=datetime.timezone.utc
            ),
            entries=[
                build_fake_entry_source_map_entry(
                    entry_num=1,
                    source_document_contents_id="CID_A",
                    source_document_update_datetime=datetime.datetime(
                        2026, 1, 1, 10, 0, tzinfo=datetime.timezone.utc
                    ),
                    source_array_index=0,
                )
            ],
        )

        self.assertEqual(_field_names(schema), set(row))
        self.assertEqual(
            _field_names(entry_source_map_schema_field().fields),
            set(row[ENTRY_SOURCE_MAP_COLUMN_NAME][0]),
        )
