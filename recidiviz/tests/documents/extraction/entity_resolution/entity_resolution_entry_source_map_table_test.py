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
"""Tests for entity_resolution_entry_source_map_table.py."""
import unittest

from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import BigQueryFieldMode
from recidiviz.common.constants.states import StateCode
from recidiviz.documents.extraction.entity_resolution.entity_resolution_entry_source_map_table import (
    EntityResolutionEntrySourceMapBQTable,
)
from recidiviz.documents.store.document_collection_config import (
    DocumentRootEntityIdType,
)
from recidiviz.documents.store.document_store_columns import (
    PERSON_ID_COLUMN_NAME,
    STAFF_ID_COLUMN_NAME,
)
from recidiviz.tests.documents.extraction.entity_resolution.entity_resolution_test_utils import (
    FAKE_COLLECTION_NAME,
)

_ASSIGNMENT_GROUP_NAME = "assignment"


def _expected_entry_columns() -> list[bigquery.SchemaField]:
    """The map table's columns below the root entity ID: the composite document
    the entries belong to, then one column per `entry_source_map` struct sub-field.
    """
    return [
        bigquery.SchemaField(
            name="document_contents_id",
            field_type=SqlTypeNames.STRING.value,
            mode=BigQueryFieldMode.REQUIRED.value,
            description="document_contents_id of the entity-resolution composite "
            "document this entry belongs to.",
        ),
        bigquery.SchemaField(
            name="entry_num",
            field_type=SqlTypeNames.INT64.value,
            mode=BigQueryFieldMode.REQUIRED.value,
            description="1-based entry number within the composite document.",
        ),
        bigquery.SchemaField(
            name="source_document_contents_id",
            field_type=SqlTypeNames.STRING.value,
            mode=BigQueryFieldMode.REQUIRED.value,
            description="document_contents_id of the first-order source document "
            "this entry was rendered from.",
        ),
        bigquery.SchemaField(
            name="source_document_update_datetime",
            field_type=SqlTypeNames.TIMESTAMP.value,
            mode=BigQueryFieldMode.REQUIRED.value,
            description="document_update_datetime of the source-note occurrence "
            "this entry was rendered from. Distinguishes occurrences of "
            "identically-worded notes, which share a document_contents_id.",
        ),
        bigquery.SchemaField(
            name="source_array_index",
            field_type=SqlTypeNames.INT64.value,
            mode=BigQueryFieldMode.NULLABLE.value,
            description="0-based element position within the source document's "
            "array; null for a top-level entity group.",
        ),
    ]


class EntityResolutionEntrySourceMapBQTableTest(unittest.TestCase):
    """Tests for EntityResolutionEntrySourceMapBQTable."""

    def test_table_id(self) -> None:
        self.assertEqual(
            "fake_extractor_collection_assignment_entry_source_map",
            EntityResolutionEntrySourceMapBQTable.table_id(
                first_order_extractor_collection_name=FAKE_COLLECTION_NAME,
                entity_group_name=_ASSIGNMENT_GROUP_NAME,
            ),
        )

    def test_address(self) -> None:
        self.assertEqual(
            BigQueryAddress.from_str(
                "us_xx_document_store_metadata."
                "fake_extractor_collection_assignment_entry_source_map"
            ),
            EntityResolutionEntrySourceMapBQTable.address(
                state_code=StateCode.US_XX,
                first_order_extractor_collection_name=FAKE_COLLECTION_NAME,
                entity_group_name=_ASSIGNMENT_GROUP_NAME,
                sandbox_prefix=None,
            ),
        )

    def test_schema(self) -> None:
        self.assertEqual(
            [
                bigquery.SchemaField(
                    name=PERSON_ID_COLUMN_NAME,
                    field_type=SqlTypeNames.INT64.value,
                    mode=BigQueryFieldMode.REQUIRED.value,
                    description="Recidiviz internal person_id for the root entity",
                ),
                *_expected_entry_columns(),
            ],
            EntityResolutionEntrySourceMapBQTable.schema(
                root_entity_id_type=DocumentRootEntityIdType.PERSON_ID
            ),
        )

    def test_schema_for_staff_rooted_collection(self) -> None:
        self.assertEqual(
            [
                bigquery.SchemaField(
                    name=STAFF_ID_COLUMN_NAME,
                    field_type=SqlTypeNames.INT64.value,
                    mode=BigQueryFieldMode.REQUIRED.value,
                    description="Recidiviz internal staff_id for the root entity",
                ),
                *_expected_entry_columns(),
            ],
            EntityResolutionEntrySourceMapBQTable.schema(
                root_entity_id_type=DocumentRootEntityIdType.STAFF_ID
            ),
        )

    def test_schema_raises_for_external_id_root_entity_type(self) -> None:
        for root_entity_id_type in (
            DocumentRootEntityIdType.PERSON_EXTERNAL_ID,
            DocumentRootEntityIdType.STAFF_EXTERNAL_ID,
        ):
            with self.subTest(root_entity_id_type=root_entity_id_type.value):
                with self.assertRaisesRegex(
                    ValueError,
                    r"^Entry→source map tables are keyed by the internal root entity "
                    r"ID, but got external-id type",
                ):
                    EntityResolutionEntrySourceMapBQTable.schema(
                        root_entity_id_type=root_entity_id_type
                    )
