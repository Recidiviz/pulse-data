# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for entities_bq_schema.py"""
import unittest
from unittest.mock import MagicMock, patch

from google.cloud.bigquery import SchemaField
from more_itertools import one

from recidiviz.persistence.entity.entities_bq_schema import (
    get_bq_schema_for_entities_module,
    get_bq_schema_for_entity_table,
)
from recidiviz.persistence.entity.identity import (
    identity_cluster_entities,
    identity_fragment_entities,
)
from recidiviz.tests.persistence.entity import fake_entities
from recidiviz.tests.persistence.entity.fake_entities_module_context import (
    FakeEntitiesModuleContext,
)


class TestGetBqSchemaForEntitiesModule(unittest.TestCase):
    """Tests for entities_bq_schema.py"""

    @patch(
        "recidiviz.persistence.entity.entities_bq_schema.entities_module_context_for_module",
        return_value=FakeEntitiesModuleContext(),
    )
    def test_bq_schema_for_entities_module(
        self, _entities_module_mock: MagicMock
    ) -> None:
        expected_schema = {
            "fake_another_entity": [
                SchemaField("state_code", "STRING", "NULLABLE"),
                SchemaField("another_entity_id", "INTEGER", "NULLABLE"),
                SchemaField("another_name", "STRING", "NULLABLE"),
                SchemaField(
                    "fake_person_id",
                    "INTEGER",
                    "NULLABLE",
                    description="Foreign key reference to fake_person",
                ),
            ],
            "fake_another_entity_fake_entity_association": [
                SchemaField("state_code", "STRING", "NULLABLE"),
                SchemaField("fake_another_entity_id", "INTEGER", "NULLABLE"),
                SchemaField("fake_entity_id", "INTEGER", "NULLABLE"),
            ],
            "fake_entity": [
                SchemaField("state_code", "STRING", "NULLABLE"),
                SchemaField("entity_id", "INTEGER", "NULLABLE"),
                SchemaField("name", "STRING", "NULLABLE"),
                SchemaField(
                    "fake_person_id",
                    "INTEGER",
                    "NULLABLE",
                    description="Foreign key reference to fake_person",
                ),
            ],
            "fake_person": [
                SchemaField("state_code", "STRING", "NULLABLE"),
                SchemaField("fake_person_id", "INTEGER", "NULLABLE"),
                SchemaField("full_name", "STRING", "NULLABLE"),
            ],
            "fake_person_external_id": [
                SchemaField("external_id", "STRING", "NULLABLE"),
                SchemaField("id_type", "STRING", "NULLABLE"),
                SchemaField("id_active_from_datetime", "DATETIME", "NULLABLE"),
                SchemaField("id_active_to_datetime", "DATETIME", "NULLABLE"),
                SchemaField("is_current_display_id_for_type", "BOOLEAN", "NULLABLE"),
                SchemaField("is_stable_id_for_type", "BOOLEAN", "NULLABLE"),
                SchemaField("state_code", "STRING", "NULLABLE"),
                SchemaField("fake_person_external_id_id", "INTEGER", "NULLABLE"),
                SchemaField(
                    "fake_person_id",
                    "INTEGER",
                    "NULLABLE",
                    description="Foreign key reference to fake_person",
                ),
            ],
        }
        schema = get_bq_schema_for_entities_module(fake_entities)
        self.assertEqual(expected_schema, schema)

    @patch(
        "recidiviz.persistence.entity.entities_bq_schema.entities_module_context_for_module",
        return_value=FakeEntitiesModuleContext(),
    )
    def test_get_bq_schema_for_entity_table(
        self, _entities_module_mock: MagicMock
    ) -> None:
        self.assertEqual(
            [
                SchemaField("state_code", "STRING", "NULLABLE"),
                SchemaField("another_entity_id", "INTEGER", "NULLABLE"),
                SchemaField("another_name", "STRING", "NULLABLE"),
                SchemaField(
                    "fake_person_id",
                    "INTEGER",
                    "NULLABLE",
                    description="Foreign key reference to fake_person",
                ),
            ],
            get_bq_schema_for_entity_table(fake_entities, "fake_another_entity"),
        )
        self.assertEqual(
            [
                SchemaField("state_code", "STRING", "NULLABLE"),
                SchemaField("fake_person_id", "INTEGER", "NULLABLE"),
                SchemaField("full_name", "STRING", "NULLABLE"),
            ],
            get_bq_schema_for_entity_table(fake_entities, "fake_person"),
        )

    def test_string_typed_foreign_key(self) -> None:
        """Tests that a child entity's FK column to a root entity with a
        string-typed primary key is generated as STRING (not the
        previously-hardcoded INTEGER).
        """
        schema = get_bq_schema_for_entity_table(
            identity_cluster_entities, "identity_cluster_name"
        )
        fk_field = one(f for f in schema if f.name == "identity_cluster_id")
        self.assertEqual("STRING", fk_field.field_type)
        self.assertEqual("NULLABLE", fk_field.mode)
        self.assertEqual(
            "Foreign key reference to identity_cluster", fk_field.description
        )

    def test_string_typed_primary_key(self) -> None:
        """Tests that a root entity's string-typed primary key column is
        emitted as STRING.
        """
        schema = get_bq_schema_for_entity_table(
            identity_cluster_entities, "identity_cluster"
        )
        pk_field = one(f for f in schema if f.name == "identity_cluster_id")
        self.assertEqual("STRING", pk_field.field_type)
        self.assertEqual("NULLABLE", pk_field.mode)

    def test_identity_fragment_tables_all_join_to_root(self) -> None:
        """Every fragment table carries a STRING identity_fragment_id column:
        the root table as its primary key, and every child table as an FK to
        the root, even for entities that reach the root only through the
        intermediate IdentityAttributes. No table carries an id column for
        that keyless intermediate.
        """
        schema = get_bq_schema_for_entities_module(identity_fragment_entities)
        for table_id, fields in schema.items():
            with self.subTest(table_id=table_id):
                id_field = one(f for f in fields if f.name == "identity_fragment_id")
                self.assertEqual("STRING", id_field.field_type)
                self.assertEqual("NULLABLE", id_field.mode)
                if table_id != "identity_fragment":
                    self.assertEqual(
                        "Foreign key reference to identity_fragment",
                        id_field.description,
                    )
                self.assertFalse(
                    any(f.name == "identity_attributes_id" for f in fields)
                )
