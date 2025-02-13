# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for entity_field_builders.py."""
import unittest
from unittest.mock import MagicMock

from google.cloud import bigquery

from recidiviz.looker.lookml_field_factory import LookMLFieldFactory
from recidiviz.persistence.entity.entity_metadata_helper import (
    AssociationTableMetadataHelper,
    EntityMetadataHelper,
)
from recidiviz.tools.looker.entity.entity_field_builders import (
    AssociationTableLookMLFieldBuilder,
    EntityLookMLFieldBuilder,
)
from recidiviz.tools.looker.state.state_dataset_custom_view_fields import (
    StateEntityLookMLCustomFieldProvider,
)


class TestEntityLookMLFieldBuilder(unittest.TestCase):
    """Tests for EntityLookMLFieldBuilder."""

    def setUp(self) -> None:
        self.metadata = MagicMock(spec=EntityMetadataHelper)
        self.metadata.primary_key = "id"
        self.metadata.root_entity_primary_key = "root_id"
        self.schema_fields = [
            bigquery.SchemaField("id", "STRING"),
            bigquery.SchemaField("name", "STRING"),
            bigquery.SchemaField("root_id", "STRING"),
        ]
        self.custom_field_provider = MagicMock(
            spec=StateEntityLookMLCustomFieldProvider
        )
        self.count_measure = LookMLFieldFactory.count_measure(drill_fields=["id"])
        self.custom_field_provider.get.return_value = [self.count_measure]
        self.table_id = "fake_entity"

        self.builder = EntityLookMLFieldBuilder(
            metadata=self.metadata,
            custom_field_provider=self.custom_field_provider,
            table_id=self.table_id,
            schema_fields=self.schema_fields,
        )

    def test_build_view_fields(self) -> None:
        fields = self.builder.build_view_fields()
        self.assertEqual(len(fields), 4)
        self.assertEqual(fields[-1], self.count_measure)
        self.assertEqual(fields[0].field_name, "id")
        self.assertEqual(fields[1].field_name, "name")
        self.assertEqual(fields[2].field_name, "root_id")


class TestAssociationTableLookMLFieldBuilder(unittest.TestCase):
    """Tests for AssociationTableLookMLFieldBuilder."""

    def setUp(self) -> None:
        self.metadata = MagicMock(spec=AssociationTableMetadataHelper)
        self.metadata.associated_entities_class_ids = ("entity_a_id", "entity_b_id")
        self.schema_fields = [
            bigquery.SchemaField("entity_a_id", "STRING"),
            bigquery.SchemaField("entity_b_id", "STRING"),
        ]
        self.custom_field_provider = MagicMock(
            spec=StateEntityLookMLCustomFieldProvider
        )
        self.count_measure = LookMLFieldFactory.count_measure(drill_fields=["id"])
        self.custom_field_provider.get.return_value = [self.count_measure]
        self.table_id = "fake_entity"

        self.builder = AssociationTableLookMLFieldBuilder(
            metadata=self.metadata,
            custom_field_provider=self.custom_field_provider,
            table_id=self.table_id,
            schema_fields=self.schema_fields,
        )

    def test_build_view_fields(self) -> None:
        fields = self.builder.build_view_fields()
        self.assertEqual(len(fields), 4)
        self.assertEqual(fields[-1], self.count_measure)
        self.assertEqual(fields[0].field_name, "entity_a_id")
        self.assertEqual(fields[1].field_name, "entity_b_id")
        self.assertEqual(fields[2].field_name, "primary_key")
