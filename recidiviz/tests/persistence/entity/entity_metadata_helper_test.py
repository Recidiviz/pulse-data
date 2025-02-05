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
"""Tests for entity_metadata_helper.py."""
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.persistence.entity.entity_metadata_helper import (
    AssociationTableMetadataHelper,
    EntityMetadataHelper,
)
from recidiviz.tests.persistence.entity import fake_entities


class TestEntityMetadataHelper(unittest.TestCase):
    """Tests for EntityMetadataHelper."""

    def test_for_table_id(self) -> None:
        metadata = EntityMetadataHelper.for_table_id(
            entities_module=fake_entities, table_id="fake_another_entity"
        )

        self.assertEqual(
            metadata.primary_key,
            fake_entities.FakeAnotherEntity.get_primary_key_column_name(),
        )
        self.assertEqual(
            metadata.root_entity_primary_key,
            fake_entities.FakePerson.get_primary_key_column_name(),
        )


class TestAssociationTableMetadataHelper(unittest.TestCase):
    """Tests for AssociationTableMetadataHelper."""

    @patch(
        "recidiviz.persistence.entity.entity_metadata_helper.get_entities_by_association_table_id",
        return_value=(fake_entities.FakeAnotherEntity, fake_entities.FakeEntity),
    )
    def test_for_table_id(self, _mock_get_entities: MagicMock) -> None:
        metadata = AssociationTableMetadataHelper.for_table_id(
            entities_module=fake_entities,
            table_id="fake_another_entity_fake_entity_association",
        )

        entity_cls_a_id, entity_cls_b_id = metadata.associated_entities_class_ids
        self.assertEqual(
            entity_cls_a_id, fake_entities.FakeAnotherEntity.get_class_id_name()
        )
        self.assertEqual(entity_cls_b_id, fake_entities.FakeEntity.get_class_id_name())
