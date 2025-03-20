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
"""Tests for recidiviz.tools.looker.entity.entity_custom_view_manager"""
import unittest
from types import ModuleType

from more_itertools import one

from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.tools.looker.entity.custom_views.person_periods import (
    PersonPeriodsJoinProvider,
    PersonPeriodsLookMLViewBuilder,
)
from recidiviz.tools.looker.entity.entity_custom_view_manager import (
    get_entity_custom_view_manager,
)


class TestEntityCustomViewManager(unittest.TestCase):
    """Tests for EntityCustomViewManager"""

    def setUp(self) -> None:
        self.dataset_id = "test_dataset"

    def test_get_custom_view_manager_with_state_entities(self) -> None:
        manager = get_entity_custom_view_manager(self.dataset_id, state_entities)

        self.assertEqual(manager.dataset_id, self.dataset_id)
        self.assertEqual(manager.view_builders, [PersonPeriodsLookMLViewBuilder])
        self.assertIsInstance(one(manager.join_providers), PersonPeriodsJoinProvider)

    def test_get_custom_view_manager_with_normalized_entities(self) -> None:
        manager = get_entity_custom_view_manager(self.dataset_id, normalized_entities)

        self.assertEqual(manager.dataset_id, self.dataset_id)
        self.assertEqual(manager.view_builders, [PersonPeriodsLookMLViewBuilder])
        self.assertIsInstance(one(manager.join_providers), PersonPeriodsJoinProvider)

    def test_get_custom_view_manager_with_unsupported_entities_module(self) -> None:
        with self.assertRaises(ValueError) as context:
            get_entity_custom_view_manager(
                self.dataset_id, ModuleType("unsupported_module")
            )
        self.assertEqual(
            str(context.exception),
            "Unsupported entities module [unsupported_module]",
        )
