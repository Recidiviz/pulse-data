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
"""Tests for schema_edge_direction_checker.py"""

import unittest

from recidiviz.persistence.entity.entities_module_context_factory import (
    ENTITIES_MODULE_CONTEXT_SUPPORTED_MODULES,
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import get_all_entity_classes_in_module
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state.entities import (
    StateIncarcerationSentence,
    StatePerson,
    StateSupervisionSentence,
    StateSupervisionViolation,
)


class TestSchemaEdgeDirectionChecker(unittest.TestCase):
    """Tests for schema_edge_direction_checker.py"""

    def test_schemaEdgeDirectionChecker_isHigherRanked_higherRank(self) -> None:
        entities_module_context = entities_module_context_for_module(state_entities)
        direction_checker = entities_module_context.direction_checker()
        self.assertTrue(
            direction_checker.is_higher_ranked(StatePerson, StateIncarcerationSentence)
        )
        self.assertTrue(
            direction_checker.is_higher_ranked(StatePerson, StateSupervisionViolation)
        )

    def test_schemaEdgeDirectionChecker_isHigherRanked_lowerRank(self) -> None:
        entities_module_context = entities_module_context_for_module(state_entities)
        direction_checker = entities_module_context.direction_checker()
        self.assertFalse(
            direction_checker.is_higher_ranked(StateSupervisionSentence, StatePerson)
        )
        self.assertFalse(
            direction_checker.is_higher_ranked(StateSupervisionViolation, StatePerson)
        )

    def test_schemaEdgeDirectionChecker_isHigherRanked_sameRank(self) -> None:
        entities_module_context = entities_module_context_for_module(state_entities)
        direction_checker = entities_module_context.direction_checker()
        self.assertFalse(direction_checker.is_higher_ranked(StatePerson, StatePerson))
        self.assertFalse(
            direction_checker.is_higher_ranked(
                StateSupervisionViolation, StateSupervisionViolation
            )
        )

    def test_schemaEdgeDirectionChecker_covers_all_entities_for_supported_schemas(
        self,
    ) -> None:
        for entities_module in ENTITIES_MODULE_CONTEXT_SUPPORTED_MODULES:
            entity_classes = get_all_entity_classes_in_module(entities_module)
            entities_module_context = entities_module_context_for_module(
                entities_module
            )
            direction_checker = entities_module_context.direction_checker()
            for entity_cls_a in entity_classes:
                for entity_cls_b in entity_classes:
                    if entity_cls_a == entity_cls_b:
                        continue
                    # If this doesn't crash, the class is covered
                    _ = direction_checker.is_higher_ranked(entity_cls_a, entity_cls_b)
