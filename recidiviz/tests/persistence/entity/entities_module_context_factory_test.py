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
"""Tests for entities_module_context_factory.py"""
import unittest
from functools import cmp_to_key

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity_class,
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import (
    get_all_entity_classes_in_module,
    get_entity_class_in_module_with_name,
)
from recidiviz.persistence.entity.operations import entities as operations_entities
from recidiviz.persistence.entity.operations.entities import (
    DirectIngestDataflowRawTableUpperBounds,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.entities import StatePerson
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateAssessment,
    NormalizedStateSentence,
)
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.utils.types import assert_subclass


class EntitiesModuleContextFactoryTest(unittest.TestCase):
    """Tests for entities_module_context_factory.py"""

    def test_normalized_and_state_have_same_class_hierarchy(self) -> None:
        """If this test fails the class_hierarchy() definitions in
        _StateEntitiesModuleContext and _NormalizedStateEntitiesModuleContext are not
        sorted in the same way for equivalent classes.
        """
        equivalent_state_classes_list = []
        normalized_state_direction_checker = entities_module_context_for_module(
            normalized_entities
        ).direction_checker()

        def sort_fn(a: type[Entity], b: type[Entity]) -> int:
            return (
                -1
                if not normalized_state_direction_checker.is_higher_ranked(a, b)
                else 1
            )

        sorted_normalized_by_rank = sorted(
            get_all_entity_classes_in_module(normalized_state_direction_checker),
            key=cmp_to_key(sort_fn),
        )
        for normalized_entity_cls in sorted_normalized_by_rank:
            equivalent_state_class = get_entity_class_in_module_with_name(
                state_entities,
                assert_subclass(
                    normalized_entity_cls, NormalizedStateEntity
                ).base_class_name(),
            )
            if not equivalent_state_class:
                continue
            equivalent_state_classes_list.append(equivalent_state_class)
        state_direction_checker = entities_module_context_for_module(
            state_entities
        ).direction_checker()
        state_direction_checker.assert_sorted(equivalent_state_classes_list)

    def test_entities_module_context_for_entity_class(self) -> None:
        self.assertEqual(
            normalized_entities,
            entities_module_context_for_entity_class(
                NormalizedStateSentence
            ).entities_module(),
        )
        self.assertEqual(
            normalized_entities,
            entities_module_context_for_entity_class(
                NormalizedStateAssessment
            ).entities_module(),
        )
        self.assertEqual(
            state_entities,
            entities_module_context_for_entity_class(StatePerson).entities_module(),
        )

        self.assertEqual(
            operations_entities,
            entities_module_context_for_entity_class(
                DirectIngestDataflowRawTableUpperBounds
            ).entities_module(),
        )
