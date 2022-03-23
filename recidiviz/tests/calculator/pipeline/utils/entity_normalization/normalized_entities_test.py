# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for normalized_entities.py"""
import unittest
from typing import List, Set, Type

import attr

from recidiviz.calculator.pipeline.utils.entity_normalization import normalized_entities
from recidiviz.calculator.pipeline.utils.entity_normalization.entity_normalization_manager_utils import (
    NORMALIZATION_MANAGERS,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entities import (
    NormalizedStateEntity,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionPeriod,
    NormalizedStateSupervisionViolationResponse,
    get_entity_class_names_excluded_from_normalization,
)
from recidiviz.calculator.pipeline.utils.entity_normalization.normalized_entity_conversion_utils import (
    fields_unique_to_normalized_class,
)
from recidiviz.calculator.pipeline.utils.execution_utils import (
    entity_class_can_be_hydrated_in_pipelines,
)
from recidiviz.common.attr_mixins import attr_field_referenced_cls_name_for_field_name
from recidiviz.common.attr_utils import is_flat_field
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import (
    get_all_entity_classes_in_module,
    get_entity_class_in_module_with_name,
)
from recidiviz.persistence.entity.state import entities as state_entities

NORMALIZED_PREFIX = "Normalized"
STATE_CODE = "US_XX"


class TestNormalizedEntities(unittest.TestCase):
    """Tests the classes defined in normalized_entities.py"""

    def setUp(self) -> None:
        self.normalized_entity_classes: List[Type[NormalizedStateEntity]] = [
            entity_class
            for entity_class in get_all_entity_classes_in_module(normalized_entities)
            if issubclass(entity_class, NormalizedStateEntity)
        ]

        self.normalized_entity_bases = {
            entity_class.__base__ for entity_class in self.normalized_entity_classes
        }

    def test_normalized_class_naming(self):
        """Tests that the name of all normalized classes is 'Normalized' + the name
        of the base entity class being normalized."""
        for normalized_entity_class in self.normalized_entity_classes:
            entity_name = str(normalized_entity_class.__name__)
            self.assertTrue(NORMALIZED_PREFIX in entity_name)
            self.assertEqual(
                normalized_entity_class.__base__.__name__,
                entity_name[len(NORMALIZED_PREFIX) :],
            )
            self.assertEqual(
                normalized_entity_class.__base__.__name__,
                entity_name.replace(NORMALIZED_PREFIX, ""),
            )

    def test_subtree_coverage(self):
        """Tests that all entities in the subtrees of the root entities that get
        normalized have NormalizedStateEntity counterparts."""
        classes_in_subtrees: Set[Type[Entity]] = set()

        for normalization_manager in NORMALIZATION_MANAGERS:
            for entity in normalization_manager.normalized_entity_classes():
                classes_in_subtrees.update(classes_in_normalized_entity_subtree(entity))

        self.assertEqual(self.normalized_entity_bases, classes_in_subtrees)

    def test_not_normalized_entity_in_ref(self):
        # This should raise an error because we are trying to store a
        # StateSupervisionViolation in the supervision_violation field instead of a
        # NormalizedStateSupervisionViolation
        with self.assertRaises(TypeError) as e:
            self.normalized_sp = NormalizedStateSupervisionViolationResponse(
                state_code=STATE_CODE,
                sequence_num=1,
                supervision_violation=state_entities.StateSupervisionViolation(
                    state_code=STATE_CODE,
                ),
            )

        expected_error = (
            "The supervision_violation field on the "
            "NormalizedStateSupervisionViolationResponse class must store the "
            "Normalized version of the entity. Found: StateSupervisionViolation."
        )

        self.assertEqual(expected_error, e.exception.args[0])

    def test_not_normalized_entity_in_list_ref(self):
        # This should raise an error because we are trying to store a
        # StateSupervisionCaseTypeEntry in the case_type_entries field instead of a
        # NormalizedStateSupervisionCaseTypeEntry
        with self.assertRaises(TypeError) as e:
            self.normalized_sp = NormalizedStateSupervisionPeriod(
                state_code=STATE_CODE,
                sequence_num=1,
                case_type_entries=[
                    state_entities.StateSupervisionCaseTypeEntry(state_code=STATE_CODE)
                ],
            )

        expected_error = (
            "The case_type_entries field on the "
            "NormalizedStateSupervisionPeriod class must store the "
            "Normalized version of the entity. Found: StateSupervisionCaseTypeEntry."
        )

        self.assertEqual(expected_error, e.exception.args[0])

    def test_not_list_in_list_ref(self):
        """Tests that the original validators are also kept on the attributes,
        so that this raises an error for case_type_entries not being a list."""
        with self.assertRaises(TypeError):
            self.normalized_sp = NormalizedStateSupervisionPeriod(
                state_code=STATE_CODE,
                sequence_num=1,
                case_type_entries=NormalizedStateSupervisionCaseTypeEntry(
                    state_code=STATE_CODE
                ),
            )

    def test_ref_is_unset(self):
        # Assert that this does not fail when case_type_entries is unset
        _ = NormalizedStateSupervisionPeriod(state_code=STATE_CODE, sequence_num=1)

    def test_new_fields_are_all_flat_fields(self):
        """Tests that all attributes added to NormalizedStateEntity classes are flat
        fields."""
        for entity_cls in self.normalized_entity_classes:
            unique_fields = fields_unique_to_normalized_class(entity_cls)

            for field, attribute in attr.fields_dict(entity_cls).items():
                if field not in unique_fields:
                    continue

                if not is_flat_field(attribute):
                    raise ValueError(
                        "Only flat fields are supported as additional fields on "
                        f"NormalizedStateEntities. Found: {attribute} in field "
                        f"{field}."
                    )


def classes_in_normalized_entity_subtree(
    entity_cls: Type[Entity],
) -> Set[Type[Entity]]:
    """Returns all classes in the subtree of the given entity class that could
    potentially be impacted by normalization. A subtree is defined as any entity that
    can be reached from a given entity without traversing an edge to StatePerson.

    Excludes any classes that cannot be hydrated in calculation pipelines,
    since these classes cannot be impacted by normalization.
    """
    explored_nodes: Set[Type[Entity]] = set()
    unexplored_nodes: Set[Type[Entity]] = {entity_cls}

    # If this entity cannot be hydrated in calculation pipelines, then it does not
    # have a subtree of related entities that could be normalized as the root entity
    if not entity_class_can_be_hydrated_in_pipelines(entity_cls):
        raise ValueError(
            f"Entity {entity_cls.__name__} cannot be hydrated in "
            f"calculation pipelines, so cannot be a member of a "
            f"normalized entity subtree."
        )

    class_names_to_ignore = get_entity_class_names_excluded_from_normalization()

    while unexplored_nodes:
        node_entity_class = unexplored_nodes.pop()
        for field in attr.fields_dict(node_entity_class):
            related_class_name = attr_field_referenced_cls_name_for_field_name(
                node_entity_class, field
            )
            if not related_class_name or related_class_name in class_names_to_ignore:
                continue

            related_class = get_entity_class_in_module_with_name(
                state_entities, related_class_name
            )

            if related_class not in explored_nodes:
                unexplored_nodes.add(related_class)

        explored_nodes.add(node_entity_class)

    return explored_nodes


class TestClassesInNormalizedEntitySubtree(unittest.TestCase):
    """Tests the classes_in_normalized_entity_subtree helper function."""

    def test_classes_in_normalized_entity_subtree(self):
        entity_class = state_entities.StateSupervisionSentence

        # StateSupervisionSentence can be connected to any of the following without
        # traversing a StatePerson edge
        expected_subtree = {
            state_entities.StateSupervisionSentence,
            state_entities.StateCharge,
            state_entities.StateCourtCase,
            state_entities.StateEarlyDischarge,
            state_entities.StateIncarcerationSentence,
        }

        self.assertEqual(
            expected_subtree, classes_in_normalized_entity_subtree(entity_class)
        )

    def test_classes_in_normalized_entity_subtree_leaf_node(self):
        entity_class = state_entities.StateIncarcerationPeriod

        # StateIncarcerationPeriod is a leaf node
        expected_subtree = {state_entities.StateIncarcerationPeriod}

        self.assertEqual(
            expected_subtree, classes_in_normalized_entity_subtree(entity_class)
        )

    def test_classes_in_normalized_entity_subtree_cannot_be_hydrated(self):
        # StateAgent is not a valid entity for a normalized subtree
        entity_class = state_entities.StateAgent

        with self.assertRaises(ValueError):
            _ = classes_in_normalized_entity_subtree(entity_class)
