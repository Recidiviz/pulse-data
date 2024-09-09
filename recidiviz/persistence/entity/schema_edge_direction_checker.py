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
"""Defines a utility class that can be used determine whether relationships between two
objects are forward or back edges.
"""
from types import ModuleType
from typing import Dict, List, Sequence, Type

import attr

from recidiviz.common.attr_utils import get_non_flat_attribute_class_name, is_flat_field
from recidiviz.common.common_utils import pairwise
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.core_entity import CoreEntity
from recidiviz.persistence.entity.operations import entities as operations_entities
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.errors import PersistenceError

_STATE_CLASS_HIERARCHY = [
    # StatePerson hierarchy
    state_entities.StatePerson.__name__,
    state_entities.StatePersonExternalId.__name__,
    state_entities.StatePersonAddressPeriod.__name__,
    state_entities.StatePersonHousingStatusPeriod.__name__,
    state_entities.StatePersonAlias.__name__,
    state_entities.StatePersonRace.__name__,
    state_entities.StatePersonEthnicity.__name__,
    state_entities.StateIncarcerationSentence.__name__,
    state_entities.StateSupervisionSentence.__name__,
    state_entities.StateCharge.__name__,
    state_entities.StateIncarcerationPeriod.__name__,
    state_entities.StateIncarcerationIncident.__name__,
    state_entities.StateIncarcerationIncidentOutcome.__name__,
    state_entities.StateSupervisionPeriod.__name__,
    state_entities.StateSupervisionContact.__name__,
    state_entities.StateSupervisionCaseTypeEntry.__name__,
    state_entities.StateSupervisionViolation.__name__,
    state_entities.StateSupervisionViolatedConditionEntry.__name__,
    state_entities.StateSupervisionViolationTypeEntry.__name__,
    state_entities.StateSupervisionViolationResponse.__name__,
    state_entities.StateSupervisionViolationResponseDecisionEntry.__name__,
    state_entities.StateAssessment.__name__,
    state_entities.StateProgramAssignment.__name__,
    state_entities.StateEarlyDischarge.__name__,
    state_entities.StateEmploymentPeriod.__name__,
    state_entities.StateDrugScreen.__name__,
    state_entities.StateTaskDeadline.__name__,
    state_entities.StateSentence.__name__,
    state_entities.StateSentenceServingPeriod.__name__,
    # TODO(#26240): Replace StateCharge with this entity
    state_entities.StateChargeV2.__name__,
    state_entities.StateSentenceStatusSnapshot.__name__,
    state_entities.StateSentenceLength.__name__,
    state_entities.StateSentenceGroup.__name__,
    state_entities.StateSentenceGroupLength.__name__,
    # StateStaff hierarchy
    state_entities.StateStaff.__name__,
    state_entities.StateStaffExternalId.__name__,
    state_entities.StateStaffRolePeriod.__name__,
    state_entities.StateStaffSupervisorPeriod.__name__,
    state_entities.StateStaffLocationPeriod.__name__,
    state_entities.StateStaffCaseloadTypePeriod.__name__,
]


_NORMALIZED_STATE_CLASS_HIERARCHY = [
    # NormalizedStatePerson hierarchy
    normalized_entities.NormalizedStatePerson.__name__,
    normalized_entities.NormalizedStatePersonExternalId.__name__,
    normalized_entities.NormalizedStatePersonAddressPeriod.__name__,
    normalized_entities.NormalizedStatePersonHousingStatusPeriod.__name__,
    normalized_entities.NormalizedStatePersonAlias.__name__,
    normalized_entities.NormalizedStatePersonRace.__name__,
    normalized_entities.NormalizedStatePersonEthnicity.__name__,
    normalized_entities.NormalizedStateIncarcerationSentence.__name__,
    normalized_entities.NormalizedStateSupervisionSentence.__name__,
    normalized_entities.NormalizedStateCharge.__name__,
    normalized_entities.NormalizedStateIncarcerationPeriod.__name__,
    normalized_entities.NormalizedStateIncarcerationIncident.__name__,
    normalized_entities.NormalizedStateIncarcerationIncidentOutcome.__name__,
    normalized_entities.NormalizedStateSupervisionPeriod.__name__,
    normalized_entities.NormalizedStateSupervisionContact.__name__,
    normalized_entities.NormalizedStateSupervisionCaseTypeEntry.__name__,
    normalized_entities.NormalizedStateSupervisionViolation.__name__,
    normalized_entities.NormalizedStateSupervisionViolatedConditionEntry.__name__,
    normalized_entities.NormalizedStateSupervisionViolationTypeEntry.__name__,
    normalized_entities.NormalizedStateSupervisionViolationResponse.__name__,
    normalized_entities.NormalizedStateSupervisionViolationResponseDecisionEntry.__name__,
    normalized_entities.NormalizedStateAssessment.__name__,
    normalized_entities.NormalizedStateProgramAssignment.__name__,
    normalized_entities.NormalizedStateEarlyDischarge.__name__,
    normalized_entities.NormalizedStateEmploymentPeriod.__name__,
    normalized_entities.NormalizedStateDrugScreen.__name__,
    normalized_entities.NormalizedStateTaskDeadline.__name__,
    normalized_entities.NormalizedStateSentence.__name__,
    normalized_entities.NormalizedStateSentenceServingPeriod.__name__,
    # TODO(#26240): Replace NormalizedStateCharge with this entity
    normalized_entities.NormalizedStateChargeV2.__name__,
    normalized_entities.NormalizedStateSentenceStatusSnapshot.__name__,
    normalized_entities.NormalizedStateSentenceLength.__name__,
    normalized_entities.NormalizedStateSentenceInferredGroup.__name__,
    normalized_entities.NormalizedStateSentenceGroup.__name__,
    normalized_entities.NormalizedStateSentenceGroupLength.__name__,
    # StateStaff hierarchy
    normalized_entities.NormalizedStateStaff.__name__,
    normalized_entities.NormalizedStateStaffExternalId.__name__,
    normalized_entities.NormalizedStateStaffRolePeriod.__name__,
    normalized_entities.NormalizedStateStaffSupervisorPeriod.__name__,
    normalized_entities.NormalizedStateStaffLocationPeriod.__name__,
    normalized_entities.NormalizedStateStaffCaseloadTypePeriod.__name__,
]

_OPERATIONS_CLASS_HIERARCHY = [
    # RawFileMetadata Hierarchy
    operations_entities.DirectIngestRawFileImportRun.__name__,
    operations_entities.DirectIngestRawFileImport.__name__,
    operations_entities.DirectIngestRawBigQueryFileMetadata.__name__,
    operations_entities.DirectIngestRawGCSFileMetadata.__name__,
    # DataflowMetadata Hierarchy
    operations_entities.DirectIngestDataflowJob.__name__,
    operations_entities.DirectIngestDataflowRawTableUpperBounds.__name__,
    # Classes w/o Relationships here to satifsy includes all classes
    operations_entities.DirectIngestRawFileMetadata.__name__,
    operations_entities.DirectIngestInstanceStatus.__name__,
    operations_entities.DirectIngestRawDataResourceLock.__name__,
    operations_entities.DirectIngestSftpIngestReadyFileMetadata.__name__,
    operations_entities.DirectIngestSftpRemoteFileMetadata.__name__,
    operations_entities.DirectIngestRawDataFlashStatus.__name__,
]


def _build_class_hierarchy_map(class_hierarchy: List[str]) -> Dict[str, int]:
    """Returns a map of class names with their associated rank in the schema
    graph ordering.

    Args:
        class_hierarchy: A list of class names, ordered by rank in the
            schema graph ordering.
    Returns:
        A map of class names with their associated rank in the schema graph
        ordering. Lower number means closer to the root of the graph.
    """
    return {class_name: i for i, class_name in enumerate(class_hierarchy)}


def _get_non_flat_property_class_name(
    entity_cls: Type[Entity], property_name: str
) -> str | None:
    """Returns the class name of the property with |property_name| on obj, or
    None if the property is a flat field.
    """
    if not issubclass(entity_cls, Entity):
        raise TypeError(f"Unexpected type [{entity_cls}]")

    if _is_property_flat_field(entity_cls, property_name):
        return None

    attribute = attr.fields_dict(entity_cls).get(property_name)  # type: ignore[arg-type]
    if not attribute:
        return None

    property_class_name = get_non_flat_attribute_class_name(attribute)

    if not property_class_name:
        raise ValueError(
            f"Non-flat field [{property_name}] on class [{entity_cls}] should "
            f"either correspond to list or union. Found: [{property_class_name}]"
        )
    return property_class_name


# TODO(#1886): We should not consider objects which are not ForwardRefs, but are properly typed to an entity cls
#  as a flat field
def _is_property_flat_field(entity_cls: Type[Entity], property_name: str) -> bool:
    """Returns true if the attribute corresponding to |property_name| on the
    given object is a flat field (not a List, attr class, or ForwardRef)."""

    if not issubclass(entity_cls, Entity):
        raise TypeError(f"Unexpected type [{entity_cls}]")

    attribute = attr.fields_dict(entity_cls).get(property_name)  # type: ignore[arg-type]

    if not attribute:
        raise ValueError(
            f"Unexpected None attribute for property_name [{property_name}] on class [{entity_cls}]"
        )

    return is_flat_field(attribute)


class SchemaEdgeDirectionChecker:
    """A utility class to determine whether relationships between two objects
    are forward or back edges"""

    def __init__(self, class_hierarchy: List[str]):
        self._class_hierarchy_map: Dict[str, int] = _build_class_hierarchy_map(
            class_hierarchy
        )

    def is_back_edge(self, from_cls: Type[CoreEntity], to_field_name: str) -> bool:
        """Given an entity type and a field name on that entity type, returns whether
        traversing from the class to an object in that field would be traveling
        along a 'back edge' in the object graph. A back edge is an edge that
        might introduce a cycle in the graph.
        Without back edges, the object graph should have no cycles.

        Args:
            from_cls: The class that is the origin of this edge
            to_field_name: A string field name for the field on from_cls
                containing the destination object of this edge
        Returns:
            True if a graph edge travelling from from_cls to an object in
                to_field_name is a back edge, i.e. it travels in a direction
                opposite to the class hierarchy.
        """
        from_class_name = from_cls.__name__

        if issubclass(from_cls, DatabaseEntity):
            to_class_name = from_cls.get_relationship_property_class_name(to_field_name)
        elif issubclass(from_cls, Entity):
            to_class_name = _get_non_flat_property_class_name(from_cls, to_field_name)
        else:
            raise ValueError(f"Unexpected type [{from_cls}]")

        if to_class_name is None:
            return False

        if from_class_name not in self._class_hierarchy_map:
            raise PersistenceError(
                f"Unable to convert: [{from_class_name}] not in the class "
                f"hierarchy map"
            )

        if to_class_name not in self._class_hierarchy_map:
            raise PersistenceError(
                f"Unable to convert: [{to_class_name}] not in the class "
                f"hierarchy map"
            )

        return (
            self._class_hierarchy_map[from_class_name]
            >= self._class_hierarchy_map[to_class_name]
        )

    def is_higher_ranked(
        self, cls_1: Type[CoreEntity], cls_2: Type[CoreEntity]
    ) -> bool:
        """Returns True if the provided |cls_1| has a higher rank than the
        provided |cls_2|.
        """
        type_1_name = cls_1.__name__
        type_2_name = cls_2.__name__

        return (
            self._class_hierarchy_map[type_1_name]
            < self._class_hierarchy_map[type_2_name]
        )

    def assert_sorted(self, entity_types: Sequence[Type[CoreEntity]]) -> None:
        """Throws if the input |entity_types| list is not in descending order
        based on class hierarchy.
        """
        for type_1, type_2 in pairwise(entity_types):
            if not self.is_higher_ranked(type_1, type_2):
                raise ValueError(
                    f"Unexpected ordering, found {type_1.__name__} before "
                    f"{type_2.__name__}"
                )


SCHEMA_EDGE_DIRECTION_CHECKER_SUPPORTED_MODULES = {
    state_entities,
    normalized_entities,
    operations_entities,
}

_direction_checkers_by_module: dict[ModuleType, SchemaEdgeDirectionChecker] = {}


def direction_checker_for_module(
    entities_module: ModuleType,
) -> SchemaEdgeDirectionChecker:
    if entities_module not in _direction_checkers_by_module:
        if entities_module is state_entities:
            checker = SchemaEdgeDirectionChecker(_STATE_CLASS_HIERARCHY)
        elif entities_module is normalized_entities:
            checker = SchemaEdgeDirectionChecker(_NORMALIZED_STATE_CLASS_HIERARCHY)
        elif entities_module is operations_entities:
            checker = SchemaEdgeDirectionChecker(_OPERATIONS_CLASS_HIERARCHY)
        else:
            raise ValueError(f"Unsupported module: [{entities_module}]")
        _direction_checkers_by_module[entities_module] = checker

    return _direction_checkers_by_module[entities_module]
