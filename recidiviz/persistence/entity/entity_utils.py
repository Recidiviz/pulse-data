# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Utils for working with Entity classes or various |entities| modules."""
import importlib
import inspect
import json
from collections import defaultdict
from enum import Enum, auto
from functools import cache, lru_cache
from io import TextIOWrapper
from types import ModuleType
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple, Type, Union, cast

import attr

from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attr_field_name_storing_referenced_cls_name,
    attr_field_referenced_cls_name_for_field_name,
    attr_field_type_for_field_name,
)
from recidiviz.common.attr_utils import (
    get_non_flat_attribute_class_name,
    is_flat_field,
    is_forward_ref,
    is_list,
)
from recidiviz.common.common_utils import pairwise
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.persistence.database.database_entity import DatabaseEntity
from recidiviz.persistence.database.schema.state import schema as state_schema
from recidiviz.persistence.database.schema_utils import (
    get_state_database_association_with_names,
)
from recidiviz.persistence.entity.base_entity import (
    Entity,
    EntityT,
    EnumEntity,
    ExternalIdEntity,
    HasExternalIdEntity,
    HasMultipleExternalIdsEntity,
    RootEntity,
)
from recidiviz.persistence.entity.core_entity import CoreEntity
from recidiviz.persistence.entity.entity_deserialize import EntityFactory
from recidiviz.persistence.entity.operations import entities as operations_entities
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.persistence.entity.state.state_entity_mixins import LedgerEntityMixin
from recidiviz.persistence.errors import PersistenceError
from recidiviz.utils.log_helpers import make_log_output_path
from recidiviz.utils.types import non_optional

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
    operations_entities.DirectIngestRawBigQueryFileMetadata.__name__,
    operations_entities.DirectIngestRawGCSFileMetadata.__name__,
    operations_entities.DirectIngestRawDataImportSession.__name__,
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

_state_direction_checker = None
_normalized_state_direction_checker = None
_operations_direction_checker = None


class SchemaEdgeDirectionChecker:
    """A utility class to determine whether relationships between two objects
    are forward or back edges"""

    def __init__(self, class_hierarchy: List[str], module: ModuleType):
        self._class_hierarchy_map: Dict[str, int] = _build_class_hierarchy_map(
            class_hierarchy, module
        )

    @classmethod
    def state_direction_checker(cls) -> "SchemaEdgeDirectionChecker":
        """Returns a direction checker that can be used to determine edge direction
        between classes in state/entities.py.
        """
        global _state_direction_checker
        if not _state_direction_checker:
            _state_direction_checker = cls(_STATE_CLASS_HIERARCHY, state_entities)
        return _state_direction_checker

    @classmethod
    def normalized_state_direction_checker(cls) -> "SchemaEdgeDirectionChecker":
        """Returns a direction checker that can be used to determine edge direction
        between classes in state/normalized_entities.py.
        """
        global _normalized_state_direction_checker
        if not _normalized_state_direction_checker:
            _normalized_state_direction_checker = cls(
                _NORMALIZED_STATE_CLASS_HIERARCHY, normalized_entities
            )
        return _normalized_state_direction_checker

    @classmethod
    def operations_direction_checker(cls) -> "SchemaEdgeDirectionChecker":
        global _operations_direction_checker
        if not _operations_direction_checker:
            _operations_direction_checker = cls(
                _OPERATIONS_CLASS_HIERARCHY, operations_entities
            )
        return _operations_direction_checker

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
            to_class_name = get_non_flat_property_class_name(from_cls, to_field_name)
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


def _build_class_hierarchy_map(
    class_hierarchy: List[str], entities_module: ModuleType
) -> Dict[str, int]:
    """Returns a map of class names with their associated rank in the schema
    graph ordering.

    Args:
        class_hierarchy: A list of class names, ordered by rank in the
            schema graph ordering.
    Returns:
        A map of class names with their associated rank in the schema graph
        ordering. Lower number means closer to the root of the graph.
    """
    _check_class_hierarchy_includes_all_expected_classes(
        class_hierarchy, entities_module
    )

    return {class_name: i for i, class_name in enumerate(class_hierarchy)}


def _check_class_hierarchy_includes_all_expected_classes(
    class_hierarchy: List[str], entities_module: ModuleType
) -> None:
    expected_class_names = get_all_entity_class_names_in_module(entities_module)

    given_minus_expected = set(class_hierarchy).difference(expected_class_names)
    expected_minus_given = expected_class_names.difference(class_hierarchy)

    if given_minus_expected or expected_minus_given:
        msg = ""
        if given_minus_expected:
            msg += (
                f"Found unexpected class in class hierarchy: "
                f"[{list(given_minus_expected)[0]}]. "
            )
        if expected_minus_given:
            msg += (
                f"Missing expected class in class hierarchy: "
                f"[{list(expected_minus_given)[0]}]. "
            )

        raise PersistenceError(msg)


@cache
def get_all_entity_classes_in_module(entities_module: ModuleType) -> Set[Type[Entity]]:
    """Returns a set of all subclasses of Entity that are
    defined in the given module."""
    expected_classes: Set[Type[Entity]] = set()
    for attribute_name in dir(entities_module):
        attribute = getattr(entities_module, attribute_name)
        if inspect.isclass(attribute):
            if attribute not in (
                Entity,
                HasExternalIdEntity,
                ExternalIdEntity,
                HasMultipleExternalIdsEntity,
                EnumEntity,
                LedgerEntityMixin,
            ) and issubclass(attribute, Entity):
                expected_classes.add(attribute)

    return expected_classes


def get_module_for_entity_class(entity_cls: Type[Entity]) -> ModuleType:
    if entity_cls in get_all_entity_classes_in_module(state_entities):
        return state_entities
    if entity_cls in get_all_entity_classes_in_module(normalized_entities):
        return normalized_entities
    raise ValueError(f"Unexpected entity_cls [{entity_cls}]")


def get_all_enum_classes_in_module(enums_module: ModuleType) -> Set[Type[Enum]]:
    """Returns a set of all subclasses of Enum that are defined in the given module."""
    enum_classes: Set[Type[Enum]] = set()
    for attribute_name in dir(enums_module):
        attribute = getattr(enums_module, attribute_name)
        if inspect.isclass(attribute):
            if (
                attribute is not Enum
                and attribute is not StateEntityEnum
                and issubclass(attribute, Enum)
            ):
                enum_classes.add(attribute)

    return enum_classes


def get_all_entity_factory_classes_in_module(
    factories_module: ModuleType,
) -> Set[Type[EntityFactory]]:
    """Returns a set of all subclasses of EntityFactory that are defined in the
    given module."""
    expected_classes: Set[Type[EntityFactory]] = set()
    for attribute_name in dir(factories_module):
        attribute = getattr(factories_module, attribute_name)
        if inspect.isclass(attribute):
            if attribute is not EntityFactory and issubclass(attribute, EntityFactory):
                expected_classes.add(attribute)

    return expected_classes


@cache
def get_entity_class_in_module_with_name(
    entities_module: ModuleType, class_name: str
) -> Type[Entity]:
    entity_classes = get_all_entity_classes_in_module(entities_module)

    for entity_class in entity_classes:
        if entity_class.__name__ == class_name:
            return entity_class

    raise LookupError(f"Entity class {class_name} does not exist in {entities_module}.")


@cache
def get_entity_class_in_module_with_table_id(
    entities_module: ModuleType, table_id: str
) -> Type[Entity]:
    entity_classes = get_all_entity_classes_in_module(entities_module)

    for entity_class in entity_classes:
        if entity_class.get_table_id() == table_id:
            return entity_class

    raise LookupError(
        f"Entity class with table_id {table_id} does not exist in {entities_module}."
    )


def get_all_entity_class_names_in_module(entities_module: ModuleType) -> Set[str]:
    """Returns a set of all names of subclasses of Entity that
    are defined in the given module."""
    return {cls_.__name__ for cls_ in get_all_entity_classes_in_module(entities_module)}


class EntityFieldType(Enum):
    FLAT_FIELD = auto()
    FOREIGN_KEYS = auto()
    FORWARD_EDGE = auto()
    BACK_EDGE = auto()
    ALL = auto()


class CoreEntityFieldIndex:
    """Class that caches the results of certain CoreEntity class introspection
    functionality.
    """

    def __init__(
        self,
        direction_checker: Optional[SchemaEdgeDirectionChecker] = None,
        database_entity_fields_by_field_type: Optional[
            Dict[str, Dict[EntityFieldType, Set[str]]]
        ] = None,
        entity_fields_by_field_type: Optional[
            Dict[str, Dict[EntityFieldType, Set[str]]]
        ] = None,
    ) -> None:
        self.direction_checker = (
            direction_checker or SchemaEdgeDirectionChecker.state_direction_checker()
        )

        # TODO(#24930): Cache these at the process level by SchemaType so that we always
        #  use a cached version where possible.
        # Cache of fields by field type for DatabaseEntity classes
        self.database_entity_fields_by_field_type: Dict[
            str, Dict[EntityFieldType, Set[str]]
        ] = (database_entity_fields_by_field_type or {})

        # Cache of fields by field type for Entity classes
        self.entity_fields_by_field_type: Dict[str, Dict[EntityFieldType, Set[str]]] = (
            entity_fields_by_field_type or {}
        )

    def get_fields_with_non_empty_values(
        self, entity: CoreEntity, entity_field_type: EntityFieldType
    ) -> Set[str]:
        """Returns a set of field_names that correspond to any non-empty (nonnull or
        non-empty list) fields on the provided |entity| that match the provided
        |entity_field_type|.

        Note: This function is relatively slow for Entity type entities and should not
        be called in a tight loop.
        """
        result = set()
        for field_name in self.get_all_core_entity_fields(
            type(entity), entity_field_type
        ):
            v = entity.get_field(field_name)
            if isinstance(v, list):
                if v:
                    result.add(field_name)
            elif v is not None:
                result.add(field_name)
        return result

    def get_all_core_entity_fields(
        self, entity_cls: Type[CoreEntity], entity_field_type: EntityFieldType
    ) -> Set[str]:
        """Returns a set of field_names that correspond to any fields (non-empty or
        otherwise) on the provided DatabaseEntity |entity| class that match the provided
        |entity_field_type|. Fields are included whether or not the values are non-empty
        on the provided object.

        This function caches the results for subsequent calls.
        """
        entity_name = entity_cls.get_entity_name()

        if issubclass(entity_cls, DatabaseEntity):
            if entity_name not in self.database_entity_fields_by_field_type:
                self.database_entity_fields_by_field_type[entity_name] = {}
            if (
                entity_field_type
                not in self.database_entity_fields_by_field_type[entity_name]
            ):
                self.database_entity_fields_by_field_type[entity_name][
                    entity_field_type
                ] = self._get_all_database_entity_fields_slow(
                    entity_cls, entity_field_type
                )
            return self.database_entity_fields_by_field_type[entity_name][
                entity_field_type
            ]

        if issubclass(entity_cls, Entity):
            if entity_name not in self.entity_fields_by_field_type:
                self.entity_fields_by_field_type[entity_name] = {}

            if entity_field_type not in self.entity_fields_by_field_type[entity_name]:
                self.entity_fields_by_field_type[entity_name][
                    entity_field_type
                ] = self._get_entity_fields_with_type_slow(
                    entity_cls, entity_field_type
                )
            return self.entity_fields_by_field_type[entity_name][entity_field_type]

        raise ValueError(f"Unexpected entity type: {entity_cls}")

    def _get_all_database_entity_fields_slow(
        self,
        entity_cls: Type[DatabaseEntity],
        entity_field_type: EntityFieldType,
    ) -> Set[str]:
        """Returns a set of field_names that correspond to any fields (non-empty or
        otherwise) on the provided DatabaseEntity type |entity| that match the provided
        |entity_field_type|.

        This function is relatively slow and the results should be cached across
        repeated calls.
        """
        back_edges = set()
        forward_edges = set()
        flat_fields = set()
        foreign_keys = set()

        for relationship_field_name in entity_cls.get_relationship_property_names():
            if self.direction_checker.is_back_edge(entity_cls, relationship_field_name):
                back_edges.add(relationship_field_name)
            else:
                forward_edges.add(relationship_field_name)

        for foreign_key_name in entity_cls.get_foreign_key_names():
            foreign_keys.add(foreign_key_name)

        for column_field_name in entity_cls.get_column_property_names():
            if column_field_name not in foreign_keys:
                flat_fields.add(column_field_name)

        if entity_field_type is EntityFieldType.FLAT_FIELD:
            return flat_fields
        if entity_field_type is EntityFieldType.FOREIGN_KEYS:
            return foreign_keys
        if entity_field_type is EntityFieldType.FORWARD_EDGE:
            return forward_edges
        if entity_field_type is EntityFieldType.BACK_EDGE:
            return back_edges
        if entity_field_type is EntityFieldType.ALL:
            return flat_fields | foreign_keys | forward_edges | back_edges
        raise ValueError(
            f"Unrecognized EntityFieldType [{entity_field_type}] on entity type [{entity_cls}]"
        )

    def _get_entity_fields_with_type_slow(
        self, entity_cls: Type[Entity], entity_field_type: EntityFieldType
    ) -> Set[str]:
        """Returns a set of field_names that correspond to any fields (non-empty or
        otherwise) on the provided Entity type |entity| that match the provided
        |entity_field_type|.

        This function is relatively slow and the results should be cached across
        repeated calls.
        """
        back_edges = set()
        forward_edges = set()
        flat_fields = set()
        for field, attribute in attr.fields_dict(entity_cls).items():
            # TODO(#1908): Update traversal logic if relationship fields can be
            # different types aside from Entity and List
            if is_forward_ref(attribute) or is_list(attribute):
                if self.direction_checker.is_back_edge(entity_cls, field):
                    back_edges.add(field)
                else:
                    forward_edges.add(field)
            else:
                flat_fields.add(field)

        if entity_field_type is EntityFieldType.FLAT_FIELD:
            return flat_fields
        if entity_field_type is EntityFieldType.FOREIGN_KEYS:
            return set()  # Entity objects never have foreign keys
        if entity_field_type is EntityFieldType.FORWARD_EDGE:
            return forward_edges
        if entity_field_type is EntityFieldType.BACK_EDGE:
            return back_edges
        if entity_field_type is EntityFieldType.ALL:
            return flat_fields | forward_edges | back_edges
        raise ValueError(
            f"Unrecognized EntityFieldType {entity_field_type} on entity [{entity_cls}]"
        )


def is_reference_only_entity(
    entity: CoreEntity, field_index: CoreEntityFieldIndex
) -> bool:
    """Returns true if this object does not contain any meaningful information
    describing the entity, but instead only identifies the entity for reference
    purposes. Concretely, this means the object has an external_id but no other set
    fields (aside from default values).
    """
    set_flat_fields = get_explicitly_set_flat_fields(entity, field_index)
    if isinstance(entity, (state_schema.StatePerson, state_entities.StatePerson)):
        if set_flat_fields or any([entity.races, entity.aliases, entity.ethnicities]):
            return False
        return bool(entity.external_ids)

    if isinstance(entity, (state_schema.StateStaff, state_entities.StateStaff)):
        if set_flat_fields:
            return False
        return bool(entity.external_ids)

    return set_flat_fields == {"external_id"}


def get_explicitly_set_flat_fields(
    entity: CoreEntity, field_index: CoreEntityFieldIndex
) -> Set[str]:
    """Returns the set of field names for fields on the entity that have been set with
    non-default values. The "state_code" field is also excluded, as it is set with the
    same value on every entity for a given ingest run.
    """
    set_flat_fields = field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FLAT_FIELD
    )

    primary_key_name = entity.get_primary_key_column_name()
    if primary_key_name in set_flat_fields:
        set_flat_fields.remove(primary_key_name)

    # TODO(#2244): Change this to a general approach so we don't need to check
    # explicit columns
    if "state_code" in set_flat_fields:
        set_flat_fields.remove("state_code")

    default_enum_value_fields = {
        field_name
        for field_name in set_flat_fields
        if entity.is_default_enum(field_name)
    }

    set_flat_fields -= default_enum_value_fields

    if "incarceration_type" in set_flat_fields:
        if entity.is_default_enum(
            "incarceration_type", StateIncarcerationType.STATE_PRISON.value
        ):
            set_flat_fields.remove("incarceration_type")

    return set_flat_fields


def _sort_based_on_flat_fields(
    db_entities: Sequence[CoreEntity], field_index: CoreEntityFieldIndex
) -> None:
    """Helper function that sorts all entities in |db_entities| in place as
    well as all children of |db_entities|. Sorting is done by first by an
    external_id if that field exists, then present flat fields.
    """

    def _get_entity_sort_key(e: CoreEntity) -> str:
        """Generates a sort key for the given entity based on the flat field values
        in this entity."""
        return f"{e.get_external_id()}#{get_flat_fields_json_str(e, field_index)}"

    db_entities = cast(List, db_entities)
    db_entities.sort(key=_get_entity_sort_key)
    for entity in db_entities:
        for field_name in field_index.get_fields_with_non_empty_values(
            entity, EntityFieldType.FORWARD_EDGE
        ):
            field = entity.get_field_as_list(field_name)
            _sort_based_on_flat_fields(field, field_index)


def get_flat_fields_json_str(
    entity: CoreEntity, field_index: CoreEntityFieldIndex
) -> str:
    flat_fields_dict: Dict[str, str] = {}
    for field_name in field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FLAT_FIELD
    ):
        flat_fields_dict[field_name] = str(entity.get_field(field_name))
    return json.dumps(flat_fields_dict, sort_keys=True)


def _print_indented(s: str, indent: int, file: Optional[TextIOWrapper] = None) -> None:
    print(f'{" " * indent}{s}', file=file)


def _obj_id_str(entity: CoreEntity, id_mapping: Dict[int, int]) -> str:
    python_obj_id = id(entity)

    if python_obj_id not in id_mapping:
        fake_id = len(id_mapping)
        id_mapping[python_obj_id] = fake_id
    else:
        fake_id = id_mapping[python_obj_id]

    return f"{entity.get_entity_name()} ({fake_id})"


def write_entity_tree_to_file(
    region_code: str,
    operation_for_filename: str,
    print_tree_structure_only: bool,
    field_index: CoreEntityFieldIndex,
    root_entities: Sequence[Any],
) -> str:
    filepath = make_log_output_path(
        operation_for_filename,
        region_code=region_code,
    )
    with open(filepath, "w", encoding="utf-8") as actual_output_file:
        print_entity_trees(
            root_entities,
            print_tree_structure_only=print_tree_structure_only,
            field_index=field_index,
            file=actual_output_file,
        )

    return filepath


def print_entity_trees(
    entities_list: Sequence[CoreEntity],
    print_tree_structure_only: bool = False,
    python_id_to_fake_id: Optional[Dict[int, int]] = None,
    # Default arg caches across calls to this function
    field_index: CoreEntityFieldIndex = CoreEntityFieldIndex(),
    file: Optional[TextIOWrapper] = None,
) -> None:
    """Recursively prints out all objects in the trees below the given list of
    entities. Each time we encounter a new object, we assign a new fake id (an
    auto-incrementing count) and print that with the object.

    This means that two lists with the exact same shape/flat fields will print
    out the exact same string, making it much easier to debug edge-related
    issues in Diffchecker, etc.

    If |file| is provided, the trees will be output to that file rather than printed
    to the terminal.

    Note: this function sorts any list fields in the provided entity IN PLACE
    (should not matter for any equality checks we generally do).
    """

    if python_id_to_fake_id is None:
        python_id_to_fake_id = {}
        _sort_based_on_flat_fields(entities_list, field_index)

    for entity in entities_list:
        print_entity_tree(
            entity,
            python_id_to_fake_id=python_id_to_fake_id,
            print_tree_structure_only=print_tree_structure_only,
            field_index=field_index,
            file=file,
        )


def print_entity_tree(
    entity: CoreEntity,
    print_tree_structure_only: bool = False,
    indent: int = 0,
    python_id_to_fake_id: Optional[Dict[int, int]] = None,
    # Default arg caches across calls to this function
    field_index: CoreEntityFieldIndex = CoreEntityFieldIndex(),
    file: Optional[TextIOWrapper] = None,
) -> None:
    """Recursively prints out all objects in the tree below the given entity. Each time we encounter a new object, we
    assign a new fake id (an auto-incrementing count) and print that with the object.

    This means that two entity trees with the exact same shape/flat fields will print out the exact same string, making
    it much easier to debug edge-related issues in Diffchecker, etc.

    Note: this function sorts any list fields in the provided entity IN PLACE (should not matter for any equality checks
    we generally do).
    """
    if python_id_to_fake_id is None:
        python_id_to_fake_id = {}
        _sort_based_on_flat_fields([entity], field_index)

    _print_indented(_obj_id_str(entity, python_id_to_fake_id), indent, file)

    indent = indent + 2
    for field in field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FLAT_FIELD
    ):
        if field == "external_id" or not print_tree_structure_only:
            val = entity.get_field(field)
            _print_indented(f"{field}: {str(val)}", indent, file)

    for child_field in field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FORWARD_EDGE
    ):
        child = entity.get_field(child_field)

        if child is not None:
            if isinstance(child, list):
                if not child:
                    _print_indented(f"{child_field}: []", indent, file)
                else:
                    _print_indented(f"{child_field}: [", indent, file)
                    for c in child:
                        print_entity_tree(
                            c,
                            print_tree_structure_only,
                            indent + 2,
                            python_id_to_fake_id,
                            field_index=field_index,
                            file=file,
                        )
                    _print_indented("]", indent, file)

            else:
                _print_indented(f"{child_field}:", indent, file)
                print_entity_tree(
                    child,
                    print_tree_structure_only,
                    indent + 2,
                    python_id_to_fake_id,
                    field_index=field_index,
                    file=file,
                )
        else:
            _print_indented(f"{child_field}: None", indent, file)

    for child_field in field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.BACK_EDGE
    ):
        child = entity.get_field(child_field)
        if not child:
            raise ValueError(f"Expected non-empty child value for field {child_field}")
        if isinstance(child, list):
            first_child = next(iter(child))
            unique = {id(c) for c in child}
            len_str = (
                f"{len(child)}"
                if len(unique) == len(child)
                else f"{len(child)} - ONLY {len(unique)} UNIQUE!"
            )

            id_str = _obj_id_str(first_child, python_id_to_fake_id)
            ellipsis_str = ", ..." if len(child) > 1 else ""

            _print_indented(
                f"{child_field} ({len_str}): [{id_str}{ellipsis_str}] - backedge",
                indent,
                file,
            )
        else:
            id_str = _obj_id_str(child, python_id_to_fake_id)
            _print_indented(f"{child_field}: {id_str} - backedge", indent, file)


def get_all_db_objs_from_trees(
    db_objs: Sequence[DatabaseEntity],
    field_index: CoreEntityFieldIndex,
    result: Optional[Set[DatabaseEntity]] = None,
) -> Set[DatabaseEntity]:
    if result is None:
        result = set()
    for root_obj in db_objs:
        for obj in get_all_db_objs_from_tree(root_obj, field_index, result):
            result.add(obj)
    return result


def get_all_db_objs_from_tree(
    db_obj: DatabaseEntity,
    field_index: CoreEntityFieldIndex,
    result: Optional[Set[DatabaseEntity]] = None,
) -> Set[DatabaseEntity]:
    if result is None:
        result = set()

    if db_obj in result:
        return result

    result.add(db_obj)

    set_fields = field_index.get_fields_with_non_empty_values(
        db_obj, EntityFieldType.FORWARD_EDGE
    )
    for field in set_fields:
        child = db_obj.get_field_as_list(field)
        get_all_db_objs_from_trees(child, field_index, result)

    return result


def get_all_entities_from_tree(
    entity: Entity,
    field_index: CoreEntityFieldIndex,
    result: Optional[List[Entity]] = None,
    seen_ids: Optional[Set[int]] = None,
) -> List[Entity]:
    """Returns a list of all entities in the tree below the entity,
    including the entity itself. Entities are deduplicated by Python object id.
    """

    if result is None:
        result = []
    if seen_ids is None:
        seen_ids = set()

    if id(entity) in seen_ids:
        return result

    result.append(entity)
    seen_ids.add(id(entity))

    fields = field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FORWARD_EDGE
    )

    for field in fields:
        child = entity.get_field(field)

        if child is None:
            raise ValueError("Expected only nonnull values at this point")

        if isinstance(child, list):
            for c in child:
                get_all_entities_from_tree(c, field_index, result, seen_ids)
        else:
            get_all_entities_from_tree(child, field_index, result, seen_ids)

    return result


def get_all_entity_associations_from_tree(
    entity: Entity,
    field_index: CoreEntityFieldIndex,
    result: Optional[Dict[str, Set[Tuple[str, str]]]] = None,
    seen_ids: Optional[Set[int]] = None,
) -> Dict[str, Set[Tuple[str, str]]]:
    """Returns a dictionary of association table name to a set of associations in the
    |entity| tree that belong in that table. Each association is defined as a
    (child_external_id, parent_external_id) tuple where each id is the external_id
    associated with the entity. We use external_ids instead of database primary keys so
    this function can be used on Python entity trees that do not yet have primary keys assigned.
    """

    if result is None:
        result = defaultdict(set)
    if seen_ids is None:
        seen_ids = set()

    if id(entity) in seen_ids:
        return result

    seen_ids.add(id(entity))

    many_to_many_relationships = get_many_to_many_relationships(
        entity.__class__, field_index
    )
    for relationship in many_to_many_relationships:
        parents = entity.get_field_as_list(relationship)
        for parent in parents:
            association_table = get_state_database_association_with_names(
                entity.__class__.__name__, parent.__class__.__name__
            )
            result[association_table.name].add(
                (
                    non_optional(entity.get_external_id()),
                    non_optional(parent.get_external_id()),
                )
            )

    fields = field_index.get_fields_with_non_empty_values(
        entity, EntityFieldType.FORWARD_EDGE
    )

    for field in fields:
        child = entity.get_field(field)

        if child is None:
            raise ValueError("Expected only nonnull values at this point")

        if isinstance(child, list):
            for c in child:
                get_all_entity_associations_from_tree(c, field_index, result, seen_ids)
        else:
            get_all_entity_associations_from_tree(child, field_index, result, seen_ids)
    return result


def get_entities_by_type(
    all_entities: Sequence[DatabaseEntity],
    field_index: CoreEntityFieldIndex,
    entities_of_type: Optional[Dict[Type, List[DatabaseEntity]]] = None,
    seen_entities: Optional[Set[int]] = None,
) -> Dict[Type, List[DatabaseEntity]]:
    """Creates a list of entities for each entity type present in the provided
    |all_entities| graph. Returns the types and the corresponding entity lists
    as a dictionary.
    - if |entities_of_type| is provided, this method will update this dictionary
      rather than creating a new one to return.
    - if |seen_entities| is provided, this method will skip over any entities
      found in |all_entities| that are also present in |seen_entities|.
    """
    if entities_of_type is None:
        entities_of_type = defaultdict(list)
    if seen_entities is None:
        seen_entities = set()

    for entity in all_entities:
        if id(entity) in seen_entities:
            continue
        seen_entities.add(id(entity))
        entity_cls = entity.__class__
        if entity_cls not in entities_of_type:
            entities_of_type[entity_cls] = []
        entities_of_type[entity_cls].append(entity)
        for child_name in field_index.get_fields_with_non_empty_values(
            entity, EntityFieldType.FORWARD_EDGE
        ):
            child_list = entity.get_field_as_list(child_name)
            get_entities_by_type(
                child_list, field_index, entities_of_type, seen_entities
            )

    return entities_of_type


def get_non_flat_property_class_name(
    entity_cls: Type[Entity], property_name: str
) -> Optional[str]:
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


def update_reverse_references_on_related_entities(
    updated_entity: Union[Entity, NormalizedStateEntity],
    new_related_entities: Union[List[Entity], List[NormalizedStateEntity]],
    reverse_relationship_field: str,
    reverse_relationship_field_type: BuildableAttrFieldType,
) -> None:
    """For each of the entities in the |new_related_entities| list, updates the value
    stored in the |reverse_relationship_field| to point to the |updated_entity|.

    If the attribute stored in the |reverse_relationship_field| is a list, replaces
    the reference to the original entity in the list with the |updated_entity|.
    """
    if reverse_relationship_field_type == BuildableAttrFieldType.FORWARD_REF:
        # If the reverse relationship field is a forward ref, set the updated entity
        # directly as the value.
        for new_related_entity in new_related_entities:
            setattr(new_related_entity, reverse_relationship_field, updated_entity)
        return

    if reverse_relationship_field_type != BuildableAttrFieldType.LIST:
        raise ValueError(
            f"Unexpected reverse_relationship_field_type: [{reverse_relationship_field_type}]"
        )

    for new_related_entity in new_related_entities:
        reverse_relationship_list = getattr(
            new_related_entity, reverse_relationship_field
        )

        if updated_entity not in reverse_relationship_list:
            # Add the updated entity to the list since it is not already present
            reverse_relationship_list.append(updated_entity)


@lru_cache(maxsize=None)
def _module_for_module_name(module_name: str) -> ModuleType:
    """Returns the module with the module name."""
    return importlib.import_module(module_name)


def deep_entity_update(
    original_entity: EntityT, **updated_attribute_kwargs: Any
) -> EntityT:
    """Updates the |original_entity| with all of the updated attributes provided in
    the |updated_attribute_kwargs| mapping. For any attribute in the
    updated_attribute_kwargs that is a reference to another entity (or entities),
    updates the reverse references on those entities to point to the new, updated
    version of the |original_entity|.

    Returns the new version of the entity.
    """
    entity_type = type(original_entity)
    updated_entity = original_entity

    reverse_fields_to_update: List[Tuple[str, str, Type[Entity]]] = []

    for field, updated_value in updated_attribute_kwargs.items():
        # Update the value stored in the field on the entity
        updated_entity.set_field(field, updated_value)

        related_class_name = attr_field_referenced_cls_name_for_field_name(
            entity_type, field
        )

        if not related_class_name:
            # This field doesn't store a related entity class, no need to update reverse
            # references
            continue

        if not updated_value:
            # There is nothing being set in this field, so no need to update the
            # reverse references.
            continue

        related_class = get_entity_class_in_module_with_name(
            entities_module=_module_for_module_name(
                original_entity.__class__.__module__
            ),
            class_name=related_class_name,
        )

        reverse_relationship_field = attr_field_name_storing_referenced_cls_name(
            base_cls=related_class, referenced_cls_name=entity_type.__name__
        )

        if not reverse_relationship_field:
            # Not a bi-directional relationship
            continue

        reverse_fields_to_update.append(
            (field, reverse_relationship_field, related_class)
        )

    for field, reverse_relationship_field, related_class in reverse_fields_to_update:
        updated_value = updated_attribute_kwargs[field]

        reverse_relationship_field_type = attr_field_type_for_field_name(
            related_class, reverse_relationship_field
        )

        new_related_entities: List[Entity]
        if isinstance(updated_value, list):
            new_related_entities = updated_value
        else:
            new_related_entities = [updated_value]

        # This relationship is bidirectional, so we will update the reference
        # on all related entities to point to the new updated_entity
        update_reverse_references_on_related_entities(
            new_related_entities=new_related_entities,
            reverse_relationship_field=reverse_relationship_field,
            reverse_relationship_field_type=reverse_relationship_field_type,
            updated_entity=updated_entity,
        )

    return updated_entity


def set_backedges(element: RootEntity, field_index: CoreEntityFieldIndex) -> RootEntity:
    """Set the backedges of the root entity tree using DFS traversal of the root
    entity tree."""
    root = cast(Entity, element)
    root_entity_cls = root.__class__
    stack: List[Entity] = [root]
    while stack:
        current_parent = stack.pop()
        current_parent_cls = current_parent.__class__
        forward_fields = sorted(
            field_index.get_all_core_entity_fields(
                current_parent_cls, EntityFieldType.FORWARD_EDGE
            )
        )
        for field in forward_fields:
            related_entities: List[Entity] = current_parent.get_field_as_list(field)
            if not related_entities:
                continue

            related_entity_cls_name = attr_field_referenced_cls_name_for_field_name(
                current_parent_cls, field
            )
            if not related_entity_cls_name:
                # In the context of a FORWARD_EDGE type field, this should always
                # be nonnull.
                raise ValueError(
                    f"Could not extract the referenced class name from field "
                    f"[{field}] on class [{current_parent_cls}]."
                )

            related_entity_cls = get_entity_class_in_module_with_name(
                state_entities, related_entity_cls_name
            )
            reverse_relationship_field = attr_field_name_storing_referenced_cls_name(
                base_cls=related_entity_cls,
                referenced_cls_name=current_parent_cls.__name__,
            )
            if not reverse_relationship_field:
                # In the context of a FORWARD_EDGE type field, this should always
                # be nonnull.
                raise ValueError(
                    f"Found no field on [{related_entity_cls}] referencing objects "
                    f"of type [{current_parent_cls}]"
                )

            reverse_relationship_field_type = attr_field_type_for_field_name(
                related_entity_cls, reverse_relationship_field
            )
            update_reverse_references_on_related_entities(
                updated_entity=current_parent,
                new_related_entities=related_entities,
                reverse_relationship_field=reverse_relationship_field,
                reverse_relationship_field_type=reverse_relationship_field_type,
            )

            root_reverse_relationship_field = (
                attr_field_name_storing_referenced_cls_name(
                    base_cls=related_entity_cls,
                    referenced_cls_name=root_entity_cls.__name__,
                )
            )

            if not root_reverse_relationship_field:
                raise ValueError(
                    f"Found no field on [{related_entity_cls}] referencing root "
                    f"entities of type [{root_entity_cls}]"
                )

            root_reverse_relationship_field_type = attr_field_type_for_field_name(
                related_entity_cls, root_reverse_relationship_field
            )
            update_reverse_references_on_related_entities(
                updated_entity=root,
                new_related_entities=related_entities,
                reverse_relationship_field=root_reverse_relationship_field,
                reverse_relationship_field_type=root_reverse_relationship_field_type,
            )
            stack.extend(related_entities)
    return element


def get_many_to_many_relationships(
    entity_cls: Type[CoreEntity], field_index: CoreEntityFieldIndex
) -> Set[str]:
    """Returns the set of fields on |entity| that connect that entity to a parent where
    there is a potential many-to-many relationship between entity and that parent entity type.
    """
    many_to_many_relationships = set()
    back_edges = field_index.get_all_core_entity_fields(
        entity_cls, EntityFieldType.BACK_EDGE
    )
    for back_edge in back_edges:
        relationship_field_type = attr_field_type_for_field_name(entity_cls, back_edge)

        parent_cls = get_entity_class_in_module_with_name(
            entities_module=state_entities,
            class_name=attr_field_referenced_cls_name_for_field_name(
                entity_cls, back_edge
            ),
        )

        inverse_relationship_field_name = attr_field_name_storing_referenced_cls_name(
            base_cls=parent_cls,
            referenced_cls_name=entity_cls.__name__,
        )
        inverse_relationship_field_type = (
            attr_field_type_for_field_name(parent_cls, inverse_relationship_field_name)
            if inverse_relationship_field_name
            else None
        )
        if (
            relationship_field_type == BuildableAttrFieldType.LIST
            and inverse_relationship_field_type == BuildableAttrFieldType.LIST
        ):
            many_to_many_relationships.add(back_edge)
    return many_to_many_relationships


def is_many_to_many_relationship(
    parent_cls: Type[Entity], child_cls: Type[Entity]
) -> bool:
    """Returns True if there's a many-to-many relationship between these the provided
    parent and child entities. Entity classes must be directly related otherwise this
    will throw.
    """
    if not entities_have_direct_relationship(parent_cls, child_cls):
        raise ValueError(
            f"Entities [{parent_cls.__name__}] and [{child_cls.__name__}] are not "
            f"directly related - can't call is_many_to_many_relationship()."
        )

    reference_field = attr_field_name_storing_referenced_cls_name(
        parent_cls, child_cls.__name__
    )

    if not reference_field:
        raise ValueError(
            f"Expected to find relationship between [{parent_cls.__name__}] and "
            f"[{child_cls.__name__}] but found none."
        )

    reverse_reference_field = attr_field_name_storing_referenced_cls_name(
        child_cls, parent_cls.__name__
    )

    if not reverse_reference_field:
        raise ValueError(
            f"Expected to find relationship between [{child_cls.__name__}] and "
            f"[{parent_cls.__name__}] but found none."
        )

    return (
        attr_field_type_for_field_name(parent_cls, reference_field)
        == BuildableAttrFieldType.LIST
    ) and (
        attr_field_type_for_field_name(child_cls, reverse_reference_field)
        == BuildableAttrFieldType.LIST
    )


def is_one_to_many_relationship(
    parent_cls: Type[Entity], child_cls: Type[Entity]
) -> bool:
    """Returns True if there's a one-to-many relationship between these the provided
    parent and child entities.
    """
    if not entities_have_direct_relationship(parent_cls, child_cls):
        raise ValueError(
            f"Entities [{parent_cls.__name__}] and [{child_cls.__name__}] are not "
            f"directly related - can't call is_one_to_many_relationship()."
        )

    reference_field = attr_field_name_storing_referenced_cls_name(
        parent_cls, child_cls.__name__
    )

    if not reference_field:
        raise ValueError(
            f"Expected to find relationship between [{parent_cls.__name__}] and "
            f"[{child_cls.__name__}] but found none."
        )

    reverse_reference_field = attr_field_name_storing_referenced_cls_name(
        child_cls, parent_cls.__name__
    )

    if not reverse_reference_field:
        raise ValueError(
            f"Expected to find relationship between [{child_cls.__name__}] and "
            f"[{parent_cls.__name__}] but found none."
        )

    return (
        attr_field_type_for_field_name(parent_cls, reference_field)
        == BuildableAttrFieldType.LIST
    ) and (
        attr_field_type_for_field_name(child_cls, reverse_reference_field)
        != BuildableAttrFieldType.LIST
    )


def is_many_to_one_relationship(
    parent_cls: Type[Entity], child_cls: Type[Entity]
) -> bool:
    """Returns True if there's a many-to-one relationship between these the provided
    parent and child entities.
    """
    if not entities_have_direct_relationship(parent_cls, child_cls):
        raise ValueError(
            f"Entities [{parent_cls.__name__}] and [{child_cls.__name__}] are not "
            f"directly related - can't call is_many_to_one_relationship()."
        )
    return is_one_to_many_relationship(parent_cls=child_cls, child_cls=parent_cls)


def entities_have_direct_relationship(
    entity_cls_a: Type[Entity], entity_cls_b: Type[Entity]
) -> bool:
    """Returns True if the two provided entity types are directly related in the schema
    entity tree. For example, StatePerson and StateAssessment are directly related, but
    StatePerson and StateSupervisionViolationResponse are not.
    """
    reference_field = attr_field_name_storing_referenced_cls_name(
        entity_cls_a, entity_cls_b.__name__
    )
    reverse_reference_field = attr_field_name_storing_referenced_cls_name(
        entity_cls_b, entity_cls_a.__name__
    )
    return reference_field is not None and reverse_reference_field is not None


def get_association_table_id(parent_cls: Type[Entity], child_cls: Type[Entity]) -> str:
    """For two classes that have a many to many relationship between them,
    returns the name of the association table that can be used to hydrate
    relationships between the classes.
    """
    if not is_many_to_many_relationship(parent_cls, child_cls):
        raise ValueError(
            f"Classes [{parent_cls.__name__}] and [{child_cls.__name__}] do not have a "
            f"many-to-many relationship - cannot get an association table."
        )

    # TODO(#10389): Remove this custom handling for legacy sentence association tables
    #  once we remove these classes from the schema.
    if {parent_cls, child_cls} == {
        state_entities.StateSupervisionSentence,
        state_entities.StateCharge,
    } or {parent_cls, child_cls} == {
        normalized_entities.NormalizedStateSupervisionSentence,
        normalized_entities.NormalizedStateCharge,
    }:
        return "state_charge_supervision_sentence_association"

    if {parent_cls, child_cls} == {
        state_entities.StateIncarcerationSentence,
        state_entities.StateCharge,
    } or {parent_cls, child_cls} == {
        normalized_entities.NormalizedStateIncarcerationSentence,
        normalized_entities.NormalizedStateCharge,
    }:
        return "state_charge_incarceration_sentence_association"

    parts = [
        *sorted([parent_cls.get_table_id(), child_cls.get_table_id()]),
        "association",
    ]
    return "_".join(parts)
