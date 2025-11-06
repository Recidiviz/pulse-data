# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Utility classes for validating state entities and entity trees."""
from types import ModuleType
from typing import Any, Dict, Generator, Iterable, List, Tuple, cast

import apache_beam as beam
import attr
from apache_beam.typehints import with_input_types, with_output_types
from more_itertools import one

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import (
    Entity,
    RootEntity,
    UniqueConstraint,
)
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_entity,
)
from recidiviz.persistence.entity.entity_utils import get_all_entities_from_tree
from recidiviz.persistence.entity.root_entity_utils import (
    get_entity_class_to_root_entity_class,
    get_root_entity_id,
)
from recidiviz.pipelines.ingest.state.constants import EntityClassName, EntityKey
from recidiviz.pipelines.ingest.state.validator import (
    get_entity_key,
    validate_root_entity,
)
from recidiviz.utils.types import assert_type, assert_type_list

RootEntityPrimaryKey = EntityKey
EntityCriticalFieldsDict = Dict[str, Any]
UniqueConstraintKey = Tuple[Any, ...]

ROOT_ENTITY = "root_entity"
UNIQUENESS_CONSTRAINT_ERRORS = "uniqueness_constraint_errors"
CRITICAL_FIELDS_ROOT_ENTITY_ID = "root_entity_id"


@with_input_types(RootEntity)
@with_output_types(Tuple[EntityClassName, EntityCriticalFieldsDict])
class GetEntityCriticalFields(beam.DoFn):
    """Given an input PCollection of RootEntity, returns a PCollection with one
    "critical fields" dictionary for any Entity referenced in any of the input root
    entities. A "critical fields" dictionary contains a mapping of field name to value
    for any field referenced by any uniqueness constraint on that entity class.
    """

    # Silence `Method 'process_batch' is abstract in class 'DoFn' but is not overridden (abstract-method)`
    # pylint: disable=W0223

    def __init__(
        self, constraints_by_entity_type: Dict[EntityClassName, List[UniqueConstraint]]
    ) -> None:
        super().__init__()
        self.constraints_by_entity_type = constraints_by_entity_type

    def process(
        self, element: RootEntity, *_args: Any, **kwargs: Any
    ) -> Generator[Tuple[EntityClassName, EntityCriticalFieldsDict], None, None]:
        """Outputs "critical fields" dictionaries for every entity in the input
        root entity tree.
        """
        entity = cast(Entity, element)
        entities_module_context = entities_module_context_for_entity(entity)
        for e in get_all_entities_from_tree(entity, entities_module_context):
            entity_name = e.get_entity_name()

            if entity_name not in self.constraints_by_entity_type:
                raise ValueError(
                    f"Found output entities of type [{type(e).__name__}] that are not "
                    f"in the expected entities list."
                )
            yield (
                entity_name,
                self._get_critical_fields_dict(
                    e, self.constraints_by_entity_type[entity_name]
                ),
            )

    @staticmethod
    def _get_critical_fields_dict(
        entity: Entity, constraints: List[UniqueConstraint]
    ) -> EntityCriticalFieldsDict:
        return {
            CRITICAL_FIELDS_ROOT_ENTITY_ID: get_root_entity_id(entity),
            **{f: getattr(entity, f) for c in constraints for f in c.fields},
        }


@attr.define
class UniqueConstraintFailure:
    entity_cls: type[Entity]
    unique_constraint: UniqueConstraint
    violating_unique_constraint_key: UniqueConstraintKey
    referencing_root_entities: List[RootEntityPrimaryKey]

    def error_string(self) -> str:
        violating_values_string = ", ".join(
            [
                f"{field}={self.violating_unique_constraint_key[i]}"
                for i, field in enumerate(self.unique_constraint.fields)
            ]
        )
        return (
            f"More than one {self.entity_cls.__name__} entity found with "
            f"({violating_values_string}). Referencing root entities: "
            f"{self.referencing_root_entities}"
        )


ConstraintName = str
ConstraintKeyWithMetadata = Tuple[
    UniqueConstraintKey, RootEntityPrimaryKey, ConstraintName, EntityClassName
]


class FindUniqueConstraintFailures(beam.PTransform):
    """Given an input PCollection with (entity_name, critical_fields_dict) tuples for
    all entities, returns a PCollection with all root entities that violate any
    uniqueness constraint, along with info about what entity violated what constraint.

    This processes all entity types and all constraints in a unified way, avoiding the
    need to partition by entity type.
    """

    def __init__(
        self,
        constraints_by_entity_name: Dict[EntityClassName, List[UniqueConstraint]],
        entity_name_to_root_entity_name: Dict[EntityClassName, EntityClassName],
        entity_name_to_cls: Dict[EntityClassName, type[Entity]],
    ) -> None:
        super().__init__()
        self.constraints_by_entity_name = constraints_by_entity_name
        self.entity_name_to_root_entity_name = entity_name_to_root_entity_name
        self.entity_name_to_cls = entity_name_to_cls

    def expand(
        self,
        input_or_inputs: beam.PCollection[
            Tuple[EntityClassName, EntityCriticalFieldsDict]
        ],
    ) -> beam.PCollection[
        Tuple[RootEntityPrimaryKey, Iterable[UniqueConstraintFailure]]
    ]:
        failures_by_root_entity: beam.PCollection[
            Tuple[RootEntityPrimaryKey, Iterable[UniqueConstraintFailure]]
        ] = (
            input_or_inputs
            | "Generate constraint keys for all constraints"
            >> beam.FlatMap(self._generate_all_constraint_keys)
            | "Group constraint keys to find all referencing root entities"
            >> beam.GroupByKey()
            | "Filter down to keys with more than one reference"
            >> beam.Filter(self._has_more_than_one_reference)
            | "Generate constraint failure objects"
            >> beam.FlatMap(self._generate_failure_objects)
            | "Group failures by root entity key" >> beam.GroupByKey()
        )
        return failures_by_root_entity

    def _generate_all_constraint_keys(
        self, element: Tuple[EntityClassName, EntityCriticalFieldsDict]
    ) -> List[
        Tuple[
            Tuple[ConstraintName, UniqueConstraintKey],
            Tuple[RootEntityPrimaryKey, EntityClassName],
        ]
    ]:
        """For a single entity's critical fields, generate constraint key tuples for ALL
        applicable uniqueness constraints on that entity type.

        Returns a list of ((constraint_name, constraint_key), (root_entity_key, entity_name)) tuples.
        """
        entity_name, entity_fields = element
        root_entity_name = self.entity_name_to_root_entity_name[entity_name]
        root_entity_key: RootEntityPrimaryKey = (
            assert_type(entity_fields[CRITICAL_FIELDS_ROOT_ENTITY_ID], int),
            root_entity_name,
        )

        results = []
        for constraint in self.constraints_by_entity_name[entity_name]:
            constraint_key = self._generate_constraint_key(entity_fields, constraint)

            # Skip if constraint doesn't apply (e.g., has nulls)
            if constraint.ignore_nulls and any(k is None for k in constraint_key):
                continue

            # Key by (constraint_name, constraint_key) to allow us group identical
            # tuples for the same constraint.
            results.append(
                ((constraint.name, constraint_key), (root_entity_key, entity_name))
            )

        return results

    def _generate_constraint_key(
        self, entity_fields: EntityCriticalFieldsDict, constraint: UniqueConstraint
    ) -> UniqueConstraintKey:
        """Returns a tuple with the values of the fields checked by the uniqueness
        constraint, in the same order as the fields are listed in the constraint definition.
        """
        key_values = []
        for field in constraint.fields:
            key_value = entity_fields[field]
            if field in constraint.transforms_dict:
                key_value = constraint.transforms_dict[field](key_value)
            key_values.append(key_value)
        return tuple(key_values)

    @staticmethod
    def _has_more_than_one_reference(
        element: Tuple[
            Tuple[ConstraintName, UniqueConstraintKey],
            Iterable[Tuple[RootEntityPrimaryKey, EntityClassName]],
        ]
    ) -> bool:
        _constraint_info, referencing_entities = element
        return len(list(referencing_entities)) > 1

    def _generate_failure_objects(
        self,
        element: Tuple[
            Tuple[ConstraintName, UniqueConstraintKey],
            Iterable[Tuple[RootEntityPrimaryKey, EntityClassName]],
        ],
    ) -> List[Tuple[RootEntityPrimaryKey, UniqueConstraintFailure]]:
        (constraint_name, constraint_key), referencing_entities = element
        referencing_root_entity_keys = sorted(
            {root_entity_key for root_entity_key, _ in referencing_entities}
        )
        # All entity class names should be the same for the given constraint we're
        # inspecting.
        entity_class_name = one(
            {entity_class_name for _, entity_class_name in referencing_entities}
        )
        entity_cls = self.entity_name_to_cls[entity_class_name]

        # Find the constraint object
        constraint = one(
            c
            for c in self.constraints_by_entity_name[entity_class_name]
            if c.name == constraint_name
        )

        return [
            (
                root_entity_key,
                UniqueConstraintFailure(
                    entity_cls=entity_cls,
                    unique_constraint=constraint,
                    violating_unique_constraint_key=constraint_key,
                    referencing_root_entities=referencing_root_entity_keys,
                ),
            )
            for root_entity_key in referencing_root_entity_keys
        ]


class RunValidations(beam.PTransform):
    """A PTransform that validates root entities and their attached child entities.
    Will throw on any constraint violation or
    """

    def __init__(
        self,
        expected_output_entity_classes: Iterable[type[Entity]],
        state_code: StateCode,
        entities_module: ModuleType,
    ) -> None:
        super().__init__()
        self.expected_output_entity_classes = list(expected_output_entity_classes)
        self.expected_output_entity_name_to_class = {
            entity_cls.get_entity_name(): entity_cls
            for entity_cls in expected_output_entity_classes
        }
        self.entities_module = entities_module
        self.constraints_by_entity_type = self._get_constraints_by_entity_name(
            state_code, self.expected_output_entity_classes
        )
        self.state_code = state_code

        # Build helper maps for unified constraint checking
        entity_class_to_root_entity_class = get_entity_class_to_root_entity_class(
            entities_module=entities_module
        )
        self.entity_name_to_root_entity_name = {
            entity_name: entity_class_to_root_entity_class[entity_cls].get_entity_name()
            for entity_name, entity_cls in self.expected_output_entity_name_to_class.items()
        }

    @staticmethod
    def _get_constraints_by_entity_name(
        state_code: StateCode, expected_output_entity_classes: List[type[Entity]]
    ) -> Dict[EntityClassName, List[UniqueConstraint]]:
        """Returns a dictionary mapping entity name (e.g. 'state_assessment') for all
        expected output entities to the list of unique constraints that should be
        checked for that entity. For all entities, adds a default constraint check on
        the primary key column for that entity (e.g. person_id for StatePerson).
        """
        constraints_by_entity_type = {}
        for entity_cls in expected_output_entity_classes:
            unique_constraints_for_state = [
                c
                for c in entity_cls.global_unique_constraints()
                if state_code not in c.exempt_states
            ]

            constraints_by_entity_type[
                entity_cls.get_entity_name()
            ] = unique_constraints_for_state + [
                UniqueConstraint(
                    name=f"{entity_cls.get_entity_name()}_primary_keys_unique",
                    fields=[entity_cls.get_primary_key_column_name()],
                )
            ]
        return constraints_by_entity_type

    def expand(
        self, input_or_inputs: beam.PCollection[RootEntity]
    ) -> beam.PCollection[RootEntity]:
        entity_critical_fields_with_names: beam.PCollection[
            Tuple[EntityClassName, EntityCriticalFieldsDict]
        ] = input_or_inputs | "Generate entity critical fields" >> beam.ParDo(
            GetEntityCriticalFields(
                constraints_by_entity_type=self.constraints_by_entity_type
            )
        )

        # Validate expected entities have hydrated at least one entity
        # of that type (fail here if any entity type has zero count).
        # We collect all counts into a dict before validating so we can show all
        # missing entity types at once rather than failing on the first one.
        entity_type_count_error = (
            entity_critical_fields_with_names
            | "Count entities by class" >> beam.combiners.Count.PerKey()
            | "Convert counts to dict" >> beam.combiners.ToDict()
            | "Validate entity type counts"
            >> beam.Map(self.validate_entity_type_counts)
        )

        unique_constraint_failures: beam.PCollection[
            Tuple[RootEntityPrimaryKey, Iterable[UniqueConstraintFailure]]
        ] = (
            entity_critical_fields_with_names
            | "Find all unique constraint failures"
            >> FindUniqueConstraintFailures(
                constraints_by_entity_name=self.constraints_by_entity_type,
                entity_name_to_root_entity_name=self.entity_name_to_root_entity_name,
                entity_name_to_cls=self.expected_output_entity_name_to_class,
            )
        )

        root_entity_by_key: beam.PCollection[
            Tuple[RootEntityPrimaryKey, RootEntity]
        ] = input_or_inputs | "Index RootEntities by primary key" >> beam.Map(
            lambda root_entity: (get_entity_key(root_entity), root_entity)
        )

        final_entities: beam.PCollection[RootEntity] = (
            {
                ROOT_ENTITY: root_entity_by_key,
                UNIQUENESS_CONSTRAINT_ERRORS: unique_constraint_failures,
            }
            | "Group by root entity key" >> beam.CoGroupByKey()
            | "Validate and return root entities"
            >> beam.Map(
                self.raise_errors_or_return_root_entity,
                # Pass the entity count errors as a side input so that all output
                # is blocked on these validations being checked.
                entity_type_count_error=beam.pvalue.AsSingleton(
                    entity_type_count_error
                ),
            )
        )
        return final_entities

    def validate_entity_type_counts(
        self, entity_counts: Dict[EntityClassName, int]
    ) -> str | None:
        """Validates that all expected entity types have non-zero counts.

        Returns an error string if any entity type has zero entities, None otherwise.
        """
        errors = [
            f"Expected non-zero {entity_cls.__name__} entities to be produced, but none were produced."
            for entity_name, entity_cls in self.expected_output_entity_name_to_class.items()
            if entity_counts.get(entity_name, 0) == 0
        ]
        if errors:
            return f"Found errors: {errors}"
        return None

    def raise_errors_or_return_root_entity(
        self,
        element: Tuple[RootEntityPrimaryKey, Dict[str, Any]],
        entity_type_count_error: str | None,
    ) -> RootEntity:
        """For the provided root entity and associated information, raises if there are
        any validation errors associated with this root entity. Returns the root entity
        if there are no errors.
        """
        if entity_type_count_error:
            raise ValueError(entity_type_count_error)

        _, grouped_elements = element

        root_entity = grouped_elements[ROOT_ENTITY][0]

        entity_level_errors: List[str] = list(validate_root_entity(root_entity))
        entity_level_errors += list(
            {
                f.error_string()
                for failure_list in grouped_elements[UNIQUENESS_CONSTRAINT_ERRORS]
                for f in assert_type_list(failure_list, UniqueConstraintFailure)
            }
        )
        if entity_level_errors:
            errors_str = "\n  * ".join(entity_level_errors)
            raise ValueError(
                f"Found errors for root entity {root_entity.limited_pii_repr()}:\n  * {errors_str}"
            )

        return root_entity
