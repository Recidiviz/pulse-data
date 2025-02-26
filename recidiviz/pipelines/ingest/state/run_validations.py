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
from typing import Any, Dict, Generator, Iterable, List, Tuple, cast

import apache_beam as beam
import attr
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.persistence.entity.base_entity import (
    Entity,
    RootEntity,
    UniqueConstraint,
)
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    get_all_entities_from_tree,
    get_all_entity_classes_in_module,
)
from recidiviz.persistence.entity.root_entity_utils import (
    get_entity_class_name_to_root_entity_class_name,
    get_root_entity_id,
)
from recidiviz.persistence.entity.state import entities
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

ENTITY_CLASS_NAME_TO_ROOT_ENTITY_CLASS_NAME = (
    get_entity_class_name_to_root_entity_class_name(entities_module=entities)
)


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
        self,
        field_index: CoreEntityFieldIndex,
        constraints_by_entity_type: Dict[EntityClassName, List[UniqueConstraint]],
    ) -> None:
        super().__init__()
        self.field_index = field_index
        self.constraints_by_entity_type = constraints_by_entity_type

    def process(
        self, element: RootEntity, *_args: Any, **kwargs: Any
    ) -> Generator[Tuple[EntityClassName, EntityCriticalFieldsDict], None, None]:
        """Outputs "critical fields" dictionaries for every entity in the input
        root entity tree.
        """

        for e in get_all_entities_from_tree(
            cast(Entity, element), field_index=self.field_index
        ):
            entity_name = e.get_entity_name()
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
    entity_class_name: EntityClassName
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
            f"More than one {self.entity_class_name} entity found with "
            f"({violating_values_string}). Referencing root entities: "
            f"{self.referencing_root_entities}"
        )


class FindUniqueConstraintFailuresByRootEntity(beam.PTransform):
    """Given an input PCollection with one "critical fields" dictionary per entity of
    the provided |entity_class_name|, returns a PCollection with all root entities
    that violate the given |unique_constraint|, along with info about what entity in the
    tree violated that constraint.
    """

    def __init__(
        self,
        entity_class_name: EntityClassName,
        unique_constraint: UniqueConstraint,
        field_index: CoreEntityFieldIndex,
    ) -> None:
        super().__init__()
        self.entity_class_name = entity_class_name
        self.unique_constraint = unique_constraint
        self.field_index = field_index

    def expand(
        self, input_or_inputs: beam.PCollection[EntityCriticalFieldsDict]
    ) -> beam.PCollection[
        Tuple[RootEntityPrimaryKey, Iterable[UniqueConstraintFailure]]
    ]:
        failures_by_root_entity: beam.PCollection[
            Tuple[RootEntityPrimaryKey, Iterable[UniqueConstraintFailure]]
        ] = (
            input_or_inputs
            | "Convert entity info to keys for the specific constraint"
            >> beam.Map(
                lambda d: (
                    self._generate_constraint_key(d),
                    self._generate_root_entity_key(d),
                )
            )
            | "Group constraint keys to find all referencing root entities"
            >> beam.GroupByKey()
            | "Filter down to keys with more than one reference"
            >> beam.Filter(self._has_more_than_one_reference)
            | "Generate constraint failure objects"
            >> beam.FlatMap(self._generate_failure_objects)
            | "Group failures by root entity key" >> beam.GroupByKey()
        )
        return failures_by_root_entity

    def _generate_constraint_key(
        self, entity_fields: EntityCriticalFieldsDict
    ) -> UniqueConstraintKey:
        """Returns a tuple with the values of the fields checked by the uniqueness
        constraint, in the same order as the fields are listed in the constraint
        definition.
        """
        return tuple(entity_fields[field] for field in self.unique_constraint.fields)

    def _generate_root_entity_key(
        self, entity_fields: EntityCriticalFieldsDict
    ) -> RootEntityPrimaryKey:
        """For the entity represented by the provided fields, returns the entity key
        (i.e. (primary key, entity name) tuple) for the root entity associated with
        this entity.
        """
        return (
            assert_type(entity_fields[CRITICAL_FIELDS_ROOT_ENTITY_ID], int),
            ENTITY_CLASS_NAME_TO_ROOT_ENTITY_CLASS_NAME[self.entity_class_name],
        )

    @staticmethod
    def _has_more_than_one_reference(
        element: Tuple[UniqueConstraintKey, Iterable[RootEntityPrimaryKey]]
    ) -> bool:
        _constraint_key, referencing_root_entities = element
        return len(list(referencing_root_entities)) > 1

    def _generate_failure_objects(
        self, element: Tuple[UniqueConstraintKey, Iterable[RootEntityPrimaryKey]]
    ) -> List[Tuple[RootEntityPrimaryKey, UniqueConstraintFailure]]:
        constraint_key, referencing_root_entities = element
        return [
            (
                root_entity_key,
                UniqueConstraintFailure(
                    entity_class_name=self.entity_class_name,
                    unique_constraint=self.unique_constraint,
                    violating_unique_constraint_key=constraint_key,
                    referencing_root_entities=list(referencing_root_entities),
                ),
            )
            for root_entity_key in referencing_root_entities
        ]


class RunValidations(beam.PTransform):
    """A PTransform that validates root entities and their attached child entities.
    Will throw on any constraint violation or
    """

    def __init__(
        self, expected_output_entities: Iterable[str], field_index: CoreEntityFieldIndex
    ) -> None:
        super().__init__()
        self.expected_output_entities = list(expected_output_entities)
        self.constraints_by_entity_type = self._get_constraints_by_entity_type(
            self.expected_output_entities
        )
        self.field_index = field_index

    @staticmethod
    def _get_constraints_by_entity_type(
        expected_output_entities: List[str],
    ) -> Dict[EntityClassName, List[UniqueConstraint]]:
        """Returns a dictionary mapping entity name (e.g. 'state_assessment') to the
        list of unique constraints that should be checked for that entity. For all
        entities, adds a default constraint check on the primary key column for that
        entity (e.g. person_id for StatePerson).
        """
        constraints_by_entity_type = {}
        for entity_cls in get_all_entity_classes_in_module(entities):
            if entity_cls.get_entity_name() not in expected_output_entities:
                continue

            constraints_by_entity_type[
                entity_cls.get_entity_name()
            ] = entity_cls.global_unique_constraints() + [
                UniqueConstraint(
                    name=f"{entity_cls.get_entity_name()}_primary_keys_unique",
                    fields=[entity_cls.get_primary_key_column_name()],
                )
            ]
        return constraints_by_entity_type

    def expand(
        self, input_or_inputs: beam.PCollection[RootEntity]
    ) -> beam.PCollection[RootEntity]:
        entity_names = sorted(self.expected_output_entities)
        entity_type_partitions: List[
            beam.PCollection[Tuple[EntityClassName, EntityCriticalFieldsDict]]
        ] = (
            input_or_inputs
            | "Generate entity critical fields"
            >> beam.ParDo(
                GetEntityCriticalFields(
                    field_index=self.field_index,
                    constraints_by_entity_type=self.constraints_by_entity_type,
                )
            )
            | "Partition all entities by type"
            >> beam.Partition(
                lambda entity_name_and_fields, num_partitions: entity_names.index(
                    entity_name_and_fields[0]
                ),
                len(entity_names),
            )
        )

        hydrated_entity_counts_by_entity_name: Dict[
            EntityClassName, beam.PCollection[int]
        ] = {}
        unique_constraint_failure_pcollections: List[
            beam.PCollection[
                Tuple[RootEntityPrimaryKey, Iterable[UniqueConstraintFailure]]
            ]
        ] = []
        for i, entity_infos_for_entity_cls in enumerate(entity_type_partitions):
            entity_name = entity_names[i]
            hydrated_entity_counts_by_entity_name[entity_name] = (
                entity_infos_for_entity_cls
                | f"Count {entity_name} objects" >> beam.combiners.Count.Globally()
            )

            # Silence `No value for argument 'pcoll' in function call (no-value-for-parameter)`
            # pylint: disable=E1120
            entity_critical_dicts: beam.PCollection[EntityCriticalFieldsDict] = (
                entity_infos_for_entity_cls
                | f"Remove {entity_name} entity names" >> beam.Values()
            )
            for constraint in self.constraints_by_entity_type[entity_name]:
                root_entity_to_child_constraint_failures = (
                    entity_critical_dicts
                    | f"Find all {constraint.name} failures"
                    >> FindUniqueConstraintFailuresByRootEntity(
                        entity_class_name=entity_name,
                        field_index=self.field_index,
                        unique_constraint=constraint,
                    )
                )
                unique_constraint_failure_pcollections.append(
                    root_entity_to_child_constraint_failures
                )

        all_constraint_failures: beam.PCollection[
            Tuple[RootEntityPrimaryKey, Iterable[UniqueConstraintFailure]]
        ] = (
            unique_constraint_failure_pcollections
            | "Merge all constraint failures into one list" >> beam.Flatten()
        )

        root_entity_by_key: beam.PCollection[
            Tuple[RootEntityPrimaryKey, RootEntity]
        ] = input_or_inputs | "Index RootEntities by primary key" >> beam.Map(
            lambda root_entity: (get_entity_key(root_entity), root_entity)
        )

        final_entities: beam.PCollection[RootEntity] = (
            {
                ROOT_ENTITY: root_entity_by_key,
                UNIQUENESS_CONSTRAINT_ERRORS: all_constraint_failures,
            }
            | "Group by root entity key" >> beam.CoGroupByKey()
            | beam.Map(
                self.raise_errors_or_return_root_entity,
                **{
                    entity_name: beam.pvalue.AsSingleton(count)
                    for entity_name, count in hydrated_entity_counts_by_entity_name.items()
                },
            )
        )
        return final_entities

    def get_hydrated_entities_errors(
        self, hydrated_entity_counts_by_entity_name: Dict[str, int]
    ) -> List[str]:
        return [
            f"Expected non-zero {entity_name} entities to be ingested, but none were ingested."
            for entity_name in self.expected_output_entities
            if assert_type(hydrated_entity_counts_by_entity_name[entity_name], int) == 0
        ]

    def raise_errors_or_return_root_entity(
        self,
        element: Tuple[RootEntityPrimaryKey, Dict[str, Any]],
        **hydrated_entity_counts_by_entity_name: int,
    ) -> RootEntity:
        """For the provided root entity and associated information, raises if there are
        any validation errors associated with this root entity. Returns the root entity
        if there are no errors.

        Will also raise if for any expected entity type we find no output entities.
        """
        entity_type_level_errors = self.get_hydrated_entities_errors(
            hydrated_entity_counts_by_entity_name
        )
        if entity_type_level_errors:
            raise ValueError(f"Found errors: {entity_type_level_errors}")

        root_entity_key, grouped_elements = element

        root_entity = grouped_elements[ROOT_ENTITY][0]

        entity_level_errors: List[str] = list(
            validate_root_entity(root_entity, self.field_index)
        )
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
                f"Found errors for root entity {root_entity_key}:\n  * {errors_str}"
            )

        return root_entity
