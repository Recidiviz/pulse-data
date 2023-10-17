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
import logging
from typing import Any, Dict, Iterable, List, Tuple, cast

import apache_beam as beam
from more_itertools import one

from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.persistence.database.schema_utils import get_state_entity_names
from recidiviz.persistence.entity.base_entity import (
    Entity,
    RootEntity,
    UniqueConstraint,
)
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    get_all_entities_from_tree,
    get_entity_class_in_module_with_name,
)
from recidiviz.persistence.entity.root_entity_utils import (
    get_root_entity_class_for_entity,
    get_root_entity_id,
)
from recidiviz.persistence.entity.state import entities
from recidiviz.pipelines.ingest.state.constants import (
    EntityClassName,
    EntityKey,
    Error,
    UniqueConstraintName,
)
from recidiviz.pipelines.ingest.state.validator import (
    get_entity_key,
    validate_root_entity,
)

ROOT_ENTITY_VALIDATION_ERRORS = "root_entity_validation_errors"
ENTITY_UNIQUE_ID_VALIDATION_ERRORS = "entity_unique_id_validation_errors"
ENTITY_UNIQUE_CONSTRAINT_VALIDATION_ERRORS = (
    "entity_unique_constraint_validation_errors"
)
ENTITIES_BY_ENTITY_KEY = "entities_by_entity_key"
FINAL_ERRORS = "final_errors"


# TODO(#24733): Write a cost-effective version of this transform that takes in
#  PCollection[RootEntity] and spits out PCollection[RootEntity].
class RunValidations(beam.PTransform):
    """A PTransform that validates root entities and their attached children entities."""

    def __init__(self, expected_output_entities: Iterable[str]) -> None:
        super().__init__()
        self.expected_output_entities = expected_output_entities

    def expand(
        self, input_or_inputs: beam.PCollection[RootEntity]
    ) -> beam.PCollection[Entity]:
        # Execute individual root entity validations
        root_entity_to_validation_errors: beam.PCollection[
            Tuple[EntityKey, Iterable[Error]]
        ] = input_or_inputs | "Validate root entities" >> beam.Map(validate_root_entity)

        field_index = CoreEntityFieldIndex()

        all_entities = (
            input_or_inputs
            | "Extract all entities from root entity trees"
            >> beam.FlatMap(
                lambda element: get_all_entities_from_tree(
                    cast(Entity, element), field_index=field_index
                ),
            )
        )

        # Validate that entities have unique ids by type
        entity_names = sorted(
            [name for name in get_state_entity_names() if "association" not in name]
        )
        entity_type_partitions = (
            all_entities
            | "Partition all entities by type"
            >> beam.Partition(
                lambda entity, num_partitions: entity_names.index(
                    entity.get_entity_name()
                ),
                len(entity_names),
            )
        )

        entities_expected_to_be_hydrated_errors: Dict[
            EntityClassName, beam.PCollection[int]
        ] = {}
        unique_id_validation_errors: Dict[
            EntityClassName, beam.PCollection[Tuple[EntityKey, Iterable[Error]]]
        ] = {}
        unique_constraint_validation_errors: Dict[
            Tuple[EntityClassName, UniqueConstraintName],
            beam.PCollection[Tuple[EntityKey, Iterable[Error]]],
        ] = {}
        for i, partition in enumerate(entity_type_partitions):
            entity_name = entity_names[i]

            entities_expected_to_be_hydrated_errors[entity_name] = (
                partition
                | f"Count {entity_name} objects" >> beam.combiners.Count.Globally()
            )

            unique_id_validation_errors[entity_name] = (
                partition
                | f"Group {entity_name} entities by primary key"
                >> beam.GroupBy(
                    lambda entity: get_entity_key(cast(Entity, entity)),
                )
                | f"Check for {entity_name} primary key duplicates"
                >> beam.MapTuple(self.check_id)
            )

            entity_cls = get_entity_class_in_module_with_name(
                entities, snake_to_camel(entity_name, capitalize_first_letter=True)
            )

            for constraint in entity_cls.global_unique_constraints():
                unique_constraint_validation_errors[(entity_name, constraint.name)] = (
                    partition
                    | f"Group {entity_name} by {constraint.fields}"
                    >> beam.GroupBy(*constraint.fields)
                    | f"Check for {entity_name} duplicates"
                    >> beam.FlatMap(self.check_for_duplicates, constraint=constraint)
                )

        unique_id_errors: beam.PCollection[Tuple[EntityKey, Iterable[Error]]] = (
            unique_id_validation_errors.values()
            | "Flatten all unique id errors" >> beam.Flatten()
        )
        unique_constraint_errors: beam.PCollection[
            Tuple[EntityKey, Iterable[Error]]
        ] = (
            unique_constraint_validation_errors.values()
            | "Flatten all unique constraint errors" >> beam.Flatten()
        )

        entities_by_entity_key: beam.PCollection[
            Tuple[EntityKey, Entity]
        ] = all_entities | beam.Map(
            lambda entity: (
                get_entity_key(cast(Entity, entity)),
                cast(Entity, entity),
            )
        )

        errors = (
            {
                ROOT_ENTITY_VALIDATION_ERRORS: root_entity_to_validation_errors,
                ENTITY_UNIQUE_ID_VALIDATION_ERRORS: unique_id_errors,
                ENTITY_UNIQUE_CONSTRAINT_VALIDATION_ERRORS: unique_constraint_errors,
            }
            | "CoGroup errors by Key" >> beam.CoGroupByKey()
            # TODO(#24140) Add the ability to sample errors for logging
            | beam.Map(
                self.log_any_errors,
                **{
                    entity_name: beam.pvalue.AsSingleton(rows)
                    for entity_name, rows in entities_expected_to_be_hydrated_errors.items()
                },
            )
            | beam.MapTuple(self.raise_if_errors)
        )

        # In order for writes to BigQuery to be explicit dependencies on the validations
        # and error handling, we need to re-collect all entities after errors are handled
        # to return to the rest of the pipeline execution.
        final_entities = (
            {ENTITIES_BY_ENTITY_KEY: entities_by_entity_key, FINAL_ERRORS: errors}
            | "CoGroup to get final entities" >> beam.CoGroupByKey()
            | beam.MapTuple(
                lambda _, grouped_items: one(grouped_items[ENTITIES_BY_ENTITY_KEY])
            )
        )

        return final_entities

    @staticmethod
    def check_id(
        entity_id_with_class_name: EntityKey,
        grouped_entities: Iterable[Entity],
    ) -> Tuple[EntityKey, Iterable[Error]]:
        entity_id, _ = entity_id_with_class_name
        entity_list = list(grouped_entities)
        error_messages: List[Error] = []
        if not entity_list:
            return entity_id_with_class_name, [f"No entities found for id {entity_id}"]
        if len(entity_list) > 1:
            entity = entity_list[0]
            error_messages.append(
                f"More than one {entity.get_entity_name()} entity found with {entity.get_class_id_name()} {entity_id}: {entity_list}"
            )
        return entity_id_with_class_name, error_messages

    @staticmethod
    def check_for_duplicates(
        element: Tuple[Any, Iterable[Entity]], *, constraint: UniqueConstraint
    ) -> Iterable[Tuple[EntityKey, Iterable[Error]]]:
        field_names_to_values_obj, grouped_entities = element
        error_messages = []
        if len(list(grouped_entities)) > 1:
            entity = list(grouped_entities)[0]
            error_msg = f"More than one {entity.get_entity_name()} entity found with "
            for field in constraint.fields:
                value = getattr(field_names_to_values_obj, field)
                error_msg += f"{field}={value}, "

            error_msg += "entities found: "
            for e in grouped_entities:
                root_entity_cls = get_root_entity_class_for_entity(type(entity))
                error_msg += (
                    f"[{snake_to_camel(e.get_entity_name(), capitalize_first_letter=True)}: {e.get_class_id_name()} {e.get_id()}, "
                    f"associated with root entity: {root_entity_cls.__name__} id {get_root_entity_id(e)}], "
                )

            error_msg += "This may indicate an error with the raw data."
            error_messages.append(error_msg)
        return [
            (
                get_entity_key(entity),
                error_messages,
            )
            for entity in grouped_entities
        ]

    def log_any_errors(
        self,
        element: Tuple[EntityKey, Dict[str, Iterable[Iterable[Error]]]],
        **kwargs: Dict[str, Any],
    ) -> Tuple[EntityKey, List[Error], List[Error]]:
        """This logs both entity-level and entity-type-level errors."""
        entity_key, grouped_elements = element
        entity_level_errors = (
            [
                error
                for l in grouped_elements[ROOT_ENTITY_VALIDATION_ERRORS]
                for error in l
            ]
            + [
                error
                for l in grouped_elements[ENTITY_UNIQUE_ID_VALIDATION_ERRORS]
                for error in l
            ]
            + [
                error
                for l in grouped_elements[ENTITY_UNIQUE_CONSTRAINT_VALIDATION_ERRORS]
                for error in l
            ]
        )

        entity_type_level_errors = [
            f"Expected non-zero {entity_name} entities to be ingested, but none were ingested."
            for entity_name in self.expected_output_entities
            if kwargs[entity_name] == 0
        ]
        if entity_level_errors:
            error_str = "\n".join(entity_level_errors)
            logging.error("Found errors for entity %s\n%s", entity_key, error_str)
        if entity_type_level_errors:
            error_str = "\n".join(entity_type_level_errors)
            logging.error(error_str)
        return (entity_key, entity_level_errors, entity_type_level_errors)

    @staticmethod
    def raise_if_errors(
        entity_key: EntityKey,
        entity_level_errors: List[Error],
        entity_type_level_errors: List[Error],
    ) -> Tuple[EntityKey, bool]:
        if entity_level_errors:
            raise ValueError(
                f"Found errors for entity {entity_key}\n{entity_level_errors}"
            )
        if entity_type_level_errors:
            raise ValueError(f"Found errors: {entity_type_level_errors}")
        return (entity_key, True)
