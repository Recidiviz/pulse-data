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

from typing import Any, Iterable, List, Tuple

import apache_beam as beam

from recidiviz.common.str_field_utils import snake_to_camel
from recidiviz.persistence.database.schema_utils import get_state_entity_names
from recidiviz.persistence.entity.base_entity import Entity, UniqueConstraint
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    get_all_entities_from_tree,
    get_entity_class_in_module_with_name,
)
from recidiviz.persistence.entity.state import entities
from recidiviz.pipelines.ingest.state.validator import validate_root_entity
from recidiviz.utils.types import assert_type


class RunValidations(beam.PTransform):
    """A PTransform that validates root entities and their attached children entities."""

    def expand(
        self, input_or_inputs: beam.PCollection[Entity]
    ) -> beam.PCollection[Entity]:

        # Execute individual root entity validations
        _ = input_or_inputs | "Validate root entities" >> beam.Map(validate_root_entity)

        field_index = CoreEntityFieldIndex()

        all_entities = (
            input_or_inputs
            | "Extract all entities from root entity trees"
            >> beam.FlatMap(get_all_entities_from_tree, field_index=field_index)
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

        for i, partition in enumerate(entity_type_partitions):
            entity_name = entity_names[i]

            _ = (
                partition
                | f"Group {entity_name} entities by primary key"
                >> beam.GroupBy(lambda entity: assert_type(entity.get_id(), int))
                | f"Check for {entity_name} primary key duplicates"
                >> beam.MapTuple(self.check_id)
            )

            entity_cls = get_entity_class_in_module_with_name(
                entities, snake_to_camel(entity_name, capitalize_first_letter=True)
            )

            for constraint in entity_cls.global_unique_constraints():
                _ = (
                    partition
                    | f"Group {entity_name} by {constraint.fields}"
                    >> beam.GroupBy(*constraint.fields)
                    | f"Check for {entity_name} duplicates"
                    >> beam.Map(self.check_for_duplicates, constraint=constraint)
                )

        return all_entities

    @staticmethod
    def check_id(entity_id: int, grouped_entities: List[Entity]) -> None:
        entity = grouped_entities[0]
        if entity_id is None:
            raise ValueError(f"{entity.get_entity_name()} entity found with None id.")
        if len(grouped_entities) > 1:
            raise ValueError(
                f"More than one {entity.get_entity_name()} entity found with {entity.get_class_id_name()} {entity_id}"
            )

    @staticmethod
    def check_for_duplicates(
        element: Tuple[Any, Iterable[Entity]], *, constraint: UniqueConstraint
    ) -> None:
        field_names_to_values_obj, grouped_entities = element
        if len(list(grouped_entities)) > 1:
            entity = list(grouped_entities)[0]
            error_msg = f"More than one {entity.get_entity_name()} entity found with "
            for field in constraint.fields:
                value = getattr(field_names_to_values_obj, field)
                error_msg += f"{field}={value}, "

            error_msg += "entities found: "
            for e in grouped_entities:
                error_msg += (
                    f"[{snake_to_camel(e.get_entity_name(), capitalize_first_letter=True)}: {e.get_class_id_name()} {e.get_id()}, "
                    f"associated with root entity: {e.get_root_entity_name()} id {e.get_root_entity_id()}], "
                )

            error_msg += "This may indicate an error with the raw data."
            raise ValueError(error_msg)
