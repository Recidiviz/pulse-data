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
from typing import List

import apache_beam as beam

from recidiviz.persistence.database.schema_utils import get_state_entity_names
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    get_all_entities_from_tree,
)
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
        entity_names = sorted(get_state_entity_names())
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

        return all_entities

    @staticmethod
    def check_id(entity_id: int, entities: List[Entity]) -> None:
        if entity_id is None:
            raise ValueError(
                f"{entities[0].get_entity_name()} entity found with None id."
            )
        if len(entities) > 1:
            raise ValueError(
                f"More than one {entities[0].get_entity_name()} entity found with {entities[0].get_class_id_name()} {entity_id}"
            )