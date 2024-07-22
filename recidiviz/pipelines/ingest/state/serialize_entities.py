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
"""A DoFn that serializes entities into JSON-serializable dictionaries for writing to BQ."""
from types import ModuleType
from typing import Any, Dict, Generator, cast

import apache_beam as beam
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.common.attr_mixins import attr_field_referenced_cls_name_for_field_name
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_utils import (
    get_state_database_association_with_names,
)
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    get_all_entities_from_tree,
    get_many_to_many_relationships,
)
from recidiviz.persistence.entity.serialization import serialize_entity_into_json


# pylint: disable=arguments-differ,abstract-method
@with_input_types(RootEntity)
@with_output_types(Dict[str, Any])
class SerializeEntities(beam.DoFn):
    """A DoFn that converts a RootEntity into N JSON-serializable dictionaries for
    writing to BQ, where each one represents an entity in that root entity tree.
    """

    def __init__(
        self,
        state_code: StateCode,
        field_index: CoreEntityFieldIndex,
        entities_module: ModuleType,
    ):
        super().__init__()
        self._state_code = state_code
        self._field_index = field_index
        self._entities_module = entities_module

    def process(self, element: RootEntity) -> Generator[Dict[str, Any], None, None]:
        """Generates appropriate dictionaries for all elements and association tables."""

        for entity in get_all_entities_from_tree(
            entity=cast(Entity, element), field_index=self._field_index
        ):
            many_to_many_relationships = get_many_to_many_relationships(
                entity.__class__, field_index=self._field_index
            )
            for relationship in many_to_many_relationships:
                parent_entity_cls_name = attr_field_referenced_cls_name_for_field_name(
                    entity.__class__, relationship
                )
                if not parent_entity_cls_name:
                    raise ValueError(
                        f"Could not find parent entity class name for {entity.__class__}.{relationship}"
                    )
                association_table = get_state_database_association_with_names(
                    entity.__class__.__name__, parent_entity_cls_name
                )
                parent_entities = entity.get_field_as_list(relationship)
                for parent_entity in parent_entities:
                    yield beam.pvalue.TaggedOutput(
                        association_table.name,
                        {
                            parent_entity.get_class_id_name(): parent_entity.get_id(),
                            entity.get_class_id_name(): entity.get_id(),
                            "state_code": self._state_code.value,
                        },
                    )

            yield beam.pvalue.TaggedOutput(
                entity.get_entity_name(),
                serialize_entity_into_json(
                    entity, entities_module=self._entities_module
                ),
            )
