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
from typing import Any, Dict, Generator, cast

import apache_beam as beam
from apache_beam.typehints import with_input_types, with_output_types

from recidiviz.common.attr_mixins import attr_field_referenced_cls_name_for_field_name
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.schema_utils import (
    get_state_database_association_with_names,
)
from recidiviz.persistence.entity.base_entity import CoreEntity, Entity
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    EntityFieldType,
    get_many_to_many_relationships,
    is_one_to_one_relationship,
)
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import (
    json_serializable_dict,
)


# pylint: disable=arguments-differ,abstract-method
@with_input_types(Entity)
@with_output_types(Dict[str, Any])
class SerializeEntities(beam.DoFn):
    """A DoFn that serializes entities into JSON-serializable dictionaries for writing to BQ"""

    def __init__(self, state_code: StateCode):
        super().__init__()
        self._state_code = state_code
        self._field_index = CoreEntityFieldIndex()

    def process(self, element: Entity) -> Generator[Dict[str, Any], None, None]:
        """Generates appropriate dictionaries for all elements and association tables."""
        many_to_many_relationships = get_many_to_many_relationships(
            element.__class__, field_index=self._field_index
        )
        for relationship in many_to_many_relationships:
            parent_entity_cls_name = attr_field_referenced_cls_name_for_field_name(
                element.__class__, relationship
            )
            if not parent_entity_cls_name:
                raise ValueError(
                    f"Could not find parent entity class name for {element.__class__}.{relationship}"
                )
            association_table = get_state_database_association_with_names(
                element.__class__.__name__, parent_entity_cls_name
            )
            parent_entities = element.get_field_as_list(relationship)
            for parent_entity in parent_entities:
                yield beam.pvalue.TaggedOutput(
                    association_table.name,
                    {
                        parent_entity.get_class_id_name(): parent_entity.get_id(),
                        element.get_class_id_name(): element.get_id(),
                        "state_code": self._state_code.value,
                    },
                )

        yield beam.pvalue.TaggedOutput(
            element.get_entity_name(),
            serialize_entity_into_json(
                cast(CoreEntity, element), field_index=self._field_index
            ),
        )


def serialize_entity_into_json(
    entity: CoreEntity, field_index: CoreEntityFieldIndex
) -> Dict[str, Any]:
    """Generate a JSON string of an entity's serialized flat field and backedge values."""
    flat_fields = field_index.get_all_core_entity_fields(
        entity.__class__, EntityFieldType.FLAT_FIELD
    )
    back_edges = field_index.get_all_core_entity_fields(
        entity.__class__, EntityFieldType.BACK_EDGE
    )
    for back_edge in back_edges:
        if is_one_to_one_relationship(entity.__class__, back_edge):
            raise ValueError(
                f"Unexpected one-to-one relationship here: {entity.__class__} {back_edge}"
            )

    many_to_many_relationships = get_many_to_many_relationships(
        entity.__class__, field_index
    )

    entity_field_dict = {
        **{field_name: getattr(entity, field_name) for field_name in flat_fields},
        **{
            getattr(entity, field_name)
            .get_class_id_name(): getattr(entity, field_name)
            .get_id()
            for field_name in back_edges
            if field_name not in many_to_many_relationships
            and getattr(entity, field_name)
        },
    }
    return json_serializable_dict(entity_field_dict)
