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
"""Utility function for generating primary keys from external id(s)."""
import json
from typing import List, Set, Union, cast

from recidiviz.common.attr_mixins import attr_field_referenced_cls_name_for_field_name
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import (
    CoreEntity,
    Entity,
    ExternalIdEntity,
    HasExternalIdEntity,
    RootEntity,
)
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    EntityFieldType,
)
from recidiviz.persistence.entity.state.entities import StatePerson, StateStaff
from recidiviz.pipelines.ingest.state.constants import ExternalIdKey
from recidiviz.pipelines.utils.entities.generate_primary_key import (
    PrimaryKey,
    generate_primary_key,
)
from recidiviz.pipelines.utils.entities.serialization import serialize_entity_into_json
from recidiviz.utils.types import assert_type, non_optional


def string_representation(external_id_keys: Set[ExternalIdKey]) -> str:
    """Get a string representation of a set of external ids."""
    return ",".join(
        sorted(
            _string_representation_of_key(external_id_key)
            for external_id_key in external_id_keys
        )
    )


def _string_representation_of_key(external_id_key: ExternalIdKey) -> str:
    external_id, external_id_type = external_id_key
    return f"{external_id_type}|{external_id}"


def generate_primary_keys_for_root_entity_tree(
    root_primary_key: PrimaryKey,
    root_entity: RootEntity,
    state_code: StateCode,
    field_index: CoreEntityFieldIndex,
) -> RootEntity:
    """Generate primary keys for a root entity tree by doing a Queue BFS traversal of the tree."""
    queue: List[Union[RootEntity, Entity]] = [root_entity]

    while queue:
        entity = cast(Entity, queue.pop(0))
        if isinstance(entity, (StatePerson, StateStaff)):
            entity.set_id(root_primary_key)
        elif isinstance(entity, HasExternalIdEntity):
            external_id = assert_type(entity.get_external_id(), str)
            entity.set_id(
                generate_primary_key(
                    string_representation(
                        {
                            (
                                external_id,
                                entity.get_class_id_name(),
                            )
                        }
                    ),
                    state_code,
                ),
            )
        elif isinstance(entity, ExternalIdEntity):
            entity.set_id(
                generate_primary_key(
                    string_representation(
                        {
                            (
                                entity.external_id,
                                f"{entity.id_type}#{entity.get_class_id_name()}",
                            )
                        }
                    ),
                    state_code,
                )
            )
        else:
            entity.set_id(
                generate_primary_key(
                    json.dumps(
                        serialize_entity_into_json(
                            assert_type(entity, CoreEntity), field_index
                        ),
                        sort_keys=True,
                    ),
                    state_code,
                )
            )

        forward_fields = field_index.get_all_core_entity_fields(
            entity.__class__, EntityFieldType.FORWARD_EDGE
        )
        for field in forward_fields:
            _ = non_optional(
                attr_field_referenced_cls_name_for_field_name(entity.__class__, field)
            )
            queue.extend(entity.get_field_as_list(field))
    return root_entity
