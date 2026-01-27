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
"""Helper for converting a RootEntity to its corresponding Normalized* type."""
from typing import Any, Dict, Mapping, Sequence, Type, TypeVar

from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attribute_field_type_reference_for_class,
)
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import (
    get_entity_class_in_module_with_name,
    set_backedges,
)
from recidiviz.persistence.entity.state import normalized_entities
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.pipelines.ingest.state.normalization.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)
from recidiviz.utils.types import assert_subclass, assert_type

NormalizedRootEntityT = TypeVar(
    "NormalizedRootEntityT",
    normalized_entities.NormalizedStatePerson,
    normalized_entities.NormalizedStateStaff,
)


def build_normalized_root_entity(
    pre_normalization_root_entity: RootEntity,
    normalized_root_entity_cls: Type[NormalizedRootEntityT],
    root_entity_subtree_kwargs: Mapping[str, Sequence[NormalizedStateEntity]],
    root_entity_flat_field_kwargs: Dict[str, Any],
) -> NormalizedRootEntityT:
    """Helper for converting a RootEntity to its corresponding Normalized* type.

    Args:
        pre_normalization_root_entity: The root entity to convert.
        normalized_root_entity_cls: The expected output type.
        root_entity_subtree_kwargs: A dictionary of keyword args for normalized child
            entities that needed non-standard normalization.
        root_entity_flat_field_kwargs: A dictionary of keyword args for normalized flat
            fields that need non-standard normalization.
    """
    normalized_root_entity_kwargs = {
        **root_entity_subtree_kwargs,
        **root_entity_flat_field_kwargs,
    }
    class_reference = attribute_field_type_reference_for_class(
        normalized_root_entity_cls
    )
    for field in class_reference.fields:
        field_info = class_reference.get_field_info(field)
        if field in normalized_root_entity_kwargs:
            continue
        pre_normalization_field_value = assert_type(
            pre_normalization_root_entity, Entity
        ).get_field(field)
        if field_info.referenced_cls_name is None:
            normalized_root_entity_kwargs[field] = pre_normalization_field_value
            continue

        referenced_cls = assert_subclass(
            get_entity_class_in_module_with_name(
                normalized_entities, field_info.referenced_cls_name
            ),
            NormalizedStateEntity,
        )

        if field_info.field_type is not BuildableAttrFieldType.LIST:
            raise ValueError(
                f"Unexpected field type [{field_info.field_type}] for field [{field}] "
                f"on root entity with type "
                f"[{type(pre_normalization_root_entity).__name__}]"
            )

        # If these are forward edges and there's no explicit normalization already set
        # up, convert to Normalized* types.
        try:
            normalized_values = convert_entity_trees_to_normalized_versions(
                pre_normalization_field_value,
                normalized_entity_class=referenced_cls,
                additional_attributes_map={},
            )
        except Exception as e:
            raise ValueError(
                f"Failed to convert values in field [{field}] on class "
                f"[{type(pre_normalization_root_entity).__name__}] to normalized "
                f"versions of type [{referenced_cls.__name__}]"
            ) from e
        normalized_root_entity_kwargs[field] = normalized_values
    normalized_entity = normalized_root_entity_cls(
        **normalized_root_entity_kwargs,  # type: ignore[arg-type]
    )
    entities_module_context = entities_module_context_for_module(normalized_entities)
    set_backedges(normalized_entity, entities_module_context)
    return normalized_entity
