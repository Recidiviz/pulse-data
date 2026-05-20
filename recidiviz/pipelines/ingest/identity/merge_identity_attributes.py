# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Function for merging IdentityAttributes across a cluster's full set of
fragments (all external IDs, all upper-bound dates, all ingest views)."""
from typing import Any, TypeVar

from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attr_field_type_for_field_name,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_field_index import (
    EntityFieldIndex,
    EntityFieldType,
)
from recidiviz.persistence.entity.identity.entities import (
    IdentityAttributes,
    IdentityFragment,
)

T = TypeVar("T")
EntityT = TypeVar("EntityT", bound=Entity)


def merge_identity_attributes(
    sorted_fragments: list[IdentityFragment],
    field_index: EntityFieldIndex,
) -> IdentityAttributes:
    """Merge attributes from the sorted list of fragments in a cluster into a
    single IdentityAttributes.

    For flat scalar attributes (e.g. birthdate): requires all non-None values
    to agree. Raises a ValueError if they do not.

    For entity-typed scalar attributes (e.g. name, gender, sex, ethnicity):
    merges sub-fields individually so one fragment may provide a middle_name
    while another provides only given_name and surname. Raises a ValueError
    if any sub-field has conflicting non-None values.

    For list attributes (e.g. races, phone_numbers, emails): unions across all
    fragments, deduplicating by Entity equality.
    """
    result = sorted_fragments[0].attributes
    for fragment in sorted_fragments[1:]:
        result = _merge_entity("attributes", result, fragment.attributes, field_index)
    return result


def _merge_entity(
    field_name: str,
    current: EntityT,
    incoming: EntityT,
    field_index: EntityFieldIndex,
) -> EntityT:
    """Recursively merge two entities of the same type."""
    entity_cls = type(current)
    merged_kwargs: dict[str, Any] = {}

    for f in field_index.get_all_entity_fields(entity_cls, EntityFieldType.FLAT_FIELD):
        merged_kwargs[f] = _merge_scalar(
            f"{field_name}.{f}", current.get_field(f), incoming.get_field(f)
        )

    for f in field_index.get_all_entity_fields(
        entity_cls, EntityFieldType.FORWARD_EDGE
    ):
        sub_field_type = attr_field_type_for_field_name(entity_cls, f)
        if sub_field_type == BuildableAttrFieldType.FORWARD_REF:
            cur_val = current.get_field(f)
            inc_val = incoming.get_field(f)
            if cur_val is None:
                merged_kwargs[f] = inc_val
            elif inc_val is None:
                merged_kwargs[f] = cur_val
            else:
                merged_kwargs[f] = _merge_entity(
                    f"{field_name}.{f}", cur_val, inc_val, field_index
                )
        else:
            combined = current.get_field(f) + incoming.get_field(f)
            if combined and field_index.get_all_entity_fields(
                type(combined[0]), EntityFieldType.FORWARD_EDGE
            ):
                raise ValueError(
                    f"Cannot merge list field {field_name}.{f}: elements of "
                    f"type {type(combined[0]).__name__} have forward-edge "
                    f"children."
                )
            merged_kwargs[f] = _dedupe(combined)

    return entity_cls(**merged_kwargs)


def _merge_scalar(field_name: str, current: T | None, incoming: T | None) -> T | None:
    """Merge a single scalar field. Returns the non-None value if only one is
    set, or the shared value if both are equal. Raises a ValueError if both
    are non-None and differ."""

    if incoming is None:
        return current

    if current is None:
        return incoming

    # TODO(#75622): Replace crash-on-conflict with more sophisticated demographic guard.
    if current != incoming:
        raise ValueError(
            f"Conflicting non-None values for {field_name!r}: "
            f"{current!r} vs {incoming!r}."
        )
    return current


def _dedupe(items: list[T]) -> list[T]:
    """Deduplicate using Entity equality, preserving the first occurrence."""
    result: list[T] = []
    for item in items:
        if item not in result:
            result.append(item)
    return result
