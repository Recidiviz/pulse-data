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
"""Converts an `IdentityFragment`-tree's `IdentityAttributes` subtree into
kwargs for the structurally-flat `IdentityCluster` root, analogous to
`convert_entity_trees_to_normalized_versions` for the state <-> normalized_state
trees.

Each `Identity{X}` fragment-side entity has a structurally identical
`IdentityCluster{X}` counterpart with the same flat fields (excepting a
different back-edge name). The pairing is resolved via
`cluster_entity_class_with_fragment_class_name`, and flat fields and forward
edges are copied using `EntityFieldIndex`, so adding a new attribute child
requires no changes here.

The cluster tree is intentionally flattened relative to the fragment tree:
there is no `IdentityClusterAttributes` intermediate, so the flat fields and
forward edges of a merged `IdentityAttributes` land directly on the
`IdentityCluster`.
"""
from typing import Any

from recidiviz.common.attr_mixins import (
    BuildableAttrFieldType,
    attr_field_type_for_field_name,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_field_index import EntityFieldType
from recidiviz.persistence.entity.identity import identity_fragment_entities
from recidiviz.persistence.entity.identity.identity_cluster_entities_utils import (
    cluster_entity_class_with_fragment_class_name,
)
from recidiviz.persistence.entity.identity.identity_cluster_entity import (
    IdentityClusterEntity,
)
from recidiviz.persistence.entity.identity.identity_fragment_entities import (
    IdentityAttributes,
)

# Flat fields on IdentityAttributes that the caller of
# convert_attributes_to_cluster_kwargs sets explicitly on IdentityCluster, so
# the converter must omit them from the returned kwargs to avoid a
# duplicate-keyword-argument error.
_ATTRIBUTES_FIELDS_SET_EXPLICITLY_ON_CLUSTER: frozenset[str] = frozenset({"tenant"})


def _build_cluster_kwargs(
    src: Entity, exclude_fields: frozenset[str] = frozenset()
) -> dict[str, Any]:
    """Returns kwargs for the cluster-tree counterpart of |src|, copying flat
    fields and recursively converting forward-edge children."""
    field_index = entities_module_context_for_module(
        identity_fragment_entities
    ).field_index()
    src_cls = type(src)

    kwargs: dict[str, Any] = {}
    for field_name in field_index.get_all_entity_fields(
        src_cls, EntityFieldType.FLAT_FIELD
    ):
        if field_name in exclude_fields:
            continue
        kwargs[field_name] = src.get_field(field_name)

    for field_name in field_index.get_all_entity_fields(
        src_cls, EntityFieldType.FORWARD_EDGE
    ):
        if field_name in exclude_fields:
            continue
        value = src.get_field(field_name)
        # EntityFieldType.FORWARD_EDGE includes only FORWARD_REF (singular
        # child) and COLLECTION (list/tuple of children) fields.
        field_type = attr_field_type_for_field_name(src_cls, field_name)
        if field_type is BuildableAttrFieldType.FORWARD_REF:
            kwargs[field_name] = (
                _convert_entity_to_cluster_version(value) if value is not None else None
            )
            continue
        kwargs[field_name] = tuple(
            _convert_entity_to_cluster_version(child) for child in value
        )
    return kwargs


def _convert_entity_to_cluster_version(src: Entity) -> IdentityClusterEntity:
    """Returns a new cluster-tree entity built from |src|'s flat fields and
    forward-edge children."""
    cluster_cls = cluster_entity_class_with_fragment_class_name(type(src).__name__)
    return cluster_cls(**_build_cluster_kwargs(src))


def convert_attributes_to_cluster_kwargs(
    attributes: IdentityAttributes,
) -> dict[str, Any]:
    """Returns kwargs for `IdentityCluster(...)` corresponding to the flat
    fields and forward edges of a merged `IdentityAttributes`."""
    return _build_cluster_kwargs(
        attributes,
        exclude_fields=_ATTRIBUTES_FIELDS_SET_EXPLICITLY_ON_CLUSTER,
    )
