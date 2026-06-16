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
"""Utils for mapping between the fragment-tree and cluster-tree identity
entity hierarchies, analogous to `normalized_entities_utils` for the
state <-> normalized_state hierarchies."""
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import (
    get_all_entity_classes_in_module,
    get_entity_class_in_module_with_name,
)
from recidiviz.persistence.entity.identity import (
    identity_cluster_entities,
    identity_fragment_entities,
)
from recidiviz.persistence.entity.identity.identity_cluster_entity import (
    IdentityClusterEntity,
)


def cluster_entity_class_with_fragment_class_name(
    fragment_class_name: str,
) -> type[IdentityClusterEntity]:
    """Returns the IdentityClusterEntity class whose fragment-tree counterpart
    has the name |fragment_class_name| (e.g. `IdentityName` ->
    `IdentityClusterName`).

    Classes that override `fragment_class_name()` to raise `NotImplementedError`
    (i.e. the cluster root, which has no single fragment counterpart) are
    skipped — they can never be the answer.
    """
    cluster_entity_classes = get_all_entity_classes_in_module(identity_cluster_entities)

    for cluster_entity_class in cluster_entity_classes:
        if not issubclass(cluster_entity_class, IdentityClusterEntity):
            raise ValueError(
                f"Found cluster entity class which is not an "
                f"IdentityClusterEntity: [{cluster_entity_class}]"
            )
        try:
            candidate_fragment_class_name = cluster_entity_class.fragment_class_name()
        except NotImplementedError:
            continue
        if candidate_fragment_class_name == fragment_class_name:
            return cluster_entity_class

    raise ValueError(
        f"No IdentityClusterEntity class corresponding with a fragment class "
        f"name of: [{fragment_class_name}]"
    )


def get_fragment_entity_class_for_cluster_entity(
    cluster_entity_cls: type[IdentityClusterEntity],
) -> type[Entity]:
    """For the given IdentityClusterEntity, returns the fragment-tree Entity
    class that is its pre-clustering counterpart. For example: for
    `IdentityClusterName`, returns `IdentityName`.
    """
    fragment_class_name = cluster_entity_cls.fragment_class_name()
    return get_entity_class_in_module_with_name(
        identity_fragment_entities, fragment_class_name
    )
