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
"""IdentityClusterEntity is the common base class for all entities in the
identity cluster tree (the post-clustering output of the batch identity
clustering pipeline)."""

from recidiviz.persistence.entity.identity.identity_entity_mixin import (
    IdentityEntityMixin,
)

# Class-name prefix shared by every entity in the cluster tree (e.g.
# `IdentityClusterName`). Its fragment-tree counterpart drops the `Cluster`
# infix (e.g. `IdentityName`); together these two prefixes encode the
# `IdentityCluster{X}` <-> `Identity{X}` naming convention that pairs the two
# parallel hierarchies.
IDENTITY_CLUSTER_CLASS_NAME_PREFIX = "IdentityCluster"
IDENTITY_FRAGMENT_CLASS_NAME_PREFIX = "Identity"


class IdentityClusterEntity(IdentityEntityMixin):
    """Common base class for all entities in the identity cluster tree,
    analogous to `NormalizedStateEntity` for the normalized_state dataset.

    Hosts the naming convention that pairs each cluster-tree class with its
    fragment-tree counterpart, so the convention lives with the entities
    rather than in the pipeline code that converts between the two trees.
    """

    @classmethod
    def fragment_class_name(cls) -> str:
        """The name of the class in `identity_fragment_entities.py` that is the
        pre-clustering counterpart of this class (e.g. `IdentityClusterName` ->
        `IdentityName`), derived by naming convention.

        Note: this is only well-defined for the cluster-tree *child* entities.
        The `IdentityCluster` root is intentionally flattened relative to the
        fragment tree (its fields come from `IdentityFragment` and its
        `IdentityAttributes` subtree combined), so it has no single fragment
        counterpart and is never resolved through this method.
        """
        if not cls.__name__.startswith(IDENTITY_CLUSTER_CLASS_NAME_PREFIX):
            raise ValueError(
                f"Name of IdentityClusterEntity class [{cls.__name__}] must "
                f"start with [{IDENTITY_CLUSTER_CLASS_NAME_PREFIX}]"
            )
        return (
            f"{IDENTITY_FRAGMENT_CLASS_NAME_PREFIX}"
            f"{cls.__name__[len(IDENTITY_CLUSTER_CLASS_NAME_PREFIX):]}"
        )
