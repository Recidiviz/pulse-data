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
"""`EntitiesModuleContext` for the `identity_cluster_entities` module.

Lives in its own file (rather than alongside the other module contexts in
`entities_module_context_factory`) so `identity_cluster_entities` can import
it for hash computation in `IdentityCluster.__attrs_post_init__` without
forming an `identity_cluster_entities` -> factory ->
`identity_cluster_entities` cycle.
"""
from types import ModuleType

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context import EntitiesModuleContext
from recidiviz.persistence.entity.identity import identity_cluster_entities
from recidiviz.persistence.entity.identity.entity_documentation_utils import (
    description_for_field,
)


class IdentityClusterEntitiesModuleContext(EntitiesModuleContext):
    """EntitiesModuleContext for the identity cluster entities module."""

    @classmethod
    def entities_module(cls) -> ModuleType:
        return identity_cluster_entities

    @classmethod
    def class_hierarchy(cls) -> list[str]:
        return [
            identity_cluster_entities.IdentityCluster.__name__,
            identity_cluster_entities.IdentityClusterExternalId.__name__,
            identity_cluster_entities.IdentityClusterName.__name__,
            identity_cluster_entities.IdentityClusterGender.__name__,
            identity_cluster_entities.IdentityClusterSex.__name__,
            identity_cluster_entities.IdentityClusterRace.__name__,
            identity_cluster_entities.IdentityClusterEthnicity.__name__,
            identity_cluster_entities.IdentityClusterPhoneNumber.__name__,
            identity_cluster_entities.IdentityClusterEmail.__name__,
        ]

    @classmethod
    def partition_column_name(cls) -> str:
        return "tenant"

    @classmethod
    def field_description(cls, entity_cls: type[Entity], field_name: str) -> str | None:
        return description_for_field(entity_cls, field_name)


IDENTITY_CLUSTER_ENTITIES_CONTEXT: EntitiesModuleContext = (
    IdentityClusterEntitiesModuleContext()
)
