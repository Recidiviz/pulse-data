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
"""`EntitiesModuleContext` for the `identity_fragment_entities` module.

Lives in its own file (rather than alongside the other module contexts in
`entities_module_context_factory`) so `identity_fragment_entities` can import
it for field-index lookup in `IdentityAttributes.__attrs_post_init__` without
forming an `identity_fragment_entities` -> factory ->
`identity_fragment_entities` cycle.
"""
from types import ModuleType

from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entities_module_context import EntitiesModuleContext
from recidiviz.persistence.entity.identity import identity_fragment_entities
from recidiviz.persistence.entity.identity.entity_documentation_utils import (
    description_for_field as identity_description_for_field,
)


class IdentityFragmentEntitiesModuleContext(EntitiesModuleContext):
    """EntitiesModuleContext for the identity fragment entities module."""

    @classmethod
    def entities_module(cls) -> ModuleType:
        return identity_fragment_entities

    @classmethod
    def class_hierarchy(cls) -> list[str]:
        return [
            identity_fragment_entities.IdentityFragment.__name__,
            identity_fragment_entities.IdentityExternalId.__name__,
            identity_fragment_entities.IdentityAttributes.__name__,
            identity_fragment_entities.IdentityName.__name__,
            identity_fragment_entities.IdentityGender.__name__,
            identity_fragment_entities.IdentitySex.__name__,
            identity_fragment_entities.IdentityRace.__name__,
            identity_fragment_entities.IdentityEthnicity.__name__,
            identity_fragment_entities.IdentityPhoneNumber.__name__,
            identity_fragment_entities.IdentityEmail.__name__,
        ]

    @classmethod
    def field_description(cls, entity_cls: type[Entity], field_name: str) -> str | None:
        return identity_description_for_field(entity_cls, field_name)


IDENTITY_FRAGMENT_ENTITIES_CONTEXT: EntitiesModuleContext = (
    IdentityFragmentEntitiesModuleContext()
)
