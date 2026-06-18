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
"""Manifest compiler delegate for the identity ingest pipeline.

Analogous to StateSchemaIngestViewManifestCompilerDelegate in
recidiviz/ingest/direct/ingest_mappings/ingest_view_manifest_compiler_delegate.py.
"""

import os
from enum import Enum
from typing import Type

import recidiviz.pipelines.ingest.identity
from recidiviz.common.constants.identity import PersonType
from recidiviz.common.constants.tenants import Tenant
from recidiviz.common.demographics import Ethnicity, Gender, Race, Sex
from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.ingest_mappings.custom_function_registry import (
    CustomFunctionRegistry,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IS_LOCAL_PROPERTY_NAME,
    IS_PRODUCTION_PROPERTY_NAME,
    IS_STAGING_PROPERTY_NAME,
    IngestViewManifestCompilerDelegate,
)
from recidiviz.persistence.entity.base_entity import Entity, EntityT
from recidiviz.persistence.entity.entity_deserialize import (
    DeserializableEntityFieldValue,
    EntityFactory,
)
from recidiviz.persistence.entity.entity_utils import (
    get_entity_class_in_module_with_name,
)
from recidiviz.persistence.entity.identity import (
    identity_fragment_entities as identity_fragment_entities_module,
)
from recidiviz.persistence.entity.identity import (
    identity_fragment_entity_factories as identity_fragment_entity_factories_module,
)

_IDENTITY_MAPPINGS_DIR_NAME = "identity_mappings"

_REGIONS_DIR = os.path.dirname(regions.__file__)


class IdentityIngestViewManifestCompilerDelegate(
    IngestViewManifestCompilerDelegate,
):
    """Manifest compiler delegate for building IdentityFragment entity trees
    from identity mapping YAMLs stored under identity_mappings/ directories."""

    def __init__(self, tenant: Tenant) -> None:
        self.tenant = tenant
        self._entity_cls_cache: dict[str, Type[Entity]] = {}
        self._enum_cls_cache: dict[str, Type[Enum]] = {}

    def get_ingest_view_manifest_path(self, ingest_view_name: str) -> str:
        tenant_lower = self.tenant.value.lower()
        return os.path.join(
            _REGIONS_DIR,
            tenant_lower,
            _IDENTITY_MAPPINGS_DIR_NAME,
            f"{tenant_lower}_{ingest_view_name}.yaml",
        )

    def get_identity_ingest_view_names(self) -> list[str]:
        """Returns names of all identity mapping views for this tenant by scanning
        the identity_mappings directory."""
        identity_mappings_dir = os.path.join(
            _REGIONS_DIR, self.tenant.value.lower(), _IDENTITY_MAPPINGS_DIR_NAME
        )
        return self.get_ingest_view_names_from_mappings_dir(
            identity_mappings_dir, self.tenant.value
        )

    def get_env_property_type(self, property_name: str) -> Type:
        if property_name in (
            IS_LOCAL_PROPERTY_NAME,
            IS_STAGING_PROPERTY_NAME,
            IS_PRODUCTION_PROPERTY_NAME,
        ):
            return bool
        raise ValueError(f"Unexpected environment property: [{property_name}]")

    def get_common_args(self) -> dict[str, DeserializableEntityFieldValue]:
        return {"tenant": self.tenant}

    def get_entity_factory_class(self, entity_cls_name: str) -> Type[EntityFactory]:
        factory_cls_name = f"{entity_cls_name}Factory"
        factory_cls = getattr(
            identity_fragment_entity_factories_module, factory_cls_name, None
        )
        if factory_cls is None:
            raise ValueError(
                f"No factory class [{factory_cls_name}] found in "
                f"identity.identity_fragment_entity_factories."
            )
        return factory_cls

    def get_entity_cls(self, entity_cls_name: str) -> Type[Entity]:
        if entity_cls_name in self._entity_cls_cache:
            return self._entity_cls_cache[entity_cls_name]
        entity_cls = get_entity_class_in_module_with_name(
            identity_fragment_entities_module, entity_cls_name
        )
        self._entity_cls_cache[entity_cls_name] = entity_cls
        return entity_cls

    def get_enum_cls(self, enum_cls_name: str) -> Type[Enum]:
        if not self._enum_cls_cache:
            self._hydrate_enum_cls_cache()
        if enum_cls_name not in self._enum_cls_cache:
            raise ValueError(f"Unexpected enum class name [{enum_cls_name}].")
        return self._enum_cls_cache[enum_cls_name]

    def _hydrate_enum_cls_cache(self) -> None:
        for enum_cls in (Ethnicity, Gender, PersonType, Race, Sex):
            self._enum_cls_cache[enum_cls.__name__] = enum_cls

    def get_custom_function_registry(self) -> CustomFunctionRegistry:
        return CustomFunctionRegistry(
            custom_functions_root_module=recidiviz.pipelines.ingest.identity
        )

    def get_filter_if_null_field(self, entity_cls: Type[EntityT]) -> str | None:
        return None

    def is_json_field(self, entity_cls: Type[EntityT], field_name: str) -> bool:
        return False
