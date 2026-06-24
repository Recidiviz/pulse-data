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
"""Manifest compiler delegate for the activity ingest pipeline."""

import inspect
from enum import Enum
from types import ModuleType
from typing import Type

from recidiviz.common.constants import state as state_constants
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.ingest_mappings.custom_function_registry import (
    CustomFunctionRegistry,
)
from recidiviz.ingest.direct.ingest_mappings.ingest_view_manifest_compiler_delegate import (
    IS_LOCAL_PROPERTY_NAME,
    IS_PRODUCTION_PROPERTY_NAME,
    IS_STAGING_PROPERTY_NAME,
    IngestViewManifestCompilerDelegate,
    yaml_mappings_filepath,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.persistence.entity.activity import (
    deserialize_entity_factories as state_deserialize_entity_factories,
)
from recidiviz.persistence.entity.activity import entities as state_entities
from recidiviz.persistence.entity.base_entity import Entity, EntityT
from recidiviz.persistence.entity.entity_deserialize import (
    DeserializableEntityFieldValue,
    EntityFactory,
)
from recidiviz.persistence.entity.entity_utils import (
    get_all_enum_classes_in_module,
    get_entity_class_in_module_with_name,
)


class ActivityIngestViewManifestCompilerDelegate(
    IngestViewManifestCompilerDelegate, ModuleCollectorMixin
):
    """Implementation of the IngestViewManifestCompilerDelegate for parsing ingest view
    mappings for the STATE schema.
    """

    def __init__(self, region: DirectIngestRegion) -> None:
        self.region = region
        self.entity_cls_cache: dict[str, Type[Entity]] = {}
        self.enum_cls_cache: dict[str, Type[Enum]] = {}

    def get_ingest_view_manifest_path(self, ingest_view_name: str) -> str:
        return yaml_mappings_filepath(
            region=self.region,
            ingest_view_name=ingest_view_name,
            ingest_pipeline_type=IngestPipelineType.ACTIVITY,
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
        # All entities in the state schema have the state_code field so we add this
        # as a common argument so we don't have to specify it in the yaml mappings.
        return {"state_code": self.region.region_code}

    def _get_deserialize_factories_module(self) -> ModuleType:
        return state_deserialize_entity_factories

    def _get_entities_module(self) -> ModuleType:
        return state_entities

    def _get_enums_modules(self) -> list[ModuleType]:
        return [state_constants]

    def _get_enum_submodule_prefix_filter(self) -> str:
        return "state"

    def get_entity_factory_class(self, entity_cls_name: str) -> Type[EntityFactory]:
        factory_entity_name = f"{entity_cls_name}Factory"
        factory_cls = getattr(
            self._get_deserialize_factories_module(), factory_entity_name
        )
        return factory_cls

    def get_entity_cls(self, entity_cls_name: str) -> Type[Entity]:
        if entity_cls_name in self.entity_cls_cache:
            return self.entity_cls_cache[entity_cls_name]

        entity_cls = get_entity_class_in_module_with_name(
            self._get_entities_module(), entity_cls_name
        )
        self.entity_cls_cache[entity_cls_name] = entity_cls
        return entity_cls

    def get_enum_cls(self, enum_cls_name: str) -> Type[Enum]:
        if not self.enum_cls_cache:
            self._hydrate_enum_cls_cache()

        if enum_cls_name not in self.enum_cls_cache:
            raise ValueError(f"Unexpected enum class name [{enum_cls_name}].")

        return self.enum_cls_cache[enum_cls_name]

    def _hydrate_enum_cls_cache(self) -> None:
        enums_root_modules = self._get_enums_modules()

        for enums_root_module in enums_root_modules:
            enum_file_modules = ModuleCollectorMixin.get_submodules(
                enums_root_module,
                submodule_name_prefix_filter=self._get_enum_submodule_prefix_filter(),
            )
            for enum_module in enum_file_modules:
                enum_classes = get_all_enum_classes_in_module(enum_module)
                for enum_cls in enum_classes:
                    if enum_cls.__name__ in self.enum_cls_cache:
                        raise ValueError(
                            f"Found duplicate enum already added to the cache: "
                            f"[{enum_cls.__name__}]"
                        )
                    self.enum_cls_cache[enum_cls.__name__] = enum_cls
                # Also register any module-level alias names (e.g. StateGender = Gender)
                # so that YAML mappings referencing the alias name still resolve.
                for attr_name, attr_val in vars(enum_module).items():
                    if (
                        inspect.isclass(attr_val)
                        and attr_val in enum_classes
                        and attr_name != attr_val.__name__
                    ):
                        self.enum_cls_cache.setdefault(attr_name, attr_val)

    def get_custom_function_registry(self) -> CustomFunctionRegistry:
        region_code = self.region.region_code.lower()
        return CustomFunctionRegistry(
            custom_functions_root_module=self.get_relative_module(
                self.region.region_module, [region_code]
            )
        )

    def get_filter_if_null_field(self, entity_cls: Type[EntityT]) -> str | None:
        if issubclass(entity_cls, state_entities.StatePersonAlias):
            return "full_name"
        return None

    def is_json_field(self, entity_cls: Type[EntityT], field_name: str) -> bool:
        """Returns whether a string field is expected to contain JSON. Whenever we add
        a new string field that should hold JSON to the STATE schema, this function
        should be updated to return True for that field (if it does not already).
        """
        return field_name.endswith("_metadata") or field_name.endswith("full_name")
