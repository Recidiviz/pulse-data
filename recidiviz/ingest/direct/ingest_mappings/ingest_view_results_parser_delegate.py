# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Interface and implementation for a delegate that abstracts state/schema specific
logic from the ingest view parser.
"""

import abc
import os
from enum import Enum
from types import ModuleType
from typing import Callable, Dict, List, Optional, Type

from recidiviz.common.constants import state as state_constants
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.ingest.direct.ingest_mappings.custom_function_registry import (
    CustomFunctionRegistry,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_deserialize import (
    DeserializableEntityFieldValue,
    EntityFactory,
    EntityT,
)
from recidiviz.persistence.entity.entity_utils import (
    get_all_enum_classes_in_module,
    get_entity_class_in_module_with_name,
)
from recidiviz.persistence.entity.state import (
    deserialize_entity_factories as state_deserialize_entity_factories,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.utils import environment
from recidiviz.utils.regions import Region


class IngestViewResultsParserDelegate:
    """Interface and for a delegate that abstracts state/schema specific logic from the
    ingest view parser.
    """

    @abc.abstractmethod
    def get_ingest_view_manifest_path(self, ingest_view_name: str) -> str:
        """Returns the path to the ingest view manifest for a given file tag."""

    @abc.abstractmethod
    def get_common_args(self) -> Dict[str, DeserializableEntityFieldValue]:
        """Returns a dictionary containing any fields, with their corresponding values,
        that should be set on every entity produced by the parser.
        """

    @abc.abstractmethod
    def get_entity_factory_class(self, entity_cls_name: str) -> Type[EntityFactory]:
        """Returns the factory class that can be used to instantiate an entity with the
        provided class name.
        """

    @abc.abstractmethod
    def get_entity_cls(self, entity_cls_name: str) -> Type[Entity]:
        """Returns the class for a given entity name"""

    @abc.abstractmethod
    def get_enum_cls(self, enum_cls_name: str) -> Type[Enum]:
        """Returns the class for a given enum name"""

    @abc.abstractmethod
    def get_custom_function_registry(self) -> CustomFunctionRegistry:
        """Returns an object that gives the parser access to custom python functions
        that can be used for parsing.
        """

    @abc.abstractmethod
    def get_env_property(self, property_name: str) -> bool:
        """Returns a boolean value associated with an environment property with the
        given name. Throws ValueError for all unexpected properties.
        """

    @abc.abstractmethod
    def get_filter_predicate(
        self, entity_cls: Type[EntityT]
    ) -> Optional[Callable[[EntityT], bool]]:
        """Returns a predicate function which can be used to fully filter the evaluated
        EntityTreeManifest from the result.
        """


_INGEST_VIEW_MANIFESTS_SUBDIR = "ingest_mappings"

# Supported $env properties
IS_PRODUCTION_PROPERTY_NAME = "is_production"
IS_PRIMARY_INSTANCE_PROPERTY_NAME = "is_primary_instance"
IS_SECONDARY_INSTANCE_PROPERTY_NAME = "is_secondary_instance"


def ingest_view_manifest_dir(region: Region) -> str:
    """Returns the directory where all ingest view manifests for a given region live."""
    if region.region_module.__file__ is None:
        raise ValueError(f"No file associated with {region.region_module}.")
    return os.path.join(
        os.path.dirname(region.region_module.__file__),
        region.region_code.lower(),
        _INGEST_VIEW_MANIFESTS_SUBDIR,
    )


def yaml_mappings_filepath(region: Region, ingest_view_name: str) -> str:
    return os.path.join(
        ingest_view_manifest_dir(region),
        f"{region.region_code.lower()}_{ingest_view_name}.yaml",
    )


class IngestViewResultsParserDelegateImpl(
    IngestViewResultsParserDelegate, ModuleCollectorMixin
):
    """Standard implementation of the IngestViewFileParserDelegate, for use in
    production code.
    """

    def __init__(
        self,
        region: Region,
        schema_type: SchemaType,
        ingest_instance: DirectIngestInstance,
    ) -> None:
        self.region = region
        self.schema_type = schema_type
        self.ingest_instance = ingest_instance
        self.entity_cls_cache: Dict[str, Type[Entity]] = {}
        self.enum_cls_cache: Dict[str, Type[Enum]] = {}

    def get_ingest_view_manifest_path(self, ingest_view_name: str) -> str:
        return yaml_mappings_filepath(self.region, ingest_view_name)

    def get_common_args(self) -> Dict[str, DeserializableEntityFieldValue]:
        if self.schema_type == SchemaType.STATE:
            # All entities in the state schema have the state_code field - we add this
            # as a common argument so we don't have to specify it in the yaml mappings.
            return {"state_code": self.region.region_code}
        if self.schema_type == SchemaType.JAILS:
            return {}
        raise ValueError(f"Unexpected schema type [{self.schema_type}]")

    def _get_deserialize_factories_module(self) -> ModuleType:
        if self.schema_type == SchemaType.STATE:
            return state_deserialize_entity_factories
        raise ValueError(f"Unexpected schema type [{self.schema_type}]")

    def _get_entities_module(self) -> ModuleType:
        if self.schema_type == SchemaType.STATE:
            return state_entities
        raise ValueError(f"Unexpected schema type [{self.schema_type}]")

    def _get_enums_modules(self) -> List[ModuleType]:
        if self.schema_type == SchemaType.STATE:
            return [state_constants]
        raise ValueError(f"Unexpected schema type [{self.schema_type}]")

    def _get_enum_submodule_prefix_filter(self) -> str:
        if self.schema_type == SchemaType.STATE:
            return "state"
        raise ValueError(f"Unexpected schema type [{self.schema_type}]")

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
                for enum_cls in get_all_enum_classes_in_module(enum_module):
                    if enum_cls.__name__ in self.enum_cls_cache:
                        raise ValueError(
                            f"Found duplicate enum already added to the cache: "
                            f"[{enum_cls.__name__}]"
                        )
                    self.enum_cls_cache[enum_cls.__name__] = enum_cls

    def get_custom_function_registry(self) -> CustomFunctionRegistry:
        region_code = self.region.region_code.lower()
        return CustomFunctionRegistry(
            custom_functions_root_module=self.get_relative_module(
                self.region.region_module, [region_code]
            )
        )

    def get_env_property(self, property_name: str) -> bool:
        if property_name == IS_PRODUCTION_PROPERTY_NAME:
            return environment.in_gcp_production()
        if property_name == IS_PRIMARY_INSTANCE_PROPERTY_NAME:
            return self.ingest_instance == DirectIngestInstance.PRIMARY
        if property_name == IS_SECONDARY_INSTANCE_PROPERTY_NAME:
            return self.ingest_instance == DirectIngestInstance.SECONDARY

        raise ValueError(f"Unexpected environment property: [{property_name}]")

    # TODO(#8905): Consider using more general logic to build a filter predicate, like
    #  building a @required field annotation for fields that must be hydrated, otherwise
    #  the whole entity is filtered out.
    def get_filter_predicate(
        self, entity_cls: Type[EntityT]
    ) -> Optional[Callable[[EntityT], bool]]:
        if issubclass(entity_cls, state_entities.StatePersonAlias):

            def state_person_alias_filter_predicate(e: EntityT) -> bool:
                return getattr(e, "full_name") is None

            return state_person_alias_filter_predicate
        return None
