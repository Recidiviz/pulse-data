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
from types import ModuleType
from typing import Dict, Optional, Type, Union

from recidiviz.common.constants.enum_parser import EnumParser
from recidiviz.common.module_collector_mixin import ModuleCollectorMixin
from recidiviz.ingest.direct.controllers.custom_function_registry import (
    CustomFunctionRegistry,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_deserialize import EntityFactory
from recidiviz.persistence.entity.entity_utils import (
    get_entity_class_in_module_with_name,
)
from recidiviz.persistence.entity.state import (
    deserialize_entity_factories as state_deserialize_entity_factories,
)
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.utils.regions import Region


class IngestViewFileParserDelegate:
    """Interface and for a delegate that abstracts state/schema specific logic from the
    ingest view parser.
    """

    @abc.abstractmethod
    def get_ingest_view_manifest_path(self, file_tag: str) -> str:
        """Returns the path to the ingest view manifest for a given file tag."""

    @abc.abstractmethod
    def get_common_args(self) -> Dict[str, Optional[Union[str, EnumParser]]]:
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
    def get_custom_function_registry(self) -> CustomFunctionRegistry:
        """Returns an object that gives the parser access to custom python functions
        that can be used for parsing.
        """


def ingest_view_manifest_dir(region: Region) -> str:
    """Returns the directory where all ingest view manifests for a given region live."""
    # TODO(#9059): Move ingest view manifests out of top level region dir
    return os.path.join(
        os.path.dirname(region.region_module.__file__),
        region.region_code.lower(),
    )


def yaml_mappings_filepath(region: Region, file_tag: str) -> str:
    return os.path.join(
        ingest_view_manifest_dir(region),
        f"{region.region_code.lower()}_{file_tag}.yaml",
    )


class IngestViewFileParserDelegateImpl(
    IngestViewFileParserDelegate, ModuleCollectorMixin
):
    """Standard implementation of the IngestViewFileParserDelegate, for use in
    production code.
    """

    def __init__(self, region: Region, schema_type: SchemaType) -> None:
        self.region = region
        self.schema_type = schema_type
        self.entity_cls_cache: Dict[str, Type[Entity]] = {}

    def get_ingest_view_manifest_path(self, file_tag: str) -> str:
        return yaml_mappings_filepath(self.region, file_tag)

    def get_common_args(self) -> Dict[str, Optional[Union[str, EnumParser]]]:
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

    def get_custom_function_registry(self) -> CustomFunctionRegistry:
        region_code = self.region.region_code.lower()
        return CustomFunctionRegistry(
            custom_functions_root_module=self.get_relative_module(
                self.region.region_module, [region_code]
            )
        )
