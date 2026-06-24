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
"""Interface for a delegate that abstracts state/schema specific logic from the
IngestViewManifestCompiler, plus shared helpers used by every concrete
delegate.
"""

import abc
import os
from enum import Enum
from typing import Dict, List, Optional, Type

from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion
from recidiviz.ingest.direct.ingest_mappings.custom_function_registry import (
    CustomFunctionRegistry,
)
from recidiviz.ingest.direct.types.ingest_pipeline_type import IngestPipelineType
from recidiviz.persistence.entity.base_entity import Entity, EntityT
from recidiviz.persistence.entity.entity_deserialize import (
    DeserializableEntityFieldValue,
    EntityFactory,
)


class IngestViewManifestCompilerDelegate:
    """Interface for a delegate that abstracts state/schema specific logic from the
    IngestViewManifestCompiler.
    """

    @abc.abstractmethod
    def get_ingest_view_manifest_path(self, ingest_view_name: str) -> str:
        """Returns the path to the ingest view manifest for a given ingest name."""

    @abc.abstractmethod
    def get_env_property_type(self, property_name: str) -> Type:
        """Returns the expected value type for the given env property (i.e. the type
        of the value returned by IngestViewContentsContext.get_env_property()).
        """

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
    def get_filter_if_null_field(self, entity_cls: Type[EntityT]) -> Optional[str]:
        """Returns a field (if there is one) where, if for any entity this field's value
        evaluates to None, that entity should be filtered out of the result.
        """

    @abc.abstractmethod
    def is_json_field(self, entity_cls: Type[EntityT], field_name: str) -> bool:
        """Returns whether a string field is expected to contain JSON. Whenever we add
        a new string field that should hold JSON to the schema associated with this
        delegate, this function should be updated to return True for that field.
        """

    @staticmethod
    def get_ingest_view_names_from_mappings_dir(
        mappings_dir: str, region_or_tenant: str
    ) -> List[str]:
        """Returns sorted ingest view names by scanning a mappings directory for
        YAML files matching the {region_or_tenant}_{view_name}.yaml pattern."""
        if not os.path.isdir(mappings_dir):
            return []
        prefix = f"{region_or_tenant.lower()}_"
        return sorted(
            filename[len(prefix) : -len(".yaml")]
            for filename in os.listdir(mappings_dir)
            if filename.startswith(prefix) and filename.endswith(".yaml")
        )


# Supported $env properties
IS_LOCAL_PROPERTY_NAME = "is_local"
IS_STAGING_PROPERTY_NAME = "is_staging"
IS_PRODUCTION_PROPERTY_NAME = "is_production"


def ingest_view_manifest_dir(
    region: DirectIngestRegion, ingest_pipeline_type: IngestPipelineType
) -> str:
    """Returns the directory where the given ingest pipeline's manifest YAMLs
    live for a given region (e.g. `ingest_mappings/` for activity,
    `identity_mappings/` for identity)."""
    if region.region_module.__file__ is None:
        raise ValueError(f"No file associated with {region.region_module}.")
    return os.path.join(
        os.path.dirname(region.region_module.__file__),
        region.region_code.lower(),
        ingest_pipeline_type.manifest_subdir_name,
    )


def yaml_mappings_filepath(
    *,
    region: DirectIngestRegion,
    ingest_view_name: str,
    ingest_pipeline_type: IngestPipelineType,
) -> str:
    """Returns the path to the YAML manifest for a single ingest view, for a
    given region and ingest pipeline type."""
    return os.path.join(
        ingest_view_manifest_dir(region, ingest_pipeline_type),
        f"{region.region_code.lower()}_{ingest_view_name}.yaml",
    )
