# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Tools for working with regions.

Regions represent geographic areas/legal jurisdictions from which we ingest
criminal justice data and calculate metrics.
"""
import os
import pkgutil
from enum import Enum
from types import ModuleType
from typing import Any, Dict, List, Optional, Set

import attr
import yaml

from recidiviz.ingest.direct import regions as direct_ingest_regions_module
from recidiviz.utils import environment
from recidiviz.utils.environment import GCPEnvironment


class RemovedFromWebsite(Enum):
    RELEASED = "RELEASED"
    UNKNOWN_SIGNIFICANCE = "UNKNOWN_SIGNIFICANCE"


# Cache of the `Region` objects.
REGIONS: Dict[str, "Region"] = {}


def _to_lower(s: str) -> str:
    return s.lower()


# TODO(#13703): Rename to DirectIngestRegion
@attr.s(frozen=True)
class Region:
    """Constructs region entity with attributes and helper functions

    Builds a region entity, which holds useful info about a region as properties
    and has helper functions to get region-specific configuration info.

    Attributes:
        region_code: (string) Region code
        agency_name: (string) Human-readable agency name
        region_module: (ModuleType) The module where the ingest configuration for this region resides.
        environment: (string) The environment the region is allowed to run in.
        playground: (bool) If this is a playground region and should only exist in staging.
    """

    region_code: str = attr.ib(converter=_to_lower)
    agency_name: str = attr.ib()
    region_module: ModuleType = attr.ib(default=None)
    environment: Optional[str] = attr.ib(default=None)
    playground: Optional[bool] = attr.ib(default=False)
    # TODO(#13703): Remove this flag since it is now always set to true
    is_direct_ingest: bool = attr.ib(default=True)

    def __attrs_post_init__(self) -> None:
        if self.environment not in {*environment.GCP_ENVIRONMENTS, None}:
            raise ValueError(f"Invalid environment: {self.environment}")

    def is_ingest_launched_in_env(self) -> bool:
        """Returns true if ingest can be launched for this region in the current
        environment.

        If we are in prod, the region config must be explicitly set to specify
        this region can be run in prod. All regions can be triggered to run in
        staging.
        """
        return (
            not environment.in_gcp_production()
            or self.environment == environment.get_gcp_environment()
        )

    def is_ingest_launched_in_production(self) -> bool:
        """Returns true if ingest can be launched for this region in production."""
        return (
            self.environment is not None
            and self.environment.lower() == GCPEnvironment.PRODUCTION.value.lower()
        )


# TODO(#13703): Rename to `get_direct_ingest_region()`
def get_region(
    region_code: str,
    is_direct_ingest: bool = True,
    region_module_override: Optional[ModuleType] = None,
) -> Region:
    if region_module_override:
        region_module = region_module_override
    else:
        region_module = direct_ingest_regions_module

    if region_code not in REGIONS:
        REGIONS[region_code] = Region(
            region_code=region_code.lower(),
            is_direct_ingest=is_direct_ingest,
            region_module=region_module,
            **get_region_manifest(region_code.lower(), region_module),
        )
    return REGIONS[region_code]


MANIFEST_NAME = "manifest.yaml"


def get_region_manifest(region_code: str, region_module: ModuleType) -> Dict[str, Any]:
    """Gets manifest for a specific region

    Args:
        region_code: (string) Region code

    Returns:
        Region manifest as dictionary
    """
    if region_module.__file__ is None:
        raise ValueError(f"No file associated with {region_module}.")
    with open(
        os.path.join(
            os.path.dirname(region_module.__file__), region_code, MANIFEST_NAME
        ),
        encoding="utf-8",
    ) as region_manifest:
        return yaml.full_load(region_manifest)


def get_supported_direct_ingest_region_codes() -> Set[str]:
    """Returns all direct ingest regions with defined packages in the
    `recidiviz/ingest/direct/regions` module.
    """

    base_region_module = direct_ingest_regions_module
    if base_region_module.__file__ is None:
        raise ValueError(f"No file associated with {base_region_module}.")
    base_region_path = os.path.dirname(base_region_module.__file__)

    return {
        region_module.name
        for region_module in pkgutil.iter_modules([base_region_path])
        if region_module.ispkg
    }


def get_supported_direct_ingest_regions() -> List["Region"]:
    return [
        get_region(region_code, is_direct_ingest=True)
        for region_code in get_supported_direct_ingest_region_codes()
    ]


# TODO(#13703): Move this to a more general location and rename to something like
#  'is_valid_python_dir()'.
def is_valid_region_directory(dir_path: str) -> bool:
    """Returns whether a directory path is valid: it points to an actual directory, the directory is
    non-empty, and it does not only contain a __pycache__ directory. It is possible to get into that
    situation when switching from a git branch that contains a region directory from one that
    doesn't, since __pycache__ directories are ignored by git."""
    return os.path.isdir(dir_path) and any(
        path != "__pycache__" and not path.startswith(".")
        for path in os.listdir(dir_path)
    )
