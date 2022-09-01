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
from types import ModuleType
from typing import Any, Dict, Optional

import attr
import yaml

from recidiviz.ingest.direct import regions as direct_ingest_regions_module
from recidiviz.utils import environment
from recidiviz.utils.environment import GCPEnvironment

# Cache of the `DirectIngestRegion` objects.
REGIONS: Dict[str, "DirectIngestRegion"] = {}


def _to_lower(s: str) -> str:
    return s.lower()


@attr.s(frozen=True)
class DirectIngestRegion:
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

    def __attrs_post_init__(self) -> None:
        if self.environment not in {*environment.GCP_ENVIRONMENTS, None}:
            raise ValueError(f"Invalid environment: {self.environment}")

    def exists_in_env(self) -> bool:
        """Returns true if the ingest infrastructure (queues, databases, etc.) for this
        region exist in this environment.

        We don't create infrastructure for the playground regions in prod.
        """
        return not environment.in_gcp_production() or not self.playground

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


def get_direct_ingest_region(
    region_code: str, region_module_override: Optional[ModuleType] = None
) -> DirectIngestRegion:
    if region_module_override:
        region_module = region_module_override
    else:
        region_module = direct_ingest_regions_module

    if region_code not in REGIONS:
        REGIONS[region_code] = DirectIngestRegion(
            region_code=region_code.lower(),
            region_module=region_module,
            **get_direct_ingest_region_manifest(region_code.lower(), region_module),
        )
    return REGIONS[region_code]


MANIFEST_NAME = "manifest.yaml"


def get_direct_ingest_region_manifest(
    region_code: str, region_module: ModuleType
) -> Dict[str, Any]:
    """Gets manifest for a specific region

    Args:
        region_code: (string) Region code
        region_module: (ModuleType) The module where the ingest configuration for this
            region resides.

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
