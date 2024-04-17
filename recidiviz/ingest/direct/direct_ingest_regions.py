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
from collections import defaultdict
from types import ModuleType
from typing import Any, Dict, Optional

import attr
import yaml

from recidiviz.common.attr_converters import lowercase_str
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import regions as direct_ingest_regions_module
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import environment, metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING

# Cache of the `DirectIngestRegion` objects.
_REGIONS: Dict[str, Dict[str, "DirectIngestRegion"]] = defaultdict(dict)


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

    region_code: str = attr.ib(converter=lowercase_str)
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
        return (
            not (
                # TODO(#15073): This checks both the RECIDIVIZ_ENV environment variable
                # and the project id because the environment variable won't be set in
                # local scripts, like when we are running migrations. We should consider
                # getting rid of the environment variable entirely and just always
                # using project id throughout the codebase.
                environment.in_gcp_production()
                or metadata.running_against(GCP_PROJECT_PRODUCTION)
            )
            or not self.playground
        )

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


def get_direct_ingest_region(
    region_code: str, region_module_override: Optional[ModuleType] = None
) -> DirectIngestRegion:
    region_code = region_code.lower()
    if region_module_override:
        region_module = region_module_override
    else:
        region_module = direct_ingest_regions_module

    region_module_str = region_module.__name__
    if region_code not in _REGIONS[region_module_str]:
        _REGIONS[region_module_str][region_code] = DirectIngestRegion(
            region_code=region_code,
            region_module=region_module,
            **get_direct_ingest_region_manifest(region_code, region_module),
        )
    return _REGIONS[region_module_str][region_code]


@environment.test_only
def clear_direct_ingest_regions_cache() -> None:
    _REGIONS.clear()


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


# TODO(#12390): Delete once raw data pruning is live.
def raw_data_pruning_enabled_in_state_and_instance(
    state_code: StateCode,
    instance: DirectIngestInstance,
) -> bool:
    return metadata.project_id() == GCP_PROJECT_STAGING and (
        state_code == StateCode.US_OZ
        or (
            state_code == StateCode.US_CO and instance == DirectIngestInstance.SECONDARY
        )
    )
