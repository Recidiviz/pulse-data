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
import importlib
import os
import pkgutil
from datetime import datetime, tzinfo
from enum import Enum, auto
from itertools import chain
from types import ModuleType
from typing import Any, Dict, List, Optional, Set

import attr
import pytz
import yaml

from recidiviz.common.attr_validators import is_str
from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.ingest.direct import regions as direct_ingest_regions_module
from recidiviz.ingest.scrape import regions as scraper_regions_module
from recidiviz.utils import environment
from recidiviz.utils.environment import GCPEnvironment


class RemovedFromWebsite(Enum):
    RELEASED = "RELEASED"
    UNKNOWN_SIGNIFICANCE = "UNKNOWN_SIGNIFICANCE"


class IngestType(Enum):
    DIRECT_INGEST = auto()
    SCRAPER = auto()


# Cache of the `Region` objects.
REGIONS: Dict[IngestType, Dict[str, "Region"]] = {}


def _to_lower(s: str) -> str:
    return s.lower()


@attr.s(frozen=True)
class Region:
    """Constructs region entity with attributes and helper functions

    Builds a region entity, which holds useful info about a region as properties
    and has helper functions to get region-specific configuration info.

    Attributes:
        region_code: (string) Region code
        agency_name: (string) Human-readable agency name
        agency_type: (string) 'prison' or 'jail'
        environment: (string) The environment the region is allowed to run in.
        base_url: (string) Base URL for scraping
        should_proxy: (string) Whether or not to send requests through the proxy
        timezone: (string) Timezone in which this region resides. If the region
            is in multiple timezones, this is the timezone in which most of the
            region resides, where "most" is whatever is most useful for that
            region, e.g. population count versus land size.
        shared_queue: (string) The name of an existing queue to use instead of
            creating one for this region.
        queue: (dict) Any parameters to override when creating the queue for
            this region.
        removed_from_website: (string) Value to use when a person is removed
            from a website (converted to `RemovedFromWebsite`).
        names_file: (string) Optional filename of names file for this region
        is_stoppable: (string) Whether or not this region is stoppable via the
            cron job /scraper/stop.
        stripe: (string) Stripe to which this region belongs to. This is used
            further divide up a timezone
        facility_id: (string) Default facility ID for region
    """

    region_code: str = attr.ib(converter=_to_lower)
    jurisdiction_id: str = attr.ib(validator=is_str)
    agency_name: str = attr.ib()
    agency_type: str = attr.ib()
    timezone: tzinfo = attr.ib(converter=pytz.timezone)
    region_module: ModuleType = attr.ib(default=None)
    environment: Optional[str] = attr.ib(default=None)
    base_url: str = attr.ib(default=None)
    shared_queue: Optional[str] = attr.ib(default=None)
    queue: Optional[Dict[str, Any]] = attr.ib(default=None)
    removed_from_website: RemovedFromWebsite = attr.ib(
        default=RemovedFromWebsite.RELEASED, converter=RemovedFromWebsite
    )
    names_file: Optional[str] = attr.ib(default=None)
    should_proxy: Optional[bool] = attr.ib(default=False)
    is_stoppable: Optional[bool] = attr.ib(default=False)
    is_direct_ingest: bool = attr.ib(default=False)
    stripe: Optional[str] = attr.ib(default="0")
    facility_id: Optional[str] = attr.ib(default=None)

    @region_code.validator
    def validate_region_code(self, _attr: attr.Attribute, region_code: str) -> None:
        # TODO(#6523): Re-enable this once all the regions have been fixed.
        # fips.validate_county_code(region_code)
        pass

    def __attrs_post_init__(self):
        if self.queue and self.shared_queue:
            raise ValueError("Only one of `queue` and `shared_queue` can be set.")
        if self.is_direct_ingest and (self.queue or self.shared_queue):
            raise ValueError(
                "Direct ingest regions may not have queues configured via yaml."
            )
        if self.environment not in {*environment.GCP_ENVIRONMENTS, None}:
            raise ValueError(f"Invalid environment: {self.environment}")
        if self.facility_id and len(self.facility_id) != 16:
            raise ValueError(
                f"Improperly formatted FID [{self.facility_id}], should be length 16"
            )

    def get_scraper_class(self):
        """Retrieve the class for the scraper for a particular region

        Returns:
            An instance of the region's scraper (e.g., UsNyScraper)
        """
        if self.is_direct_ingest:
            raise ValueError("Method not supported for direct ingest region.")

        ingest_type_name = "Scraper"

        module_name = (
            f"{self.region_module.__name__}.{self.region_code}"
            f".{self.region_code}_{ingest_type_name.lower()}"
        )

        module = importlib.import_module(module_name)
        ingest_class = getattr(module, get_scraper_name(self.region_code))

        return ingest_class

    def get_scraper(self):
        """Retrieve a scraper for a particular region

        Returns:
            An instance of the region's scraper (e.g., UsNyScraper)
        """
        if self.is_direct_ingest:
            raise ValueError("Method not supported for direct ingest region.")
        ingest_class = self.get_scraper_class()

        return ingest_class()

    def get_scraper_enum_overrides(self) -> EnumOverrides:
        """Retrieves the overrides object of a scraper region."""
        if self.is_direct_ingest:
            raise ValueError("Method not supported for direct ingest region.")
        obj = self.get_scraper()
        if obj:
            return obj.get_enum_overrides()
        return EnumOverrides.empty()

    def get_queue_name(self) -> str:
        """Returns the name of the queue to be used for the region"""
        return (
            self.shared_queue
            if self.shared_queue
            else f"{self.region_code.replace('_', '-')}-scraper-v2"
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

    def is_ingest_launched_in_production(self) -> bool:
        """Returns true if ingest can be launched for this region in production."""
        return (
            self.environment is not None
            and self.environment.lower() == GCPEnvironment.PRODUCTION.value.lower()
        )


def get_region(
    region_code: str,
    is_direct_ingest: bool = False,
    region_module_override: Optional[ModuleType] = None,
) -> Region:
    global REGIONS

    if region_module_override:
        region_module = region_module_override
    elif is_direct_ingest:
        region_module = direct_ingest_regions_module
    else:
        region_module = scraper_regions_module

    ingest_type = IngestType.DIRECT_INGEST if is_direct_ingest else IngestType.SCRAPER
    if ingest_type not in REGIONS:
        REGIONS[ingest_type] = {}

    if region_code not in REGIONS[ingest_type]:
        REGIONS[ingest_type][region_code] = Region(
            region_code=region_code.lower(),
            is_direct_ingest=is_direct_ingest,
            region_module=region_module,
            **get_region_manifest(region_code.lower(), region_module),
        )
    return REGIONS[ingest_type][region_code]


MANIFEST_NAME = "manifest.yaml"


def get_region_manifest(region_code: str, region_module: ModuleType) -> Dict[str, Any]:
    """Gets manifest for a specific region

    Args:
        region_code: (string) Region code

    Returns:
        Region manifest as dictionary
    """
    with open(
        os.path.join(
            os.path.dirname(region_module.__file__), region_code, MANIFEST_NAME
        ),
        encoding="utf-8",
    ) as region_manifest:
        return yaml.full_load(region_manifest)


def get_supported_scrape_region_codes(
    timezone: tzinfo = None, stripes: List[str] = None
) -> Set[str]:
    """Retrieve a list of known scraper regions / region codes

    Args:
        timezone: (str) If set, return only regions in the right timezone
        stripes: (List[str]) If set, return only regions in the right stripes

    Returns:
        Set of region codes (strings)
    """
    return _get_supported_region_codes_for_base_region_module(
        scraper_regions_module,
        is_direct_ingest=False,
        timezone=timezone,
        stripes=stripes,
    )


def get_supported_direct_ingest_region_codes() -> Set[str]:
    return _get_supported_region_codes_for_base_region_module(
        direct_ingest_regions_module, is_direct_ingest=True
    )


def _get_supported_region_codes_for_base_region_module(
    base_region_module: ModuleType,
    is_direct_ingest: bool,
    timezone: tzinfo = None,
    stripes: List[str] = None,
):
    """Returns all regions that support the given module type, e.g. direct ingest versus scraper. Will optionally
    filter on the additional arguments."""

    base_region_path = os.path.dirname(base_region_module.__file__)
    all_region_codes = {
        region_module.name for region_module in pkgutil.iter_modules([base_region_path])
    }
    if timezone:
        dt = datetime.now()
        filtered_regions = {
            region_code
            for region_code in all_region_codes
            if timezone.utcoffset(dt)
            == get_region(
                region_code, is_direct_ingest=is_direct_ingest
            ).timezone.utcoffset(dt)
        }
    else:
        filtered_regions = all_region_codes

    if stripes and len(stripes) > 0:
        return {
            region_code
            for region_code in filtered_regions
            if get_region(region_code, is_direct_ingest=is_direct_ingest).stripe
            in stripes
        }

    return filtered_regions


def get_supported_regions() -> List["Region"]:
    return get_supported_scrape_regions() + get_supported_direct_ingest_regions()


def get_supported_scrape_regions() -> List["Region"]:
    return [
        get_region(region_code, is_direct_ingest=False)
        for region_code in get_supported_scrape_region_codes()
    ]


def get_supported_direct_ingest_regions() -> List["Region"]:
    return [
        get_region(region_code, is_direct_ingest=True)
        for region_code in get_supported_direct_ingest_region_codes()
    ]


def validate_region_code(region_code):
    """Verifies a region code is one of the Recidiviz supported regions.

    Args:
        region_code: (string) Region code (e.g., us_ny)

    Returns:
        True if valid region
        False if invalid region
    """
    return region_code in get_supported_scrape_region_codes()


def get_scraper_name(region_code: str) -> str:
    """Returns the class name for a given region_code and ingest_module"""
    return "".join(s.title() for s in chain(region_code.split("_"), ["Scraper"]))
