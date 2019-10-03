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
from enum import Enum
from itertools import chain
from types import ModuleType
from typing import Any, Dict, Optional, Set, Union
from typing import List

import attr
import pytz
import yaml

from recidiviz.common.constants.enum_overrides import EnumOverrides
from recidiviz.utils import environment
from recidiviz.ingest.scrape import regions as scraper_regions_module
from recidiviz.ingest.direct import regions as direct_ingest_regions_module


class RemovedFromWebsite(Enum):
    RELEASED = 'RELEASED'
    UNKNOWN_SIGNIFICANCE = 'UNKNOWN_SIGNIFICANCE'

# Cache of the `Region` objects.
REGIONS: Dict[str, 'Region'] = {}

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
    """

    region_code: str = attr.ib()
    jurisdiction_id: str = attr.ib(validator=attr.validators.instance_of(str))
    agency_name: str = attr.ib()
    agency_type: str = attr.ib()
    environment: Union[str, bool] = attr.ib()
    timezone: tzinfo = attr.ib(converter=pytz.timezone)
    base_url: Optional[str] = attr.ib(default=None)
    shared_queue: Optional[str] = attr.ib(default=None)
    queue: Optional[Dict[str, Any]] = attr.ib(default=None)
    removed_from_website: RemovedFromWebsite = \
        attr.ib(default=RemovedFromWebsite.RELEASED,
                converter=RemovedFromWebsite)
    names_file: Optional[str] = attr.ib(default=None)
    should_proxy: Optional[bool] = attr.ib(default=False)
    is_stoppable: Optional[bool] = attr.ib(default=False)
    is_direct_ingest: Optional[bool] = attr.ib(default=False)

    def __attrs_post_init__(self):
        if self.queue and self.shared_queue:
            raise ValueError(
                'Only one of `queue` and `shared_queue` can be set.')
        if self.environment not in {*environment.GAE_ENVIRONMENTS, False}:
            raise ValueError('Invalid environment: {}'.format(self.environment))

    def get_ingestor(self):
        """Retrieve an ingest object for a particular region

        Returns:
            An instance of the region's ingest class (e.g., UsNyScraper)
        """
        ingest_module = 'direct' if self.is_direct_ingest else 'scrape'
        ingest_type_name = 'Controller' if self.is_direct_ingest else 'Scraper'

        module_name = \
            f'recidiviz.ingest.{ingest_module}.regions.{self.region_code}' \
            f'.{self.region_code}_{ingest_type_name.lower()}'
        module = importlib.import_module(module_name)
        ingest_class = getattr(
            module, get_ingestor_name(self.region_code, ingest_type_name))

        return ingest_class()

    def get_enum_overrides(self):
        """Retrieves the overrides object of a region"""
        obj = self.get_ingestor()
        if obj:
            return obj.get_enum_overrides()
        return EnumOverrides.empty()

    def get_queue_name(self):
        """Returns the name of the queue to be used for the region"""
        return self.shared_queue if self.shared_queue \
            else '{}-scraper-v2'.format(self.region_code.replace('_', '-'))

    def is_ingest_launched_in_env(self):
        """Returns true if ingest can be launched for this region in the current
        environment.

        If we are in prod, the region config must be explicitly set to specify
        this region can be run in prod. All regions can be triggered to run in
        staging.
        """
        return not environment.in_gae_production() \
            or self.environment == environment.get_gae_environment()

def get_region(region_code: str, is_direct_ingest: bool = False) -> Region:
    global REGIONS
    if region_code not in REGIONS:
        REGIONS[region_code] = Region(region_code=region_code,
                                      is_direct_ingest=is_direct_ingest,
                                      **get_region_manifest(region_code,
                                                            is_direct_ingest))
    return REGIONS[region_code]


MANIFEST_NAME = 'manifest.yaml'


def get_region_manifest(region_code: str,
                        is_direct_ingest: bool = False) -> Dict[str, Any]:
    """Gets manifest for a specific region

    Args:
        region_code: (string) Region code
        is_direct_ingest: (bool) Flag indicating to read the region info as if
            it's a direct ingest partner.

    Returns:
        Region manifest as dictionary
    """
    region_module = \
        direct_ingest_regions_module \
        if is_direct_ingest else scraper_regions_module
    with open(os.path.join(os.path.dirname(region_module.__file__),
                           region_code,
                           MANIFEST_NAME)) as region_manifest:
        return yaml.full_load(region_manifest)


def get_supported_scrape_region_codes(timezone: tzinfo = None) -> Set[str]:
    """Retrieve a list of known scraper regions / region codes

    Args:
        timezone: (str) If set, return only regions in the right timezone

    Returns:
        Set of region codes (strings)
    """
    return _get_supported_region_codes_for_base_region_module(
        scraper_regions_module,
        is_direct_ingest=False,
        timezone=timezone)


def get_supported_direct_ingest_region_codes() -> Set[str]:
    return _get_supported_region_codes_for_base_region_module(
        direct_ingest_regions_module,
        is_direct_ingest=True)


def _get_supported_region_codes_for_base_region_module(
        base_region_module: ModuleType,
        is_direct_ingest: bool,
        timezone: tzinfo = None):

    base_region_path = os.path.dirname(base_region_module.__file__)
    all_region_codes = {region_module.name for region_module
                        in pkgutil.iter_modules([base_region_path])}
    if timezone:
        dt = datetime.now()
        return {region_code for region_code in all_region_codes
                if timezone.utcoffset(dt) == \
                get_region(
                    region_code,
                    is_direct_ingest=is_direct_ingest
                ).timezone.utcoffset(dt)}
    return all_region_codes


def get_supported_regions() -> List['Region']:
    return get_supported_scrape_regions() + \
           get_supported_direct_ingest_regions()


def get_supported_scrape_regions() -> List['Region']:
    return [get_region(region_code, is_direct_ingest=False)
            for region_code in get_supported_scrape_region_codes()]


def get_supported_direct_ingest_regions() -> List['Region']:
    return [get_region(region_code, is_direct_ingest=True)
            for region_code in get_supported_direct_ingest_region_codes()]


def validate_region_code(region_code):
    """Verifies a region code is one of the Recidiviz supported regions.

    Args:
        region_code: (string) Region code (e.g., us_ny)

    Returns:
        True if valid region
        False if invalid region
    """
    return region_code in get_supported_scrape_region_codes()


def get_ingestor_name(region_code: str, ingest_type_name: str) -> str:
    """Returns the class name for a given region_code and ingest_module"""
    return ''.join(s.title() for s in
                   chain(region_code.split('_'), [ingest_type_name]))
