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
from typing import Any, Dict, Optional, Set, Union

import attr
import pytz
import yaml

from recidiviz.utils import environment


class RemovedFromWebsite(Enum):
    RELEASED = 'RELEASED'
    UNKNOWN_SIGNIFICANCE = 'UNKNOWN_SIGNIFICANCE'


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

    def __attrs_post_init__(self):
        if self.queue and self.shared_queue:
            raise ValueError(
                'Only one of `queue` and `shared_queue` can be set.')
        if self.environment not in {*environment.GAE_ENVIRONMENTS, False}:
            raise ValueError('Invalid environment: {}'.format(self.environment))

    def get_scraper(self):
        """Retrieve a scraper object for a particular region

        Returns:
            An instance of the region's scraper class (e.g., UsNyScraper)
        """
        scraper_module_name = \
            'recidiviz.ingest.scrape.regions.{region}.{region}_scraper'.format(
                region=self.region_code
            )
        scraper_module = importlib.import_module(scraper_module_name)
        scraper_class = getattr(scraper_module,
                                scraper_class_name(self.region_code))

        return scraper_class()

    def get_enum_overrides(self):
        """Retrieves the overrides object of a region"""
        return self.get_scraper().get_enum_overrides()

    def get_queue_name(self):
        """Returns the name of the queue to be used for the region"""
        return self.shared_queue if self.shared_queue \
            else '{}-scraper'.format(self.region_code.replace('_', '-'))

# Cache of the `Region` objects.
REGIONS: Dict[str, 'Region'] = {}
def get_region(region_code: str) -> Region:
    global REGIONS
    if not region_code in REGIONS:
        REGIONS[region_code] = Region(region_code=region_code,
                                      **get_region_manifest(region_code))
    return REGIONS[region_code]


BASE_REGION_PATH = 'recidiviz/ingest/scrape/regions'
MANIFEST_NAME = 'manifest.yaml'

def get_region_manifest(region_code: str) -> Dict[str, Any]:
    """Gets manifest for a specific region

    Args:
        region_code: (string) Region code

    Returns:
        Region manifest as dictionary
    """
    with open(os.path.join(BASE_REGION_PATH, region_code, MANIFEST_NAME)) \
            as region_manifest:
        return yaml.load(region_manifest)


def get_supported_region_codes(timezone: tzinfo = None) -> Set[str]:
    """Retrieve a list of known scraper regions / region codes

    Args:
        timezone: (str) If set, return only regions in the right timezone

    Returns:
        Set of region codes (strings)
    """
    all_region_codes = {region_module.name for region_module
                        in pkgutil.iter_modules([BASE_REGION_PATH])}
    if timezone:
        dt = datetime.now()
        return {region_code for region_code in all_region_codes
                if timezone.utcoffset(dt) == \
                    get_region(region_code).timezone.utcoffset(dt)}
    return all_region_codes


def get_supported_regions():
    return [get_region(region_code)
            for region_code in get_supported_region_codes()]


def validate_region_code(region_code):
    """Verifies a region code is one of the Recidiviz supported regions.

    Args:
        region_code: (string) Region code (e.g., us_ny)

    Returns:
        True if valid region
        False if invalid region
    """
    return region_code in get_supported_region_codes()

def scraper_class_name(region_code: str) -> str:
    """Returns the class name for a given region_code)"""
    return ''.join(s.title() for s in region_code.split('_')) + 'Scraper'
