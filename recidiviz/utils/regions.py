# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional, Set

import attr
import pytz
import yaml


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
        base_url: (string) Base URL for scraping
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
    agency_name: str = attr.ib()
    agency_type: str = attr.ib()
    base_url: str = attr.ib()
    timezone: str = attr.ib()
    shared_queue: Optional[str] = attr.ib(default=None)
    queue: Optional[Dict[str, Any]] = attr.ib(default=None)
    removed_from_website: RemovedFromWebsite = \
        attr.ib(default=RemovedFromWebsite.RELEASED,
                converter=RemovedFromWebsite)
    names_file: Optional[str] = attr.ib(default=None)

    def __attrs_post_init__(self):
        if self.queue and self.shared_queue:
            raise ValueError(
                'Only one of `queue` and `shared_queue` can be set.')

    def get_scraper_module(self):
        """Return the scraper module for this region

        Note that this is only the module containing the scraper and/or entity
        subclasses/models, not the scraper itself (use get_scraper() to get an
        instance of the scraper class).

        Some regions will use a general-purpose scraper that is not in the same
        package as the entity sub-kinds for that region. For such a region, you
        would pass different args into this method to get the scraper class
        versus the entity classes.

        Args:
            N/A

        Returns:
            Scraper module (e.g., recidiviz.ingest.scrape.regions.us_ny)
        """
        top_level = __import__("recidiviz")
        ingest_module = getattr(top_level, "ingest")
        scrape_module = getattr(ingest_module, "scrape")
        regions_module = getattr(scrape_module, "regions")
        scraper_module = getattr(regions_module, self.region_code)

        return scraper_module

    def get_scraper(self):
        """Retrieve a scraper object for a particular region

        Returns:
            An instance of the region's scraper class (e.g., UsNyScraper)
        """
        scraper_string = '{}_scraper'.format(self.region_code)
        scraper_module = self.get_scraper_module()
        scraper = getattr(scraper_module, scraper_string)
        scraper_class = getattr(scraper, ''.join(
            [s.title() for s in scraper_string.split('_')]))

        return scraper_class()

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


def get_supported_region_codes(timezone: str = None) -> Set[str]:
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
                if timezones_share_offset(
                    timezone, get_region(region_code).timezone, dt)}
    return all_region_codes


def timezones_share_offset(tz_name1: str, tz_name2: str, dt: datetime) -> bool:
    tz1 = pytz.timezone(tz_name1)
    tz2 = pytz.timezone(tz_name2)
    return tz1.utcoffset(dt) == tz2.utcoffset(dt)


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
