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

"""Utils file for ingest module"""
import logging
from datetime import tzinfo
from typing import List, Optional, Set, Union

import pytz

from recidiviz.ingest.scrape import constants
from recidiviz.utils import environment, regions


def lookup_timezone(timezone: Optional[str]) -> Optional[tzinfo]:
    return pytz.timezone(timezone) if timezone else None


def _regions_matching_environment(region_codes: Set[str]) -> Set[str]:
    """Filter to regions with the matching environment.

    If we are running locally, include all supported regions.
    """
    if not environment.in_gcp():
        return region_codes
    gcp_env = environment.get_gcp_environment()
    return {
        region_code
        for region_code in region_codes
        if regions.get_region(region_code).environment == gcp_env
    }


def validate_regions(
    region_codes: List[str], timezone: tzinfo = None, stripes: List[str] = None
) -> Union[Set[str], bool]:
    """Validates the region arguments.

    If any region in |region_codes| is "all", then all supported regions will be
    returned.

    Args:
        region_codes: List of regions from URL parameters
        timezone: If set, returns only regions in the matching timezone
        stripes: If set, returns only regions in the matching stripes

    Returns:
        False if invalid regions
        List of regions to scrape if successful
    """
    region_codes_output = set(region_codes)

    supported_regions = regions.get_supported_scrape_region_codes(
        timezone=timezone, stripes=stripes
    )
    for region in region_codes:
        if region == "all":
            # We only do this if all is passed to still allow people to manually
            # run any scrapers they wish.
            region_codes_output = _regions_matching_environment(supported_regions)
        elif region not in supported_regions:
            logging.error("Region '%s' not recognized.", region)
            return False

    return region_codes_output


def validate_scrape_types(scrape_type_list: List[str]) -> List[constants.ScrapeType]:
    """Validates the scrape type arguments.

    If any scrape type in |scrape_type_list| is "all", then all supported scrape
    types will be returned.

    Args:
        scrape_type_list: List of scrape types from URL parameters

    Returns:
        False if invalid scrape types
        List of scrape types if successful
    """
    if not scrape_type_list:
        return [constants.ScrapeType.BACKGROUND]

    scrape_types_output = []
    all_types = False
    for scrape_type in scrape_type_list:
        if scrape_type == "all":
            all_types = True
        else:
            try:
                scrape_types_output.append(constants.ScrapeType(scrape_type))
            except ValueError:
                logging.error("Scrape type '%s' not recognized.", scrape_type)
                return []

    return list(constants.ScrapeType) if all_types else scrape_types_output
