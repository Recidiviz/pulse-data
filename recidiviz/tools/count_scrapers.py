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

"""Counts scrapers in the repo by parent (vendor or BaseScraper) class."""

import inspect
from collections import Counter
from pprint import pprint

import us

from recidiviz.ingest.scrape import scraper as scraper_module
from recidiviz.utils import regions

GRANTEE_FIPS = [
    '13051', '17163', '20045', '21009', '21013', '21021', '21051', '21071',
    '21095', '21109', '21119', '21121', '21125', '21131', '21133', '21151',
    '21193', '21195', '21205', '21235', '26077', '28001', '28005', '28037',
    '28157', '36007', '37001', '37065', '37127', '37195', '42003', '42013',
    '42027', '42063', '47107', '47131', '47155', '48021', '48085', '48121',
    '48181', '48209', '48423', '48491'
]


def get_state(region: regions.Region) -> us.states.State:
    return us.states.lookup(region.region_code[3:5])


def get_parent_class(region: regions.Region) -> type:
    return inspect.getmro(region.get_ingestor_class())[1]


def count_scrapers_by_vendor():
    """Prints the number of scrapers of each parent class overall and for
    IOB target states."""
    scraper_module.ScraperCloudTaskManager = lambda: None  # type: ignore
    scrape_regions = regions.get_supported_scrape_regions()

    all_scrapers = Counter(
        get_parent_class(region).__name__ for region in scrape_regions)
    print(f"Of {sum(all_scrapers.values())} total scrapers:")
    pprint(all_scrapers)

    ky_scrapers = Counter(
        get_parent_class(region).__name__ for region in scrape_regions
        if get_state(region) is us.states.KY)
    print(f"Of {sum(ky_scrapers.values())} KY scrapers:")
    pprint(ky_scrapers)

    tn_scrapers = Counter(
        get_parent_class(region).__name__ for region in scrape_regions
        if get_state(region) is us.states.TN)
    print(f"Of {sum(tn_scrapers.values())} TN scrapers:")
    pprint(tn_scrapers)

    in_scrapers = Counter(
        get_parent_class(region).__name__ for region in scrape_regions
        if get_state(region) is us.states.IN)
    print(f"Of {sum(in_scrapers.values())} IN scrapers:")
    pprint(in_scrapers)

    grantee_scrapers = Counter(
        get_parent_class(region).__name__ for region in scrape_regions
        if region.jurisdiction_id[:5] in GRANTEE_FIPS)
    print(f"Of {sum(grantee_scrapers.values())} grantee scrapers:")
    pprint(grantee_scrapers)


if __name__ == '__main__':
    count_scrapers_by_vendor()
