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
"""Helpers for creating fake regions for use in tests."""
from types import ModuleType
from typing import Optional

from mock import create_autospec

from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.utils.regions import Region


def fake_region(
    *,
    region_code: str = "us_xx",
    agency_type: str = "prison",
    environment: str = "local",
    jurisdiction_id: str = "unknown",
    scraper: Optional[BaseScraper] = None,
    region_module: Optional[ModuleType] = None,
    is_direct_ingest: bool = True,
    is_stoppable: Optional[bool] = None,
) -> Region:
    """Fake Region Object"""
    region = create_autospec(Region)
    region.region_code = region_code
    region.agency_type = agency_type
    region.environment = environment
    region.jurisdiction_id = jurisdiction_id
    region.region_module = region_module
    region.get_scraper.return_value = scraper
    region.is_direct_ingest = is_direct_ingest
    region.is_stoppable = is_stoppable

    def fake_is_launched_in_env() -> bool:
        return Region.is_ingest_launched_in_env(region)

    region.is_ingest_launched_in_env = fake_is_launched_in_env

    return region


TEST_STATE_REGION = fake_region(region_code="us_xx", agency_type="prison")
TEST_COUNTY_REGION = fake_region(
    region_code="us_xx_yyyyy",
    agency_type="jail",
    environment="production",
)
