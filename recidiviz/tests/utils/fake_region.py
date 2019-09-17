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
from typing import Optional, Union

from mock import create_autospec

from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.utils.regions import Region


def fake_region(*,
                region_code: str = 'us_ca',
                agency_type: str = 'prison',
                environment: str = 'local',
                jurisdiction_id: str = 'unknown',
                ingestor: Optional[Union[BaseScraper,
                                         BaseDirectIngestController]] = None):
    region = create_autospec(Region)
    region.region_code = region_code
    region.agency_type = agency_type
    region.environment = environment
    region.jurisdiction_id = jurisdiction_id
    region.get_ingestor.return_value = \
        ingestor if ingestor else create_autospec(BaseDirectIngestController)
    return region


TEST_STATE_REGION = fake_region(region_code='us_nd', agency_type='prison')
TEST_COUNTY_REGION = fake_region(region_code='us_tx_brazos', agency_type='jail')
