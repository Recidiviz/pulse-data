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
from typing import Any, Dict, Optional, Union

from mock import create_autospec

from recidiviz.ingest.direct.controllers.base_direct_ingest_controller import \
    BaseDirectIngestController
from recidiviz.ingest.scrape.base_scraper import BaseScraper
from recidiviz.utils.regions import Region


def fake_region(*,
                region_code: str = 'us_xx',
                agency_type: str = 'prison',
                environment: str = 'local',
                jurisdiction_id: str = 'unknown',
                ingestor: Optional[Union[BaseScraper,
                                         BaseDirectIngestController]] = None,
                is_raw_vs_ingest_file_name_detection_enabled: bool = False,
                are_raw_data_bq_imports_enabled_in_env: bool = False,
                are_ingest_view_exports_enabled_in_env: bool = False,
                queue: Optional[Dict[str, Any]] = None,
                shared_queue: Optional[str] = None) -> Region:
    region = create_autospec(Region)
    region.region_code = region_code
    region.agency_type = agency_type
    region.environment = environment
    region.jurisdiction_id = jurisdiction_id
    region.get_ingestor.return_value = \
        ingestor if ingestor else create_autospec(BaseDirectIngestController)

    def fake_is_launched_in_env():
        return Region.is_ingest_launched_in_env(region)

    region.is_ingest_launched_in_env = fake_is_launched_in_env
    region.is_raw_vs_ingest_file_name_detection_enabled.return_value = is_raw_vs_ingest_file_name_detection_enabled
    region.are_raw_data_bq_imports_enabled_in_env.return_value = are_raw_data_bq_imports_enabled_in_env
    region.are_ingest_view_exports_enabled_in_env.return_value = are_ingest_view_exports_enabled_in_env
    region.queue = queue
    region.shared_queue = shared_queue
    return region


TEST_STATE_REGION = fake_region(region_code='us_xx', agency_type='prison')
TEST_COUNTY_REGION = fake_region(region_code='us_xx_yyyyy', agency_type='jail')
