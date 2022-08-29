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

from recidiviz.ingest.direct.direct_ingest_regions import DirectIngestRegion


def fake_region(
    *,
    region_code: str = "us_xx",
    environment: str = "local",
    region_module: Optional[ModuleType] = None,
) -> DirectIngestRegion:
    """Fake Region Object"""
    region = create_autospec(DirectIngestRegion)
    region.region_code = region_code
    region.environment = environment
    region.region_module = region_module

    def fake_is_launched_in_env() -> bool:
        return DirectIngestRegion.is_ingest_launched_in_env(region)

    region.is_ingest_launched_in_env = fake_is_launched_in_env

    return region


TEST_STATE_REGION = fake_region(region_code="us_xx")
