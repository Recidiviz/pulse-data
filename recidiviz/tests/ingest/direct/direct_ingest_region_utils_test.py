# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Tests for direct_ingest_region_utils"""
import unittest
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_project,
    get_direct_ingest_states_launched_in_env,
)
from recidiviz.tests.utils.fake_region import fake_region
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING


class TestDirectIngestRegionUtils(unittest.TestCase):
    """Tests for direct_ingest_region_utils."""

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch(
        "recidiviz.ingest.direct.regions.direct_ingest_region_utils.get_existing_direct_ingest_states"
    )
    @patch("recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region")
    def test_get_direct_ingest_states_launched_in_env_staging(
        self,
        mock_region: MagicMock,
        mock_direct_ingest_states: MagicMock,
        mock_environment: MagicMock,
    ) -> None:
        """Tests for get_direct_ingest_states_launched_in_env when in staging."""
        mock_environment.return_value = "staging"
        mock_region.return_value = fake_region(environment="staging")
        mock_direct_ingest_states.return_value = [StateCode["US_XX"]]

        self.assertEqual(
            get_direct_ingest_states_launched_in_env(), [StateCode["US_XX"]]
        )

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch(
        "recidiviz.ingest.direct.regions.direct_ingest_region_utils.get_existing_direct_ingest_states"
    )
    @patch("recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region")
    def test_get_direct_ingest_states_launched_in_env_production(
        self,
        mock_region: MagicMock,
        mock_direct_ingest_states: MagicMock,
        mock_environment: MagicMock,
    ) -> None:
        """Tests for get_direct_ingest_states_launched_in_env when in production."""
        mock_environment.return_value = "production"
        mock_region.return_value = fake_region(environment="production")
        mock_direct_ingest_states.return_value = [StateCode["US_XX"]]

        self.assertEqual(
            get_direct_ingest_states_launched_in_env(), [StateCode["US_XX"]]
        )

    @patch(
        "recidiviz.ingest.direct.regions.direct_ingest_region_utils.get_existing_direct_ingest_states"
    )
    @patch("recidiviz.ingest.direct.direct_ingest_regions.get_direct_ingest_region")
    def test_get_direct_ingest_states_existing_in_project(
        self,
        mock_region: MagicMock,
        mock_direct_ingest_states: MagicMock,
    ) -> None:
        """A non-playground state exists in every project; a playground state
        exists in staging but not production."""
        regions_by_code = {
            "us_xx": fake_region(region_code="us_xx", playground=False),
            "us_yy": fake_region(region_code="us_yy", playground=True),
        }
        mock_region.side_effect = lambda region_code: regions_by_code[region_code]
        mock_direct_ingest_states.return_value = [
            StateCode.US_XX,
            StateCode.US_YY,
        ]

        self.assertEqual(
            [StateCode.US_XX, StateCode.US_YY],
            get_direct_ingest_states_existing_in_project(
                project_id=GCP_PROJECT_STAGING
            ),
        )
        self.assertEqual(
            [StateCode.US_XX],
            get_direct_ingest_states_existing_in_project(
                project_id=GCP_PROJECT_PRODUCTION
            ),
        )
