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
from recidiviz.ingest.direct.direct_ingest_region_utils import (
    get_direct_ingest_states_with_sftp_queue,
    get_direct_ingest_states_launched_in_env,
)
from recidiviz.tests.utils.fake_region import fake_region


class TestDirectIngestRegionUtils(unittest.TestCase):
    """Tests for direct_ingest_region_utils."""

    @patch("recidiviz.utils.environment.get_gcp_environment")
    @patch(
        "recidiviz.ingest.direct.direct_ingest_region_utils.get_existing_direct_ingest_states"
    )
    @patch("recidiviz.utils.regions.get_region")
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
        "recidiviz.ingest.direct.direct_ingest_region_utils.get_existing_direct_ingest_states"
    )
    @patch("recidiviz.utils.regions.get_region")
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

    def test_get_direct_ingest_states_with_sftp_queue(self) -> None:
        """Tests the get_direct_ingest_states_with_sftp_queue returns the correct state codes"""
        state_codes = set(get_direct_ingest_states_with_sftp_queue())
        self.assertEqual(state_codes, {StateCode.US_ID})
