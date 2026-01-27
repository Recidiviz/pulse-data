# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for SFTP ingest ready file timeliness monitoring."""

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
    get_region_raw_file_config,
)
from recidiviz.monitoring.keys import AttributeKey
from recidiviz.monitoring.sftp_ingest_ready_file_timeliness import (
    report_sftp_ingest_ready_file_timeliness_metrics,
)
from recidiviz.tests.ingest.direct import fake_regions
from recidiviz.tests.utils.monitoring_test_utils import OTLMock


class TestReportSftpIngestReadyFileTimelinessMetrics(unittest.TestCase):
    """Tests for report_sftp_ingest_ready_file_timeliness_metrics function."""

    def setUp(self) -> None:
        self.project_id_patcher = patch(
            "recidiviz.utils.metadata.project_id",
            return_value="recidiviz-test",
        )
        self.project_id_patcher.start()

        self.in_gcp_patcher = patch(
            "recidiviz.utils.environment.in_gcp",
            return_value=True,
        )
        self.in_gcp_patcher.start()

        def get_region_config(region_code: str) -> DirectIngestRegionRawFileConfig:
            return get_region_raw_file_config(region_code, region_module=fake_regions)

        self.get_region_config_patcher = patch(
            "recidiviz.monitoring.sftp_ingest_ready_file_timeliness.get_region_raw_file_config",
            side_effect=get_region_config,
        )
        self.get_region_config_patcher.start()

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        self.in_gcp_patcher.stop()
        self.get_region_config_patcher.stop()

    @patch(
        "recidiviz.monitoring.sftp_ingest_ready_file_timeliness.states_with_sftp_delegates"
    )
    def test_some_stale_regions(
        self,
        mock_states_with_sftp_delegates: MagicMock,
    ) -> None:
        """Tests that gauge observations are emitted for stale regions only."""
        current_utc_datetime = datetime.now(timezone.utc)

        upload_times_by_region_code = {
            # States have a weekly upload cadence
            "US_XX": (current_utc_datetime - timedelta(days=8.9)).isoformat(),  # stale
            "US_YY": (current_utc_datetime - timedelta(days=8)).isoformat(),  # fresh
            "US_LL": (current_utc_datetime - timedelta(days=8.8)).isoformat(),  # stale
        }

        mock_states_with_sftp_delegates.return_value = {
            StateCode.US_XX,
            StateCode.US_YY,
            StateCode.US_LL,
            # Should ignore states with no upload times
            StateCode.US_DD,
        }

        otl_mock = OTLMock()
        otl_mock.set_up()

        # Create a mock gauge to track set() calls
        mock_gauge = MagicMock()
        with patch(
            "recidiviz.monitoring.sftp_ingest_ready_file_timeliness.get_monitoring_instrument",
            return_value=mock_gauge,
        ):
            report_sftp_ingest_ready_file_timeliness_metrics(
                upload_times_by_region_code
            )

        expected_stale_states = {
            StateCode.US_XX,
            StateCode.US_LL,
        }

        self.assertEqual(mock_gauge.set.call_count, len(expected_stale_states))

        reported_states = set()
        for call in mock_gauge.set.call_args_list:
            # Verify amount (hours_stale) is positive
            self.assertGreater(call.kwargs["amount"], 0)
            reported_states.add(call.kwargs["attributes"][AttributeKey.REGION])

        self.assertEqual(
            reported_states, {state.value for state in expected_stale_states}
        )
