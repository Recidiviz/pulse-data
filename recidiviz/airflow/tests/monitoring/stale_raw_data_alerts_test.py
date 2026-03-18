# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Tests for stale raw data alerting."""
import unittest
from datetime import datetime, timedelta, timezone
from types import ModuleType
from unittest.mock import MagicMock, patch

from recidiviz.airflow.dags.monitoring.stale_raw_data_alerts import (
    report_stale_raw_data_to_github,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
    get_region_raw_file_config,
)
from recidiviz.tests.ingest.direct import fake_regions


class TestReportStaleRawDataToGitHub(unittest.TestCase):
    """Tests for stale_raw_data_alerts."""

    def setUp(self) -> None:
        def get_region_config(
            region_code: str, _region_module: ModuleType | None = None
        ) -> DirectIngestRegionRawFileConfig:
            return get_region_raw_file_config(region_code, region_module=fake_regions)

        self.get_region_config_patcher = patch(
            "recidiviz.airflow.dags.monitoring.stale_raw_data_alerts.get_region_raw_file_config",
            side_effect=get_region_config,
        )
        self.get_region_config_patcher.start()

        self.in_gcp_production_patcher = patch(
            "recidiviz.airflow.dags.monitoring.stale_raw_data_alerts.in_gcp_production",
            return_value=True,
        )
        self.in_gcp_production_patcher.start()

        self.get_project_id_patcher = patch(
            "recidiviz.airflow.dags.monitoring.stale_raw_data_alerts.get_project_id",
            return_value="recidiviz-test",
        )
        self.get_project_id_patcher.start()

        self.mock_service = MagicMock()
        self.get_service_patcher = patch(
            "recidiviz.airflow.dags.monitoring.stale_raw_data_alerts.StaleRawDataGitHubService.get_stale_raw_data_service_for_state_code",
            return_value=self.mock_service,
        )
        self.get_service_patcher.start()

        self.get_all_referenced_file_tags_patcher = patch(
            "recidiviz.airflow.dags.monitoring.stale_raw_data_alerts.get_all_referenced_file_tags",
            return_value={"tagBasicData"},
        )
        self.get_all_referenced_file_tags_patcher.start()

    def tearDown(self) -> None:
        self.get_region_config_patcher.stop()
        self.in_gcp_production_patcher.stop()
        self.get_project_id_patcher.stop()
        self.get_service_patcher.stop()
        self.get_all_referenced_file_tags_patcher.stop()

    @patch(
        "recidiviz.airflow.dags.monitoring.stale_raw_data_alerts.get_direct_ingest_states_launched_in_env"
    )
    def test_stale_file_creates_incident(
        self, mock_get_launched_states: MagicMock
    ) -> None:
        mock_get_launched_states.return_value = [StateCode.US_XX, StateCode.US_DD]
        current_utc = datetime.now(timezone.utc)

        update_datetimes_by_region = {
            "US_XX": {
                # US_XX default cadence is WEEKLY -> max_hours_before_stale = 7*24+12 = 180h
                "tagBasicData": (current_utc - timedelta(hours=181)).isoformat(),
                # should ignore file tags that have been deleted
                "deleted_tag": (current_utc - timedelta(hours=1000)).isoformat(),
            },
            "US_DD": {
                # US_DD has default_update_cadence: IRREGULAR, so should ignore
                "table1": (current_utc - timedelta(days=999)).isoformat(),
            },
        }

        report_stale_raw_data_to_github(update_datetimes_by_region)

        self.mock_service.handle_incident.assert_called_once()
        incident = self.mock_service.handle_incident.call_args[0][0]
        self.assertEqual(incident.state_code, "US_XX")
        self.assertEqual(incident.file_tag, "tagBasicData")
        self.assertGreater(incident.hours_stale, 0)
        self.assertAlmostEqual(incident.hours_stale, 1.0, places=1)

    @patch(
        "recidiviz.airflow.dags.monitoring.stale_raw_data_alerts.get_direct_ingest_states_launched_in_env"
    )
    def test_fresh_file(self, mock_get_launched_states: MagicMock) -> None:
        mock_get_launched_states.return_value = [StateCode.US_XX]
        current_utc = datetime.now(timezone.utc)
        # US_XX default cadence is WEEKLY -> max_hours_before_stale = 7*24+12 = 180h
        update_datetime = (current_utc - timedelta(hours=100)).isoformat()

        update_datetimes_by_region = {
            "US_XX": {
                "tagBasicData": update_datetime,
            }
        }

        report_stale_raw_data_to_github(update_datetimes_by_region)

        # we should still call `handle_incident` for fresh files so we can close any existing incidents
        self.mock_service.handle_incident.assert_called_once()
        incident = self.mock_service.handle_incident.call_args[0][0]
        self.assertEqual(incident.state_code, "US_XX")
        self.assertEqual(incident.file_tag, "tagBasicData")
        self.assertEqual(incident.hours_stale, 0.0)

    @patch(
        "recidiviz.airflow.dags.monitoring.stale_raw_data_alerts.get_direct_ingest_states_launched_in_env"
    )
    def test_skips_unreferenced_file_tags(
        self, mock_get_launched_states: MagicMock
    ) -> None:
        mock_get_launched_states.return_value = [StateCode.US_XX]
        self.get_all_referenced_file_tags_patcher.stop()
        with patch(
            "recidiviz.airflow.dags.monitoring.stale_raw_data_alerts.get_all_referenced_file_tags",
            return_value=set(),
        ):
            current_utc = datetime.now(timezone.utc)

            update_datetimes_by_region = {
                "US_XX": {
                    "tagBasicData": (current_utc - timedelta(hours=181)).isoformat(),
                }
            }

            report_stale_raw_data_to_github(update_datetimes_by_region)

        self.mock_service.handle_incident.assert_not_called()
        self.get_all_referenced_file_tags_patcher.start()

    def test_skips_non_production_environment(self) -> None:
        with patch(
            "recidiviz.airflow.dags.monitoring.stale_raw_data_alerts.in_gcp_production",
            return_value=False,
        ):
            current_utc = datetime.now(timezone.utc)

            update_datetimes_by_region = {
                "US_XX": {
                    "tagBasicData": (current_utc - timedelta(hours=181)).isoformat(),
                }
            }

            report_stale_raw_data_to_github(update_datetimes_by_region)

        self.mock_service.handle_incident.assert_not_called()
