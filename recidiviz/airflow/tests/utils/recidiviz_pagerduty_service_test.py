# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for RecidivizPagerDutyService."""
import unittest
from typing import Dict, List

from recidiviz.airflow.dags.utils.recidiviz_pagerduty_service import (
    RecidivizPagerDutyService,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_existing_direct_ingest_states,
)
from recidiviz.utils.environment import GCP_PROJECTS


class TestRecidivizPagerDutyService(unittest.TestCase):
    """Tests for RecidivizPagerDutyService."""

    def test_services_for_each_deployed_state(self) -> None:
        """Tests that every deployed ingest state has a corresponding
        RecidivizPagerDutyService enum value defined. If this test fails and you've just
        added a new state, the RecidivizPagerDutyService enum needs to be updated.
        """
        for state_code in get_existing_direct_ingest_states():
            service = RecidivizPagerDutyService.airflow_service_for_state_code(
                project_id="recidiviz-456",
                state_code=state_code,
            )
            self.assertIsInstance(service, RecidivizPagerDutyService)

    def test_service_integration_email(self) -> None:
        self.assertEqual(
            "data-platform-airflow-recidiviz-456@recidiviz.pagerduty.com",
            RecidivizPagerDutyService.data_platform_airflow_service(
                project_id="recidiviz-456"
            ).service_integration_email,
        )
        self.assertEqual(
            "us-ca-airflow-recidiviz-456@recidiviz.pagerduty.com",
            RecidivizPagerDutyService.airflow_service_for_state_code(
                project_id="recidiviz-456", state_code=StateCode.US_CA
            ).service_integration_email,
        )
        self.assertEqual(
            "us-id-airflow-recidiviz-456@recidiviz.pagerduty.com",
            RecidivizPagerDutyService.airflow_service_for_state_code(
                project_id="recidiviz-456", state_code=StateCode.US_ID
            ).service_integration_email,
        )

    @classmethod
    def _all_services(cls, project_id: str) -> List[RecidivizPagerDutyService]:
        """Returns the list of all services managed for a given project."""
        return [
            RecidivizPagerDutyService.data_platform_airflow_service(
                project_id=project_id
            ),
            RecidivizPagerDutyService.monitoring_airflow_service(project_id=project_id),
            *[
                RecidivizPagerDutyService.airflow_service_for_state_code(
                    project_id=project_id, state_code=state_code
                )
                for state_code in get_existing_direct_ingest_states()
            ],
        ]

    def test_services_all_have_unique_emails(self) -> None:
        found_emails: Dict[str, RecidivizPagerDutyService] = {}
        for project_id in GCP_PROJECTS:
            for service in self._all_services(project_id):
                email = service.service_integration_email
                if email in found_emails:
                    raise ValueError(
                        f"Found duplicate emails for services [{service}] and "
                        f"[{found_emails[email]}]: {email}"
                    )
                found_emails[email] = service
