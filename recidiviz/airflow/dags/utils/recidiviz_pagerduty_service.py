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
"""Information about PagerDuty services."""

import attr
from airflow.providers.sendgrid.utils.emailer import send_email

from recidiviz.airflow.dags.monitoring.airflow_alerting_incident import (
    AirflowAlertingIncident,
)
from recidiviz.airflow.dags.monitoring.recidiviz_alerting_service import (
    RecidivizAlertingService,
)
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode


@attr.define
class RecidivizPagerDutyService(RecidivizAlertingService):
    """Structure holding information about a service managed by the Recidiviz repo.
    For the full list of services, see
    https://recidiviz.pagerduty.com/service-directory.
    """

    name: str = attr.ib(validator=attr_validators.is_str)
    project_id: str = attr.ib(validator=attr_validators.is_str)
    service_integration_email: str = attr.ib(validator=attr_validators.is_str)

    @classmethod
    def data_platform_airflow_service(
        cls, project_id: str
    ) -> "RecidivizPagerDutyService":
        """Returns the service for alerts related to state-agnostic Airflow data
        platform task failures.
        """
        return RecidivizPagerDutyService(
            name="Data Platform PagerDuty Service",
            project_id=project_id,
            service_integration_email=cls._build_integration_email(
                "data-platform-airflow", project_id=project_id
            ),
        )

    @classmethod
    def polaris_airflow_service(cls, project_id: str) -> "RecidivizPagerDutyService":
        """Returns the service for alerts related to product-specific Airflow task failures."""
        return RecidivizPagerDutyService(
            name="Polaris Airflow Service",
            project_id=project_id,
            service_integration_email=cls._build_integration_email(
                "polaris-airflow", project_id=project_id
            ),
        )

    @classmethod
    def monitoring_airflow_service(cls, project_id: str) -> "RecidivizPagerDutyService":
        """Returns the service for alerts related to Airflow monitoring infrastructure
        failures.
        """
        return RecidivizPagerDutyService(
            name="Airflow Monitoring PagerDuty Service",
            project_id=project_id,
            service_integration_email=cls._build_integration_email(
                "monitoring-airflow", project_id=project_id
            ),
        )

    @classmethod
    def airflow_service_for_state_code(
        cls, project_id: str, state_code: StateCode
    ) -> "RecidivizPagerDutyService":
        """Returns the service for alerts related to non-raw data, state-specific
        Airflow data platform task failures for the given state_code.
        """
        state_code_str = state_code.value.lower().replace("_", "-")
        base_username = f"{state_code_str}-airflow"
        return RecidivizPagerDutyService(
            name=f"{state_code.value} Airflow (Default) PagerDuty Service",
            project_id=project_id,
            service_integration_email=cls._build_integration_email(
                base_username, project_id=project_id
            ),
        )

    @classmethod
    def raw_data_service_for_state_code(
        cls, project_id: str, state_code: StateCode
    ) -> "RecidivizPagerDutyService":
        """Returns the service for alerts related to state-specific raw data import
        task failures for the given state_code.
        """
        state_code_str = state_code.value.lower().replace("_", "-")
        base_username = f"{state_code_str}-airflow-raw-data"
        return RecidivizPagerDutyService(
            name=f"{state_code.value} Airflow (Raw Data) PagerDuty Service",
            project_id=project_id,
            service_integration_email=cls._build_integration_email(
                base_username, project_id=project_id
            ),
        )

    @staticmethod
    def _build_integration_email(base_username: str, project_id: str) -> str:
        return f"{base_username}-{project_id}@recidiviz.pagerduty.com"

    @staticmethod
    def _format_body(incident: AirflowAlertingIncident) -> str:
        failure_date_strs = ", ".join(
            [
                f"`{failure_date.isoformat()}`"
                for failure_date in incident.failed_execution_dates
            ]
        )

        detail_msg = (
            incident.error_message or ""
            if incident.next_success_date is None
            else f"Incident resolved {incident.next_success_date.isoformat()}"
        )

        return f"Failed run of [{incident.job_id}] on the following dates: [ {failure_date_strs} ]. {detail_msg}"

    def handle_incident(self, incident: AirflowAlertingIncident) -> None:
        event_type = "Failure:" if incident.next_success_date is None else "Success:"
        send_email(
            to=self.service_integration_email,
            subject=f"{event_type} {incident.unique_incident_id}",
            html_content=self._format_body(incident),
        )
