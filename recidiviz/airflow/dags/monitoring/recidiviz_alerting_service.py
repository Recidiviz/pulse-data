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
"""Interface for sending an AirflowAlertingIncident to an alerting backend"""
import abc

from recidiviz.airflow.dags.monitoring.airflow_alerting_incident import (
    AirflowAlertingIncident,
)


class RecidivzAlertingService:

    name: str

    @abc.abstractmethod
    def handle_incident(self, incident: AirflowAlertingIncident) -> None:
        """Update the alerting backend with updated information about this incident.
        This incident may be a new incident that the service has never seen before,
        or one that is now resolved and may need to be cleaned up in some capacity.
        """
