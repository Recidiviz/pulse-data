# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Data model of an Airflow alerting incident"""
import json
from datetime import datetime
from functools import cached_property
from typing import Any, Dict, List, Optional

import attr


@attr.s(auto_attribs=True)
class AirflowAlertingIncident:
    """Representation of an alerting incident"""

    dag_id: str
    conf: str
    task_id: str
    failed_execution_dates: List[datetime] = attr.ib()

    # This value will never be more than INCIDENT_START_DATE_LOOKBACK ago,
    # even if it started failing before then.
    previous_success_date: Optional[datetime] = attr.ib(default=None)
    next_success_date: Optional[datetime] = attr.ib(default=None)

    @property
    def incident_start_date(self) -> datetime:
        return self.failed_execution_dates[0]

    @property
    def most_recent_failure(self) -> datetime:
        return self.failed_execution_dates[-1]

    @cached_property
    def conf_obj(self) -> Dict[str, Any]:
        return json.loads(self.conf)

    @property
    def unique_incident_id(self) -> str:
        """
        The PagerDuty email integration is configured to group incidents by their subject.
        Incidents can only be resolved once. Afterward, any new alerts will not re-open the incident.
        The unique incident id includes the last successful task run in order to group incidents by distinct sets of
        consecutive failures.
        """
        conf_string = f"{self.conf} " if self.conf != "{}" else ""
        start_date = self.incident_start_date.strftime("%Y-%m-%d %H:%M %Z")
        return f"{conf_string}{self.dag_id}.{self.task_id}, started: {start_date}"
