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
"""Alerting incident for stale raw data files."""
import datetime

import attr

from recidiviz.airflow.dags.monitoring.alerting_incident import AlertingIncident
from recidiviz.common import attr_validators


@attr.define
class StaleRawDataAlertingIncident(AlertingIncident):
    """An incident representing a stale raw data file for a specific region and file tag."""

    state_code: str = attr.ib(validator=attr_validators.is_str)
    file_tag: str = attr.ib(validator=attr_validators.is_str)
    hours_stale: float = attr.ib(validator=attr.validators.instance_of(float))
    most_recent_import_date: datetime.datetime = attr.ib(
        validator=attr.validators.instance_of(datetime.datetime)
    )

    @property
    def file_tag_incident_prefix(self) -> str:
        return f"Stale raw data: {self.file_tag}"

    @property
    def unique_incident_id(self) -> str:
        return f"{self.file_tag_incident_prefix}, last import: {self.most_recent_import_date.isoformat()}"

    @property
    def is_resolved(self) -> bool:
        return self.hours_stale <= 0
