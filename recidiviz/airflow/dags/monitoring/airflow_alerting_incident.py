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
import pandas as pd
from pandas.api.typing import NaTType

from recidiviz.common import attr_validators


@attr.s(auto_attribs=True)
class AirflowAlertingIncident:
    """Representation of something distinct that went wrong during a series of
    consecutive DAG runs.


    The |previous_success_date| and |failed_execution_dates| will never be older than the
    lookback period provided to the incident history builder, even if the incident started
    failing  before then. If an incident's "true" |start_date| is older than the lookback,
    the same incident's unique_incident_id will change based on the run frequency of the
    DAG that is failing, as the lookback window will truncate the chain of failed tasks
    to the first one inside the lookback window.
    """

    dag_id: str = attr.ib(validator=attr_validators.is_str)
    dag_run_config: str = attr.ib(validator=attr_validators.is_str)
    job_id: str = attr.ib(validator=attr_validators.is_str)
    incident_type: str = attr.ib(validator=attr_validators.is_str)
    # sorted list of failed execution dates [ earliest date, ... , latest date ]
    failed_execution_dates: List[datetime] = attr.ib(
        validator=attr_validators.is_list_of(datetime)
    )
    previous_success_date: Optional[datetime] = attr.ib(
        default=None, validator=attr_validators.is_opt_datetime
    )
    next_success_date: Optional[datetime] = attr.ib(
        default=None, validator=attr_validators.is_opt_datetime
    )
    # since error messages can be quite large, don't include them in the airflow logs
    error_message: Optional[str] = attr.ib(
        default=None, validator=attr_validators.is_opt_str, repr=False
    )

    @property
    def incident_start_date(self) -> datetime:
        return self.failed_execution_dates[0]

    @property
    def most_recent_failure(self) -> datetime:
        return self.failed_execution_dates[-1]

    @cached_property
    def dag_run_config_obj(self) -> Dict[str, Any]:
        return json.loads(self.dag_run_config)

    @property
    def unique_incident_id(self) -> str:
        """The PagerDuty email integration is configured to group incidents by their subject.
        Incidents can only be resolved once. Afterward, any new alerts will not re-open the incident.
        The unique incident id includes the last successful job run in order to group incidents by distinct sets of
        consecutive failures.
        """
        conf_string = f"{self.dag_run_config} " if self.dag_run_config != "{}" else ""
        start_date = self.incident_start_date.strftime("%Y-%m-%d %H:%M %Z")
        return f"{self.incident_type}: {conf_string}{self.dag_id}.{self.job_id}, started: {start_date}"

    @classmethod
    def build(
        cls,
        *,
        dag_id: str,
        conf: str,
        job_id: str,
        failed_execution_dates: list[datetime],
        previous_success: pd.Timestamp | NaTType,
        next_success: pd.Timestamp | NaTType,
        incident_type: str,
        error_message: str | None,
    ) -> "AirflowAlertingIncident":
        return AirflowAlertingIncident(
            dag_id=dag_id,
            dag_run_config=conf,
            job_id=job_id,
            failed_execution_dates=sorted(failed_execution_dates),
            previous_success_date=(
                previous_success.to_pydatetime()
                if not pd.isna(previous_success)
                else None
            ),
            next_success_date=(
                next_success.to_pydatetime() if not pd.isna(next_success) else None
            ),
            error_message=error_message,
            incident_type=incident_type,
        )
