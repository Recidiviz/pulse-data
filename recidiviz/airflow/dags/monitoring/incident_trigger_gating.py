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
"""Predicates for whether an incident should be sent to PagerDuty"""
import enum
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional, Tuple

import attr

from recidiviz.airflow.dags.monitoring.airflow_alerting_incident import (
    AirflowAlertingIncident,
)
from recidiviz.airflow.dags.monitoring.dag_registry import (
    get_calculation_dag_id,
    get_raw_data_import_dag_id,
    get_sftp_dag_id,
)
from recidiviz.airflow.dags.utils.branch_utils import BRANCH_END_TASK_NAME
from recidiviz.airflow.dags.utils.environment import get_project_id


class TriggerPredicateMethod(enum.Enum):
    SILENCE = "silence"
    PRECONDITION = "precondition"


@attr.s(auto_attribs=True)
class AlertingIncidentTriggerPredicate:
    """Predicate containing rules for incident inclusion"""

    method: TriggerPredicateMethod
    condition: Callable[
        [AirflowAlertingIncident],
        bool,
    ]
    failure_message: str
    dag_id: Optional[str] = attr.ib(default=None)


def _incident_has_been_updated_recently(incident: AirflowAlertingIncident) -> bool:
    """Incidents are only reported to PagerDuty if they are "active" (opened, still
    occurring, or resolved) within the last two days. This has no intentional functional
    difference and is only done to save on SendGrid email quota.

    [!!] edge case: if the monitoring DAG is down for more than 2 days, some auto-resolve
    emails will be missed
    """
    now_utc = datetime.now(tz=timezone.utc)
    most_recent_update = max(
        incident.previous_success_date or datetime.fromtimestamp(0, tz=timezone.utc),
        incident.most_recent_failure,
        incident.next_success_date or datetime.fromtimestamp(0, tz=timezone.utc),
    )

    return timedelta(days=2) > now_utc - most_recent_update


def _get_trigger_predicates() -> List[AlertingIncidentTriggerPredicate]:
    project_id = get_project_id()
    return [
        AlertingIncidentTriggerPredicate(
            method=TriggerPredicateMethod.PRECONDITION,
            condition=_incident_has_been_updated_recently,
            failure_message="incident has not occurred recently",
        ),
        AlertingIncidentTriggerPredicate(
            method=TriggerPredicateMethod.PRECONDITION,
            dag_id=get_calculation_dag_id(project_id),
            condition=lambda incident: (
                "ingest_instance" in incident.dag_run_config_obj
                and incident.dag_run_config_obj["ingest_instance"] == "PRIMARY"
            ),
            failure_message="incident is not for the primary ingest instance",
        ),
        AlertingIncidentTriggerPredicate(
            method=TriggerPredicateMethod.SILENCE,
            condition=lambda incident: incident.job_id.endswith(BRANCH_END_TASK_NAME),
            failure_message="branch_end is not an actionable failure",
        ),
        AlertingIncidentTriggerPredicate(
            method=TriggerPredicateMethod.SILENCE,
            dag_id=get_sftp_dag_id(project_id),
            condition=lambda incident: (
                incident.job_id == "US_IX.remote_file_download.download_sftp_files"
                and len(incident.failed_execution_dates) == 1
            ),
            failure_message="must fail at least twice",
        ),
        # TODO(#28239) remove once we actually want to route secondary failures to pager duty
        AlertingIncidentTriggerPredicate(
            method=TriggerPredicateMethod.SILENCE,
            dag_id=get_raw_data_import_dag_id(project_id),
            condition=lambda incident: (
                "ingest_instance" in incident.dag_run_config_obj
                and incident.dag_run_config_obj["ingest_instance"] == "SECONDARY"
            ),
            failure_message="raw data import DAG secondary is being used for testing",
        ),
    ]


def should_trigger_airflow_alerting_incident(
    incident: AirflowAlertingIncident,
) -> Tuple[bool, List[str]]:
    """Returns whether the incident should be triggered and a list of reasons why if it should not
    In order for an incident to be triggered:
    * All TriggerPredicateMethod.PRECONDITION predicate conditions must be true
    * All TriggerPredicateMethod.SILENCE predicate conditions must be false
    """
    predicates = [
        predicate
        for predicate in _get_trigger_predicates()
        # Gather predicates that apply to all DAGs, or ones specific to this DAG
        if predicate.dag_id is None or incident.dag_id == predicate.dag_id
    ]

    failure_messages = [
        predicate.failure_message
        for predicate in predicates
        if (
            predicate.method == TriggerPredicateMethod.PRECONDITION
            and not predicate.condition(incident)
        )
        or (
            predicate.method == TriggerPredicateMethod.SILENCE
            and predicate.condition(incident)
        )
    ]

    return len(failure_messages) == 0, failure_messages
