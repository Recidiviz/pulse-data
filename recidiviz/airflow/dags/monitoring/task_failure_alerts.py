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
"""Functionality for reporting consecutive failures of tasks as incidents to PagerDuty"""
import logging
from datetime import timedelta
from pprint import pprint

from recidiviz.airflow.dags.monitoring.dag_registry import get_all_dag_ids
from recidiviz.airflow.dags.monitoring.incident_alert_routing import (
    get_alerting_services_for_incident,
)
from recidiviz.airflow.dags.monitoring.incident_history_builder import (
    IncidentHistoryBuilder,
)
from recidiviz.airflow.dags.monitoring.incident_trigger_gating import (
    should_trigger_airflow_alerting_incident,
)
from recidiviz.airflow.dags.utils.email import can_send_mail
from recidiviz.airflow.dags.utils.environment import (
    get_composer_environment,
    get_project_id,
    is_experiment_environment,
)

INCIDENT_START_DATE_LOOKBACK = timedelta(days=21)


def report_failed_tasks() -> None:
    """Reports unique task failure incidents to PagerDuty.
    If the task has succeeded since the incident was opened, we send with the subject `Task success: `
    which resolves the open incident in PagerDuty.
    """
    for dag_id in get_all_dag_ids(project_id=get_project_id()):

        logging.info("Building task history for DAG: %s", dag_id)

        incident_history = IncidentHistoryBuilder(dag_id=dag_id).build(
            lookback=INCIDENT_START_DATE_LOOKBACK
        )

        # Print the incident history for use when reviewing task logs
        pprint(incident_history)

        if is_experiment_environment() and not can_send_mail():
            logging.info(
                "Cannot report incidents to PagerDuty in %s as Sendgrid is not configured",
                get_composer_environment(),
            )
            return

        for incident in incident_history.values():
            should_trigger, messages = should_trigger_airflow_alerting_incident(
                incident
            )

            for message in messages:
                logging.info(
                    "Skipping reporting of incident: %s, reason: %s",
                    incident.unique_incident_id,
                    message,
                )

            if not should_trigger:
                continue

            alerting_services = get_alerting_services_for_incident(incident)

            for service in alerting_services:
                logging.info(
                    "Reporting incident %s to %s",
                    incident.unique_incident_id,
                    service.name,
                )
                service.handle_incident(incident)
