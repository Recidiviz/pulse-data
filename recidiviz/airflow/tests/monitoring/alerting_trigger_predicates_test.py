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
"""Tests for trigger predicates"""
import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from recidiviz.airflow.dags.monitoring.airflow_alerting_incident import (
    AirflowAlertingIncident,
)
from recidiviz.airflow.dags.monitoring.incident_trigger_gating import (
    should_trigger_airflow_alerting_incident,
)
from recidiviz.airflow.dags.utils.branch_utils import BRANCH_END_TASK_NAME


@patch.dict(os.environ, {"GCP_PROJECT": "test_project"})
class TestPredicates(unittest.TestCase):
    """Tests for Airflow alerting trigger predicates"""

    def test_happy_path(self) -> None:
        incident = AirflowAlertingIncident(
            dag_id="test_project_calculation_dag",
            dag_run_config=json.dumps({"ingest_instance": "PRIMARY"}),
            job_id="update_managed_views_all.execute_entrypoint_operator",
            failed_execution_dates=[datetime.now(tz=timezone.utc)],
            incident_type="Task Run",
        )

        should_trigger, messages = should_trigger_airflow_alerting_incident(incident)
        self.assertTrue(should_trigger, messages)

    def test_state_code_branch_end(self) -> None:
        incident = AirflowAlertingIncident(
            dag_id="test_project_calculation_dag",
            dag_run_config="{}",
            job_id=f"post_ingest_pipelines.{BRANCH_END_TASK_NAME}",
            failed_execution_dates=[datetime.now(tz=timezone.utc)],
            incident_type="Task Run",
        )

        should_trigger, messages = should_trigger_airflow_alerting_incident(incident)
        self.assertFalse(should_trigger, messages)
        self.assertIn("branch_end is not an actionable failure", messages)

        incident = AirflowAlertingIncident(
            dag_id="test_project_another_dag",
            dag_run_config="{}",
            job_id=f"post_ingest_pipelines.{BRANCH_END_TASK_NAME}",
            failed_execution_dates=[datetime.now(tz=timezone.utc)],
            incident_type="Task Run",
        )

        should_trigger, messages = should_trigger_airflow_alerting_incident(incident)
        self.assertFalse(should_trigger, messages)
        self.assertIn("branch_end is not an actionable failure", messages)

    def test_secondary_dag(self) -> None:
        incident = AirflowAlertingIncident(
            dag_id="test_project_calculation_dag",
            dag_run_config=json.dumps({"ingest_instance": "SECONDARY"}),
            job_id="update_managed_views_all.execute_entrypoint_operator",
            failed_execution_dates=[datetime.now(tz=timezone.utc)],
            incident_type="Task Run",
        )

        should_trigger, messages = should_trigger_airflow_alerting_incident(incident)
        self.assertTrue(should_trigger, messages)

        incident = AirflowAlertingIncident(
            dag_id="test_project_calculation_dag",
            dag_run_config="",
            job_id="update_managed_views_all.execute_entrypoint_operator",
            failed_execution_dates=[datetime.now(tz=timezone.utc)],
            incident_type="Task Run",
        )

        should_trigger, messages = should_trigger_airflow_alerting_incident(incident)
        self.assertTrue(should_trigger, messages)

    def test_stale_incident(self) -> None:
        incident = AirflowAlertingIncident(
            dag_id="test_project_calculation_dag",
            dag_run_config=json.dumps({"ingest_instance": "PRIMARY"}),
            job_id="update_managed_views_all.execute_entrypoint_operator",
            failed_execution_dates=[datetime.now(tz=timezone.utc) - timedelta(days=14)],
            incident_type="Task Run",
        )

        should_trigger, messages = should_trigger_airflow_alerting_incident(incident)
        self.assertFalse(should_trigger, messages)
        self.assertIn("incident has not occurred recently", messages)

    def test_raw_data_secondary(self) -> None:
        incident = AirflowAlertingIncident(
            dag_id="test_project_raw_data_import_dag",
            dag_run_config=json.dumps({"ingest_instance": "SECONDARY"}),
            job_id="US_OZ.hunger_games_people",
            failed_execution_dates=[datetime.now(tz=timezone.utc) - timedelta(days=1)],
            incident_type="Task Run",
        )

        should_trigger, _ = should_trigger_airflow_alerting_incident(incident)
        self.assertTrue(should_trigger)

    def test_raw_data_tasks_allowed_to_fail(self) -> None:
        incident = AirflowAlertingIncident(
            dag_id="test_project_raw_data_import_dag",
            dag_run_config=json.dumps({"ingest_instance": "PRIMARY"}),
            job_id="raw_data_branching.us_xx_primary_import_branch.pre_import_normalization.raise_file_chunking_errors",
            failed_execution_dates=[datetime.now(tz=timezone.utc) - timedelta(days=1)],
            incident_type="Task Run",
        )

        should_trigger, messages = should_trigger_airflow_alerting_incident(incident)
        self.assertFalse(should_trigger, messages)
        self.assertIn(
            "errors for this task will be handled by file-level errors", messages
        )

        incident = AirflowAlertingIncident(
            dag_id="test_project_raw_data_import_dag",
            dag_run_config=json.dumps({"ingest_instance": "PRIMARY"}),
            job_id="raw_data_branching.us_xx_primary_import_branch.raise_operations_registration_errors",
            failed_execution_dates=[datetime.now(tz=timezone.utc) - timedelta(days=1)],
            incident_type="Task Run",
        )

        should_trigger, messages = should_trigger_airflow_alerting_incident(incident)
        self.assertTrue(should_trigger)
