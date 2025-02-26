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

import attr

from recidiviz.airflow.dags.monitoring.airflow_alerting_incident import (
    AirflowAlertingIncident,
)
from recidiviz.airflow.dags.monitoring.alerting_trigger_predicates import (
    should_trigger_airflow_alerting_incident,
)
from recidiviz.airflow.dags.utils.branching_by_key import BRANCH_END_TASK_NAME


@patch.dict(os.environ, {"GCP_PROJECT": "test_project"})
class TestPredicates(unittest.TestCase):
    """Tests for Airflow alerting trigger predicates"""

    def test_happy_path(self) -> None:
        incident = AirflowAlertingIncident(
            dag_id="test_project_calculation_dag",
            conf=json.dumps({"ingest_instance": "PRIMARY"}),
            task_id="update_managed_views_all.execute_entrypoint_operator",
            failed_execution_dates=[datetime.now(tz=timezone.utc)],
        )

        should_trigger, messages = should_trigger_airflow_alerting_incident(incident)
        self.assertTrue(should_trigger, messages)

    def test_sftp(self) -> None:
        incident = AirflowAlertingIncident(
            dag_id="test_project_sftp_dag",
            conf="{}",
            task_id="US_IX.remote_file_download.download_sftp_files",
            failed_execution_dates=[datetime.now(tz=timezone.utc)],
        )

        should_trigger, messages = should_trigger_airflow_alerting_incident(incident)
        self.assertFalse(should_trigger, messages)
        self.assertIn("must fail at least twice", messages)

        incident = attr.evolve(
            incident,
            failed_execution_dates=[
                datetime.now(tz=timezone.utc),
                datetime.now(tz=timezone.utc),
            ],
        )

        should_trigger, messages = should_trigger_airflow_alerting_incident(incident)
        self.assertTrue(should_trigger, messages)
        self.assertEqual(len(messages), 0)

    def test_state_code_branch_end(self) -> None:
        incident = AirflowAlertingIncident(
            dag_id="test_project_calculation_dag",
            conf="{}",
            task_id=f"post_normalization_pipelines.{BRANCH_END_TASK_NAME}",
            failed_execution_dates=[datetime.now(tz=timezone.utc)],
        )

        should_trigger, messages = should_trigger_airflow_alerting_incident(incident)
        self.assertFalse(should_trigger, messages)
        self.assertIn("state_code_branch_end is not an actionable failure", messages)

    def test_secondary_dag(self) -> None:
        incident = AirflowAlertingIncident(
            dag_id="test_project_calculation_dag",
            conf=json.dumps({"ingest_instance": "SECONDARY"}),
            task_id="update_managed_views_all.execute_entrypoint_operator",
            failed_execution_dates=[datetime.now(tz=timezone.utc)],
        )

        should_trigger, messages = should_trigger_airflow_alerting_incident(incident)
        self.assertFalse(should_trigger, messages)
        self.assertIn("incident is not for the primary ingest instance", messages)

    def test_stale_incident(self) -> None:
        incident = AirflowAlertingIncident(
            dag_id="test_project_calculation_dag",
            conf=json.dumps({"ingest_instance": "PRIMARY"}),
            task_id="update_managed_views_all.execute_entrypoint_operator",
            failed_execution_dates=[datetime.now(tz=timezone.utc) - timedelta(days=14)],
        )

        should_trigger, messages = should_trigger_airflow_alerting_incident(incident)
        self.assertFalse(should_trigger, messages)
        self.assertIn("incident has not occurred recently", messages)
