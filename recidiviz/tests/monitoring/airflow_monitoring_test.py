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
# =============================================================================
"""Tests for airflow age metrics"""
import datetime
import unittest
from unittest.mock import patch

from google.cloud.orchestration.airflow.service_v1 import Environment
from opentelemetry.metrics import CallbackOptions, Observation

from recidiviz.monitoring.airflow_monitoring import get_airflow_environment_ages
from recidiviz.monitoring.keys import AttributeKey
from recidiviz.tests.utils.monitoring_test_utils import OTLMock


class TestAirflowAgeMetrics(unittest.TestCase):
    """Tests for airflow age metrics"""

    def setUp(self) -> None:
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = "test-project"

        self.otl_mock = OTLMock()
        self.otl_mock.set_up()

        self.client_patcher = patch(
            "google.cloud.orchestration.airflow.service_v1.EnvironmentsClient"
        )
        self.client_patcher.start().return_value.list_environments.return_value = [
            Environment(
                name="experiment-dan",
                create_time=datetime.datetime(
                    year=1970,
                    month=1,
                    day=2,
                    hour=0,
                    second=0,
                    tzinfo=datetime.timezone.utc,
                ),
            ),
            Environment(
                name="orchestration-v2",
                create_time=datetime.datetime(
                    year=1970,
                    month=1,
                    day=1,
                    hour=0,
                    second=0,
                    tzinfo=datetime.timezone.utc,
                ),
            ),
        ]

    def tearDown(self) -> None:
        self.project_id_patcher.stop()
        self.client_patcher.stop()
        self.otl_mock.tear_down()

    def test_get_airflow_environment_ages(self) -> None:
        results = list(get_airflow_environment_ages(CallbackOptions()))

        self.assertCountEqual(
            results,
            [
                Observation(
                    value=86400,
                    attributes={
                        AttributeKey.AIRFLOW_ENVIRONMENT_NAME: "experiment-dan",
                    },
                ),
                Observation(
                    value=0,
                    attributes={
                        AttributeKey.AIRFLOW_ENVIRONMENT_NAME: "orchestration-v2",
                    },
                ),
            ],
        )
