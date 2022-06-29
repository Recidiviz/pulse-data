# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Tests for BQResultsSensor."""
import unittest
from datetime import datetime
from typing import Dict, Optional
from unittest.mock import call, create_autospec, patch

import pandas as pd
from airflow import DAG
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from recidiviz.airflow.dags.operators.bq_result_sensor import (
    RESULT_ROW_COUNT_COLUMN_NAME,
    BQResultSensor,
    BQResultSensorQueryGenerator,
)
from recidiviz.airflow.tests.test_utils import execute_task


class FakeBQResultSensorQueryGenerator(BQResultSensorQueryGenerator):
    def __init__(self) -> None:
        self.operator: Optional[BQResultSensor] = None
        self.query_count = 0

    def get_query(self, operator: BQResultSensor, context: Dict) -> str:
        self.query_count += 1
        if self.operator is not None and operator != self.operator:
            raise ValueError(
                "Passed multiple different operators to the query generator."
            )
        self.operator = operator
        return "SELECT * FROM `recidiviz-456.foo.bar`;"


class TestBQResultSensor(unittest.TestCase):
    """Tests for BQResultsSensor."""

    TEST_POKE_INTERVAL = 0.1
    EXPECTED_ROW_COUNT_QUERY_STR = (
        "SELECT COUNT(*) AS row_count FROM (SELECT * FROM `recidiviz-456.foo.bar`);"
    )

    def setUp(self) -> None:
        self.mock_bq_hook = create_autospec(BigQueryHook)
        self.mock_bq_patcher = patch(
            "recidiviz.airflow.dags.operators.bq_result_sensor.BigQueryHook",
        )
        self.mock_bq_patcher.start().return_value = self.mock_bq_hook

        self.dag = DAG(dag_id="test_dag", start_date=datetime.now())

        self.query_generator = FakeBQResultSensorQueryGenerator()
        self.sensor_task = BQResultSensor(
            task_id="test_sensor",
            query_generator=self.query_generator,
            poke_interval=self.TEST_POKE_INTERVAL,
            dag=self.dag,
        )

    def tearDown(self) -> None:
        self.mock_bq_patcher.stop()

    def test_execute_basic(self) -> None:
        # Arrange
        self.mock_bq_hook.get_pandas_df.side_effect = [
            pd.DataFrame([[0]], columns=[RESULT_ROW_COUNT_COLUMN_NAME]),
            pd.DataFrame([[0]], columns=[RESULT_ROW_COUNT_COLUMN_NAME]),
            pd.DataFrame([[1]], columns=[RESULT_ROW_COUNT_COLUMN_NAME]),
        ]

        # Act
        start = datetime.now()
        execute_task(self.dag, self.sensor_task)
        end = datetime.now()

        runtime = (end - start).total_seconds()

        # Assert
        # Should be only 3x poke interval, check it's less than 5x for some buffer.
        self.assertTrue(runtime < self.TEST_POKE_INTERVAL * 5)

        self.assertEqual(3, self.query_generator.query_count)
        self.assertEqual(3, self.mock_bq_hook.get_pandas_df.call_count)
        self.mock_bq_hook.get_pandas_df.assert_has_calls(
            [call(self.EXPECTED_ROW_COUNT_QUERY_STR)]
        )

    def test_execute_immediate_success(self) -> None:
        # Arrange
        self.mock_bq_hook.get_pandas_df.side_effect = [
            pd.DataFrame([[1]], columns=[RESULT_ROW_COUNT_COLUMN_NAME]),
        ]

        # Act
        start = datetime.now()
        execute_task(self.dag, self.sensor_task)
        end = datetime.now()

        runtime = (end - start).total_seconds()

        # Assert
        # Should run almost instantly
        self.assertTrue(runtime < self.TEST_POKE_INTERVAL * 1)

        self.assertEqual(1, self.query_generator.query_count)
        self.assertEqual(1, self.mock_bq_hook.get_pandas_df.call_count)
        self.mock_bq_hook.get_pandas_df.assert_has_calls(
            [call(self.EXPECTED_ROW_COUNT_QUERY_STR)]
        )

    def test_execute_multiple_row_count_rows(self) -> None:
        # Arrange
        self.mock_bq_hook.get_pandas_df.side_effect = [
            pd.DataFrame([[0], [1]], columns=[RESULT_ROW_COUNT_COLUMN_NAME]),
        ]

        # Act
        with self.assertRaisesRegex(
            ValueError, r"Expected exactly one result row, found \[2\]."
        ):
            execute_task(self.dag, self.sensor_task)
