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
"""Custom sensor operator that returns success once the provided BQ SQL query returns a
non-zero row count.
"""

import abc
from typing import Any, Dict

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.log.logging_mixin import LoggingMixin

# Default number of seconds between times when the sensor checks for success.
DEFAULT_SENSOR_POKE_INTERVAL_SECONDS = 60

# Default number of seconds the sensor will wait for success before timing out.
DEFAULT_SENSOR_TIMEOUT_SECONDS = 60 * 120

RESULT_ROW_COUNT_COLUMN_NAME = "row_count"


class BQResultSensorQueryGenerator:
    @abc.abstractmethod
    def get_query(self, operator: "BQResultSensor", context: Dict) -> str:
        """Returns a query that will return a non-zero row count when the BQResultSensor
        should return success.
        """


class BQResultSensor(BaseSensorOperator, LoggingMixin):
    """Sensor operator that returns success once the provided BQ SQL query returns a
    non-zero row count.
    """

    def __init__(
        self,
        *,
        task_id: str,
        query_generator: BQResultSensorQueryGenerator,
        poke_interval: float = DEFAULT_SENSOR_POKE_INTERVAL_SECONDS,
        timeout: float = DEFAULT_SENSOR_TIMEOUT_SECONDS,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            task_id=task_id, poke_interval=poke_interval, timeout=timeout, **kwargs
        )
        self.query_generator = query_generator

    def poke(self, context: Dict) -> bool:
        check_query = self.query_generator.get_query(self, context).rstrip(";")

        self.log.info("Checking for rows in query: %s", check_query)
        bq_hook = BigQueryHook(use_legacy_sql=False)

        df = bq_hook.get_pandas_df(
            f"SELECT COUNT(*) AS {RESULT_ROW_COUNT_COLUMN_NAME} FROM ({check_query});"
        )
        row_counts = df[RESULT_ROW_COUNT_COLUMN_NAME].values
        if len(row_counts) != 1:
            raise ValueError(
                f"Expected exactly one result row, found [{len(row_counts)}]."
            )

        row_count = row_counts[0]
        self.log.info("Found [%s] rows in condition query.", row_count)
        return row_count > 0
