# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""A CloudSQLQueryGenerator for acquiring raw data resource locks"""
from typing import Any, Dict, List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from psycopg2.errors import UniqueViolation  # pylint: disable=no-name-in-module

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.utils.cloud_sql import (
    postgres_formatted_current_datetime_utc_str,
)
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataLockActor,
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.errors import DirectIngestRawDataResourceLockHeldError
from recidiviz.utils.string import StrictStringFormatter

LOCK_UNIQUE_CONSTRAINT = "at_most_one_active_lock_per_resource_region_and_instance"

SET_EXPIRED_LOCKS_AS_RELEASED = """
UPDATE direct_ingest_raw_data_resource_lock
SET released = TRUE
WHERE region_code = '{region_code}' 
AND raw_data_source_instance = '{raw_data_instance}' 
AND lock_resource in {lock_resources} 
AND released IS FALSE
AND lock_ttl_seconds is NOT NULL
AND EXTRACT(epoch FROM '{current_datetime}'::timestamp with time zone - lock_acquisition_time) > lock_ttl_seconds;"""

ADD_LOCKS_SQL_QUERY = """
INSERT INTO direct_ingest_raw_data_resource_lock (lock_actor, lock_resource, region_code, raw_data_source_instance, released, lock_acquisition_time, lock_ttl_seconds, lock_description) 
VALUES {values}
RETURNING lock_id, lock_resource;"""


class AcquireRawDataResourceLockSqlQueryGenerator(
    CloudSqlQueryGenerator[List[Dict[str, Any]]]
):
    """Custom query generator for acquiring raw data resource locks"""

    def __init__(
        self,
        region_code: str,
        raw_data_instance: DirectIngestInstance,
        resources: List[DirectIngestRawDataResourceLockResource],
        lock_description: str,
        lock_ttl_seconds: int = 60 * 60,  # 1 hour
    ) -> None:
        super().__init__()
        self._region_code = region_code
        self._raw_data_instance = raw_data_instance
        self._resources = resources
        self._lock_description = lock_description
        self._lock_ttl_seconds = lock_ttl_seconds

    # pylint: disable=unused-argument
    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[Dict[str, Any]]:
        # postgres_hook.run can take multiple statements that, if autocommit is set False
        # which is the default behavior, will all be executed w/in the same cursor using
        # psycopg2 w/in the same commit. thus if one of them fails, they'll all get
        # rolled back
        statements = [
            self.set_expired_lock_as_released_sql_query(),
            self.add_lock_rows_sql_query(),
        ]

        try:
            results = postgres_hook.get_records(statements)
        except UniqueViolation as e:
            if e.diag.constraint_name == LOCK_UNIQUE_CONSTRAINT:
                raise DirectIngestRawDataResourceLockHeldError from e
            raise e

        # results is list of both queries, should have length of 2
        _, add_result = results

        if add_result is None:
            raise ValueError("Expected a return result when creating locks")

        return [
            {
                "lock_id": lock[0],
                "lock_resource": DirectIngestRawDataResourceLockResource(lock[1]).value,
            }
            for lock in add_result
        ]

    @property
    def resources_as_str(self) -> str:
        return ", ".join([f"'{resource.value}'" for resource in self._resources])

    def set_expired_lock_as_released_sql_query(self) -> str:
        return StrictStringFormatter().format(
            SET_EXPIRED_LOCKS_AS_RELEASED,
            region_code=self._region_code,
            raw_data_instance=self._raw_data_instance.value,
            lock_resources=f"({self.resources_as_str})",
            current_datetime=postgres_formatted_current_datetime_utc_str(),
        )

    def add_lock_rows_sql_query(self) -> str:
        values = ",".join(
            [
                f"\n('{DirectIngestRawDataLockActor.PROCESS.value}', '{resource.value}', '{self._region_code}', '{self._raw_data_instance.value}', '0', '{postgres_formatted_current_datetime_utc_str()}', '{self._lock_ttl_seconds}', '{self._lock_description}')"
                for resource in self._resources
            ]
        )

        return StrictStringFormatter().format(ADD_LOCKS_SQL_QUERY, values=values)
