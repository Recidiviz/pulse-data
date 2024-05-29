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
"""A CloudSQLQueryGenerator for releasing raw data resource locks"""
from typing import Any, Dict, List

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryGenerator,
    CloudSqlQueryOperator,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.errors import (
    DirectIngestRawDataResourceLockAlreadyReleasedError,
)
from recidiviz.utils.string import StrictStringFormatter

GET_LOCKS_BY_IDS = """
SELECT lock_id, released
FROM direct_ingest_raw_data_resource_lock
WHERE region_code = '{region_code}' 
AND raw_data_source_instance = '{raw_data_instance}' 
AND lock_id in ({lock_ids})"""


SET_LOCKS_AS_RELEASED_BY_IDS = """
UPDATE direct_ingest_raw_data_resource_lock
SET released = TRUE
WHERE region_code = '{region_code}' 
AND raw_data_source_instance = '{raw_data_instance}' 
AND lock_id in ({lock_ids})
RETURNING lock_id, released;"""


class ReleaseRawDataResourceLockSqlQueryGenerator(
    CloudSqlQueryGenerator[List[Dict[str, Any]]]
):
    """Custom query generator for releasing raw data resource locks"""

    def __init__(
        self,
        region_code: str,
        raw_data_instance: DirectIngestInstance,
        acquire_resource_lock_task_id: str,
    ) -> None:
        super().__init__()
        self._region_code = region_code
        self._raw_data_instance = raw_data_instance
        self._acquire_resource_lock_task_id = acquire_resource_lock_task_id

    def execute_postgres_query(
        self,
        operator: CloudSqlQueryOperator,
        postgres_hook: PostgresHook,
        context: Context,
    ) -> List[Dict[str, Any]]:
        # get lock info from xcom
        locks_to_release: List[Dict[str, Any]] = operator.xcom_pull(
            context, key="return_value", task_ids=self._acquire_resource_lock_task_id
        )

        # get existing locks to make sure they are valid lock_ids and they are not
        # released
        existing_locks = postgres_hook.get_records(
            self.get_locks_by_id_sql_query(locks_to_release)
        )

        if len(existing_locks) != len(locks_to_release):
            raise LookupError(
                f"Could not find all locks with the ids provided: {locks_to_release}"
            )

        if not all(not lock[1] for lock in existing_locks):
            raise DirectIngestRawDataResourceLockAlreadyReleasedError(
                f"Lock has already been released: {existing_locks}"
            )

        # update the released col to be True
        updated_locks = postgres_hook.get_records(
            self.set_locks_as_released_by_ids_sql_query(locks_to_release)
        )

        if len(updated_locks) != len(locks_to_release):
            raise ValueError(
                f"Updated the wrong number of locks; tried to update {len(locks_to_release)}, but updated {len(updated_locks)}"
            )

        return [{"lock_id": lock[0], "released": lock[1]} for lock in updated_locks]

    @staticmethod
    def _locks_ids_as_str(lock: List[Dict[str, Any]]) -> str:
        return ", ".join([str(lock["lock_id"]) for lock in lock])

    def get_locks_by_id_sql_query(self, locks: List[Dict[str, Any]]) -> str:

        return StrictStringFormatter().format(
            GET_LOCKS_BY_IDS,
            region_code=self._region_code,
            raw_data_instance=self._raw_data_instance.value,
            lock_ids=self._locks_ids_as_str(locks),
        )

    def set_locks_as_released_by_ids_sql_query(
        self, locks_to_release: List[Dict[str, Any]]
    ) -> str:
        return StrictStringFormatter().format(
            SET_LOCKS_AS_RELEASED_BY_IDS,
            region_code=self._region_code,
            raw_data_instance=self._raw_data_instance.value,
            lock_ids=self._locks_ids_as_str(locks_to_release),
        )
