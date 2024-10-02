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
"""Unit tests for ReleaseRawDataResourceLockSqlQueryGenerator"""
import datetime
from typing import List
from unittest.mock import create_autospec

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context
from freezegun import freeze_time

from recidiviz.airflow.dags.operators.cloud_sql_query_operator import (
    CloudSqlQueryOperator,
)
from recidiviz.airflow.dags.raw_data.acquire_resource_lock_sql_query_generator import (
    AcquireRawDataResourceLockSqlQueryGenerator,
)
from recidiviz.airflow.dags.raw_data.metadata import RESOURCE_LOCKS_NEEDED
from recidiviz.airflow.dags.raw_data.release_resource_lock_sql_query_generator import (
    ReleaseRawDataResourceLockSqlQueryGenerator,
)
from recidiviz.airflow.tests.test_utils import CloudSqlQueryGeneratorUnitTest
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import RawDataResourceLock
from recidiviz.persistence.database.schema.operations.schema import OperationsBase
from recidiviz.persistence.errors import (
    DirectIngestRawDataResourceLockAlreadyReleasedError,
)


class TestReleaseRawDataResourceLockSqlQueryGenerator(CloudSqlQueryGeneratorUnitTest):
    """Unit tests for ReleaseRawDataResourceLockSqlQueryGenerator"""

    metas = [OperationsBase]

    def setUp(self) -> None:
        super().setUp()
        self.acquire_generator = AcquireRawDataResourceLockSqlQueryGenerator(
            region_code="US_XX",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            resources=RESOURCE_LOCKS_NEEDED,
            lock_description="t3st!ng",
            lock_ttl_seconds=10,
        )
        self.release_generator = ReleaseRawDataResourceLockSqlQueryGenerator(
            region_code="US_XX",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            acquire_resource_lock_task_id="test_id",
        )
        self.mock_pg_hook = PostgresHook(postgres_conn_id=self.conn_id)

    def _acquire_locks(self) -> List[str]:
        return self.acquire_generator.execute_postgres_query(
            create_autospec(CloudSqlQueryOperator),
            self.mock_pg_hook,
            create_autospec(Context),
        )

    def _get_current_locks(self) -> List[RawDataResourceLock]:
        results: List = self.mock_pg_hook.get_records(
            "SELECT lock_id, lock_resource, released FROM direct_ingest_raw_data_resource_lock"
        )
        return [RawDataResourceLock.from_table_row(row) for row in results]

    @freeze_time(datetime.datetime(2023, 1, 26, 0, 0, 0, 0))
    def test_release_lock(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_context = create_autospec(Context)
        written_locks = self._acquire_locks()
        mock_operator.xcom_pull.return_value = written_locks

        locks_before_release = self._get_current_locks()

        assert all(not lock.released for lock in locks_before_release)

        self.release_generator.execute_postgres_query(
            mock_operator, self.mock_pg_hook, mock_context
        )

        locks_after_release = self._get_current_locks()

        self.assertSetEqual(
            {lock.lock_id for lock in locks_before_release},
            {lock.lock_id for lock in locks_after_release},
        )

        assert all(lock.released for lock in locks_after_release)

        with self.assertRaisesRegex(
            DirectIngestRawDataResourceLockAlreadyReleasedError,
            r"Lock has already been released: .*",
        ):
            self.release_generator.execute_postgres_query(
                mock_operator, self.mock_pg_hook, mock_context
            )

    @freeze_time(datetime.datetime(2023, 1, 26, 0, 0, 0, 0))
    def test_release_invalid_lock(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_operator.xcom_pull.return_value = [
            RawDataResourceLock(
                lock_id=1,
                lock_resource=DirectIngestRawDataResourceLockResource.BIG_QUERY_RAW_DATA_DATASET,
                released=False,
            ).serialize()
        ]

        with self.assertRaisesRegex(
            LookupError,
            r"Could not find all locks with the ids provided: .*",
        ):
            self.release_generator.execute_postgres_query(
                mock_operator, self.mock_pg_hook, create_autospec(Context)
            )

    def test_release_lock_already_released(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        with freeze_time(datetime.datetime(2023, 1, 26, 0, 0, 0, 0)):
            written_locks = self._acquire_locks()

        self.mock_pg_hook.run(
            self.acquire_generator.set_expired_lock_as_released_sql_query()
        )
        mock_operator.xcom_pull.return_value = written_locks

        with self.assertRaisesRegex(
            DirectIngestRawDataResourceLockAlreadyReleasedError,
            r"Lock has already been released: .*",
        ):
            self.release_generator.execute_postgres_query(
                mock_operator, self.mock_pg_hook, create_autospec(Context)
            )
