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
"""Unit tests for AcquireRawDataResourceLockSqlQueryGenerator"""
import datetime
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
from recidiviz.airflow.tests.test_utils import CloudSqlQueryGeneratorUnitTest
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.types.raw_data_import_types import RawDataResourceLock
from recidiviz.persistence.database.schema.operations.schema import OperationsBase
from recidiviz.persistence.errors import DirectIngestRawDataResourceLockHeldError


class TestAcquireRawDataResourceLockSqlQueryGenerator(CloudSqlQueryGeneratorUnitTest):
    """Unit tests for AcquireRawDataResourceLockSqlQueryGenerator"""

    metas = [OperationsBase]

    def setUp(self) -> None:
        super().setUp()
        self.generator = AcquireRawDataResourceLockSqlQueryGenerator(
            region_code="US_XX",
            raw_data_instance=DirectIngestInstance.PRIMARY,
            resources=RESOURCE_LOCKS_NEEDED,
            lock_description="t3st!ng",
            lock_ttl_seconds=10,
        )
        self.mock_pg_hook = PostgresHook(postgres_conn_id=self.conn_id)

    @freeze_time(datetime.datetime(2023, 1, 26, 0, 0, 0, 0))
    def test_acquires_empty_lock(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_context = create_autospec(Context)

        results = self.generator.execute_postgres_query(
            mock_operator, self.mock_pg_hook, mock_context
        )

        locks = [RawDataResourceLock.deserialize(result) for result in results]

        self.assertSetEqual(
            {lock.lock_resource for lock in locks},
            set(RESOURCE_LOCKS_NEEDED),
        )

        assert all(not lock.released for lock in locks)

    @freeze_time(datetime.datetime(2023, 1, 26, 0, 0, 0, 0))
    def test_acquires_lock_fails(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_context = create_autospec(Context)

        results = self.generator.execute_postgres_query(
            mock_operator, self.mock_pg_hook, mock_context
        )

        locks = [RawDataResourceLock.deserialize(result) for result in results]

        self.assertSetEqual(
            {lock.lock_resource for lock in locks},
            set(RESOURCE_LOCKS_NEEDED),
        )

        assert all(not lock.released for lock in locks)

        with self.assertRaises(DirectIngestRawDataResourceLockHeldError):
            results = self.generator.execute_postgres_query(
                mock_operator, self.mock_pg_hook, mock_context
            )

    def test_acquires_lock_updates_correctly(self) -> None:
        mock_operator = create_autospec(CloudSqlQueryOperator)
        mock_context = create_autospec(Context)

        with freeze_time(datetime.datetime(2023, 1, 26, 0, 0, 0, 0)):
            results = self.generator.execute_postgres_query(
                mock_operator, self.mock_pg_hook, mock_context
            )

        old_locks = [RawDataResourceLock.deserialize(result) for result in results]

        self.assertSetEqual(
            {lock.lock_resource for lock in old_locks},
            set(RESOURCE_LOCKS_NEEDED),
        )

        assert all(not lock.released for lock in old_locks)

        results_new = self.generator.execute_postgres_query(
            mock_operator, self.mock_pg_hook, mock_context
        )
        new_locks = [RawDataResourceLock.deserialize(result) for result in results_new]

        self.assertSetEqual(
            {lock.lock_resource for lock in new_locks},
            set(RESOURCE_LOCKS_NEEDED),
        )

        assert all(not lock.released for lock in new_locks)

        # lock id sets are disjoint
        assert {lock.lock_id for lock in old_locks} & {
            lock.lock_id for lock in new_locks
        } == set()
