# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Unit tests for airflow_cloud_sql_session_for_connection"""

import contextlib
from concurrent.futures import ThreadPoolExecutor
from typing import Iterator
from unittest.mock import MagicMock, patch

from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

from recidiviz.airflow.dags.utils.cloud_sql_session import (
    airflow_cloud_sql_session_for_connection,
)
from recidiviz.airflow.tests.test_utils import CloudSqlQueryGeneratorUnitTest

_SCRATCH_TABLE = "cloud_sql_session_test_scratch"


class TestAirflowCloudSqlSessionForConnection(CloudSqlQueryGeneratorUnitTest):
    """Unit tests for airflow_cloud_sql_session_for_connection."""

    metas = []

    def setUp(self) -> None:
        super().setUp()
        self.engine = PostgresHook(
            postgres_conn_id=self.conn_id
        ).get_sqlalchemy_engine()
        self.addCleanup(self.engine.dispose)
        with self.engine.begin() as conn:
            conn.execute(
                text(f"CREATE TABLE {_SCRATCH_TABLE} (value INTEGER PRIMARY KEY)")
            )
        self.addCleanup(self._drop_scratch_table)

        # Replace the GCP-backed proxy and hook so get_database_hook hands back a
        # PostgresHook pointed at the local test DB. A fresh CloudSQLDatabaseHook mock is
        # returned per construction so concurrent calls don't share one hook.
        self.cloud_sql_hook_patcher = patch(
            "recidiviz.airflow.dags.utils.cloud_sql_session.CloudSQLDatabaseHook",
            side_effect=self._build_cloud_sql_hook_mock,
        )
        self.mock_cloud_sql_hook_cls = self.cloud_sql_hook_patcher.start()
        self.addCleanup(self.cloud_sql_hook_patcher.stop)

        self.proxy_patcher = patch(
            "recidiviz.airflow.dags.utils.cloud_sql_session.cloud_sql_proxy",
            side_effect=self._fake_cloud_sql_proxy,
        )
        self.proxy_patcher.start()
        self.addCleanup(self.proxy_patcher.stop)

    def _drop_scratch_table(self) -> None:
        with self.engine.begin() as conn:
            conn.execute(text(f"DROP TABLE {_SCRATCH_TABLE}"))

    def _build_cloud_sql_hook_mock(self, **_kwargs: str) -> MagicMock:
        hook = MagicMock()
        hook.get_database_hook.return_value = PostgresHook(
            postgres_conn_id=self.conn_id
        )
        return hook

    @contextlib.contextmanager
    def _fake_cloud_sql_proxy(self, _hook: MagicMock) -> Iterator[MagicMock]:
        yield MagicMock()

    def _scratch_values(self) -> list[int]:
        with self.engine.begin() as conn:
            return [
                row[0]
                for row in conn.execute(
                    text(f"SELECT value FROM {_SCRATCH_TABLE} ORDER BY value")
                )
            ]

    def test_commits_on_clean_exit(self) -> None:
        with airflow_cloud_sql_session_for_connection(self.conn_id) as session:
            session.execute(text(f"INSERT INTO {_SCRATCH_TABLE} (value) VALUES (1)"))

        self.assertEqual([1], self._scratch_values())

    def test_rolls_back_and_reraises_on_error(self) -> None:
        with self.assertRaisesRegex(ValueError, r"^boom$"):
            with airflow_cloud_sql_session_for_connection(self.conn_id) as session:
                session.execute(
                    text(f"INSERT INTO {_SCRATCH_TABLE} (value) VALUES (1)")
                )
                raise ValueError("boom")

        # The insert was rolled back, so nothing persisted.
        self.assertEqual([], self._scratch_values())

    def test_concurrent_calls_for_one_connection_hold_no_shared_state(
        self,
    ) -> None:
        """Fanning the helper out across threads for one connection id writes every row
        with no cross-talk. This guards that the helper holds no session, hook, or engine
        state across calls — it builds a fresh CloudSQLDatabaseHook (and thus a fresh
        engine and Session) every call. The per-instance proxy port/socket isolation that
        makes concurrent real connections independent lives in CloudSQLDatabaseHook, which
        is mocked here and is Airflow's own, so it is out of scope for this test."""
        values = [1, 2, 3, 4, 5, 6, 7, 8]

        def _write(value: int) -> None:
            with airflow_cloud_sql_session_for_connection(self.conn_id) as session:
                session.execute(
                    text(f"INSERT INTO {_SCRATCH_TABLE} (value) VALUES ({value})")
                )

        with ThreadPoolExecutor(max_workers=len(values)) as executor:
            # list() surfaces any exception raised inside a worker.
            list(executor.map(_write, values))

        self.assertEqual([1, 2, 3, 4, 5, 6, 7, 8], self._scratch_values())
        # One CloudSQLDatabaseHook constructed per call: a regression that memoized the
        # hook (or engine) across calls would drop this below the call count.
        self.assertEqual(8, self.mock_cloud_sql_hook_cls.call_count)
