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
"""Custom operator that returns output from a Cloud SQL Postgres query."""
import abc
from contextlib import contextmanager
from typing import Any, Generic, Iterator, TypeVar

from airflow.models.baseoperator import BaseOperator
from airflow.models.connection import Connection
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLDatabaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.utils.types import assert_type

Output = TypeVar("Output")


class CloudSqlQueryGenerator(Generic[Output]):
    @abc.abstractmethod
    def execute_postgres_query(
        self,
        operator: "CloudSqlQueryOperator",
        postgres_hook: PostgresHook,
        context: Context,
    ) -> Output:
        """Executes a customized postgres query and returns a specified output."""


class CloudSqlQueryOperator(BaseOperator, Generic[Output]):
    """Custom operator that connects to a CloudSQL Postgres database and returns the
    output from a query against that database."""

    def __init__(
        self,
        task_id: str,
        cloud_sql_conn_id: str,
        query_generator: CloudSqlQueryGenerator[Output],
        **kwargs: Any
    ) -> None:
        super().__init__(task_id=task_id, **kwargs)
        self.cloud_sql_conn_id = cloud_sql_conn_id
        self.query_generator = query_generator

    def execute(self, context: Context) -> Output:
        cloud_sql_hook = CloudSQLDatabaseHook(
            gcp_cloudsql_conn_id=self.cloud_sql_conn_id
        )
        cloud_sql_hook.validate_ssl_certs()
        cloud_sql_hook.validate_socket_path_length()

        with self.cloud_sql_proxy(cloud_sql_hook) as connection:
            postgres_hook = assert_type(
                cloud_sql_hook.get_database_hook(connection),
                PostgresHook,
            )
            results = self.query_generator.execute_postgres_query(
                self, postgres_hook, context
            )
        return results

    @staticmethod
    @contextmanager
    def cloud_sql_proxy(cloud_sql_hook: CloudSQLDatabaseHook) -> Iterator[Connection]:
        """In order to connect to CloudSQL, the recommended way is through the
        Cloud SQL Auth Proxy, which needs to be started and stopped explicitly between
        transactions. https://cloud.google.com/sql/docs/postgres/sql-proxy"""
        cloud_sql_proxy_runner = None
        connection = cloud_sql_hook.create_connection()
        if cloud_sql_hook.use_proxy:
            cloud_sql_proxy_runner = cloud_sql_hook.get_sqlproxy_runner()
            cloud_sql_hook.free_reserved_port()
            cloud_sql_proxy_runner.start_proxy()

        try:
            yield connection
        finally:
            if cloud_sql_proxy_runner:
                cloud_sql_proxy_runner.stop_proxy()
