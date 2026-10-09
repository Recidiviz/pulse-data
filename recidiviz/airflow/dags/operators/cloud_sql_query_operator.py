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
from typing import Any, Generic, TypeVar

from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.context import Context

from recidiviz.airflow.dags.utils.cloud_sql_session import (
    cloud_sql_postgres_hook_for_connection,
)

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
    """Custom operator that uses |cloud_sql_conn_id| to configure a connection to a CloudSQL
    Postgres database and returns the output from a query against that database run by
    |query_generator|.
    """

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
        with cloud_sql_postgres_hook_for_connection(
            self.cloud_sql_conn_id
        ) as postgres_hook:
            return self.query_generator.execute_postgres_query(
                self, postgres_hook, context
            )
