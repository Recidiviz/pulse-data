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
"""Helpers for driving a Cloud SQL Postgres database from an in-process Airflow task."""

from contextlib import contextmanager
from typing import Iterator

from airflow.models.connection import Connection
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLDatabaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.orm import Session

from recidiviz.utils.types import assert_type


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


@contextmanager
def cloud_sql_postgres_hook_for_connection(
    cloud_sql_conn_id: str,
) -> Iterator[PostgresHook]:
    """Yields a PostgresHook connected through the Cloud SQL Auth Proxy for the duration
    of the context, tearing the proxy down on exit.

    Each call builds its own CloudSQLDatabaseHook, which generates its own proxy socket
    path (and, on the TCP path, its own reserved port), so concurrent calls for one
    connection id on one worker drive independent proxies and connections.
    """
    cloud_sql_hook = CloudSQLDatabaseHook(gcp_cloudsql_conn_id=cloud_sql_conn_id)
    cloud_sql_hook.validate_ssl_certs()
    cloud_sql_hook.validate_socket_path_length()

    with cloud_sql_proxy(cloud_sql_hook) as connection:
        yield assert_type(cloud_sql_hook.get_database_hook(connection), PostgresHook)


@contextmanager
def airflow_cloud_sql_session_for_connection(
    cloud_sql_conn_id: str,
) -> Iterator[Session]:
    """Yields a Session connected through the Cloud SQL Auth Proxy for use in an
    in-process Airflow task. Commits on clean exit; on any exception the transaction is
    discarded.
    """
    with cloud_sql_postgres_hook_for_connection(cloud_sql_conn_id) as postgres_hook:
        engine = postgres_hook.get_sqlalchemy_engine()
        try:
            with Session(bind=engine) as session, session.begin():
                yield session
        finally:
            engine.dispose()
