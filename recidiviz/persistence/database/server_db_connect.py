# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Utils for connecting from Google App Engine to our Postgres instances in
Cloud SQL.
"""
import logging
from typing import Optional

from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.persistence.database.jails_base_schema import JailsBase
from recidiviz.persistence.database.db_connect import \
    connect_session_to_database_instance
from recidiviz.persistence.database.state_base_schema import StateBase
from recidiviz.utils import secrets, environment


def connect_session_to_server_postgres_instances():
    if not environment.in_gae():
        logging.info(
            "Environment is not GAE - not connecting to postgres instances.")
        return

    _connect_session_to_jails_server_postgres_instance()
    if environment.in_gae_staging():
        # TODO(1831): Turn on for production once cloudsql instance has been
        #  configured
        _connect_session_to_state_server_postgres_instance()


def _connect_session_to_state_server_postgres_instance():
    db_user = secrets.get_secret('state_db_user')
    db_password = secrets.get_secret('state_db_password')
    db_name = secrets.get_secret('state_db_name')
    cloudsql_instance_id = secrets.get_secret('state_cloudsql_instance_id')

    _connect_session_to_server_postgres_instance(
        db_user=db_user,
        db_password=db_password,
        db_name=db_name,
        cloudsql_instance_id=cloudsql_instance_id,
        schema_base=StateBase
    )


def _connect_session_to_jails_server_postgres_instance():
    db_user = secrets.get_secret('sqlalchemy_db_user')
    db_password = secrets.get_secret('sqlalchemy_db_password')
    db_name = secrets.get_secret('sqlalchemy_db_name')
    cloudsql_instance_id = secrets.get_secret('cloudsql_instance_id')

    _connect_session_to_server_postgres_instance(
        db_user=db_user,
        db_password=db_password,
        db_name=db_name,
        cloudsql_instance_id=cloudsql_instance_id,
        schema_base=JailsBase
    )


def _connect_session_to_server_postgres_instance(
        *,
        db_user: Optional[str],
        db_password: Optional[str],
        db_name: Optional[str],
        cloudsql_instance_id: Optional[str],
        schema_base: DeclarativeMeta):
    sqlalchemy_url = ('postgresql://{db_user}:{db_password}@/{db_name}'
                      '?host=/cloudsql/{cloudsql_instance_id}').format(
                          db_user=db_user,
                          db_password=db_password,
                          db_name=db_name,
                          cloudsql_instance_id=cloudsql_instance_id)

    connect_session_to_database_instance(db_url=sqlalchemy_url,
                                         schema_base=schema_base)
