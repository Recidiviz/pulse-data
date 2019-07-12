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
"""A class to manage all SQLAlchemy Engines for our database instances."""

import logging
from typing import Dict, Optional

import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy.ext.declarative import DeclarativeMeta

from recidiviz.persistence.database.base_schema import JailsBase, \
    StateBase
from recidiviz.utils import secrets, environment


class SQLAlchemyEngineManager:
    """A class to manage all SQLAlchemy Engines for our database instances."""

    _engine_for_schema: Dict[DeclarativeMeta, Engine] = {}

    @classmethod
    def init_engine_for_db_instance(
            cls, db_url: str, schema_base: DeclarativeMeta) -> None:
        engine = sqlalchemy.create_engine(db_url)
        schema_base.metadata.create_all(engine)
        cls._engine_for_schema[schema_base] = engine

    @classmethod
    def init_engines_for_server_postgres_instances(cls) -> None:
        if not environment.in_gae():
            logging.info(
                "Environment is not GAE, not connecting to postgres instances.")
            return

        # Initialize Jails database instance
        cls.init_engine_for_db_instance(
            db_url=cls._get_jails_server_postgres_instance_url(),
            schema_base=JailsBase)
        if environment.in_gae_staging():
            # TODO(1831): Turn on for production once cloudsql instance has been
            #  configured
            # Initialize State database instance
            cls.init_engine_for_db_instance(
                db_url=cls._get_state_server_postgres_instance_url(),
                schema_base=StateBase)

    @classmethod
    def get_engine_for_schema_base(
            cls, schema_base: DeclarativeMeta) -> Optional[Engine]:
        return cls._engine_for_schema.get(schema_base, None)

    @classmethod
    def _get_state_server_postgres_instance_url(cls) -> str:
        return cls._get_server_postgres_instance_url(
            db_user_key='state_db_user',
            db_password_key='state_db_password',
            db_name_key='state_db_name',
            cloudsql_instance_id_key='state_cloudsql_instance_id')

    @classmethod
    def _get_jails_server_postgres_instance_url(cls) -> str:
        return cls._get_server_postgres_instance_url(
            db_user_key='sqlalchemy_db_user',
            db_password_key='sqlalchemy_db_password',
            db_name_key='sqlalchemy_db_name',
            cloudsql_instance_id_key='cloudsql_instance_id')

    @classmethod
    def _get_server_postgres_instance_url(cls,
                                          *,
                                          db_user_key: str,
                                          db_password_key: str,
                                          db_name_key: str,
                                          cloudsql_instance_id_key: str) -> str:
        db_user = secrets.get_secret(db_user_key)
        db_password = secrets.get_secret(db_password_key)
        db_name = secrets.get_secret(db_name_key)
        cloudsql_instance_id = secrets.get_secret(cloudsql_instance_id_key)

        sqlalchemy_url = ('postgresql://{db_user}:{db_password}@/{db_name}'
                          '?host=/cloudsql/{cloudsql_instance_id}').format(
                              db_user=db_user,
                              db_password=db_password,
                              db_name=db_name,
                              cloudsql_instance_id=cloudsql_instance_id)
        return sqlalchemy_url
