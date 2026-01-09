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
"""A base class to manage SQLAlchemy Engines for our database instances. The syncronous
and asyncronous engine manager classes will build on top of this base class."""
import os
from typing import Dict, List, Optional

from recidiviz.persistence.database.constants import (
    SQLALCHEMY_DB_HOST,
    SQLALCHEMY_DB_NAME,
    SQLALCHEMY_DB_PASSWORD,
    SQLALCHEMY_DB_PORT,
    SQLALCHEMY_DB_USER,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils import secrets

CLOUDSQL_PROXY_HOST = "127.0.0.1"
CLOUDSQL_PROXY_MIGRATION_PORT = 5440


class BaseEngineManager:
    """Base class for managing SQLAlchemy Engines."""

    @classmethod
    def _secret_manager_prefix_for_type(cls, schema_type: SchemaType) -> str:
        # TODO(#8282): Clean up the _v2 suffix eventually.
        match schema_type:
            case SchemaType.OPERATIONS:
                return "operations_v2"
            case SchemaType.JUSTICE_COUNTS:
                return "justice_counts"
            case SchemaType.CASE_TRIAGE:
                return "case_triage"
            case SchemaType.PATHWAYS:
                return "pathways"
            case SchemaType.PUBLIC_PATHWAYS:
                return "public_pathways"
            case SchemaType.WORKFLOWS:
                return "workflows"
            case SchemaType.INSIGHTS:
                return "insights"

        raise ValueError(f"Unexpected schema type [{schema_type}].")

    @classmethod
    def _secret_manager_prefix(cls, database_key: SQLAlchemyDatabaseKey) -> str:
        return cls._secret_manager_prefix_for_type(database_key.schema_type)

    @classmethod
    def _get_db_host(
        cls,
        *,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str],
    ) -> str:
        secret_key = f"{secret_prefix_override or cls._secret_manager_prefix(database_key)}_db_host"
        host = secrets.get_secret(secret_key)
        if host is None:
            raise ValueError(f"Unable to retrieve database host for key [{secret_key}]")
        return host

    @classmethod
    def _get_db_password(
        cls,
        *,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str],
    ) -> str:
        secret_key = f"{secret_prefix_override or cls._secret_manager_prefix(database_key)}_db_password"
        password = secrets.get_secret(secret_key)
        if password is None:
            raise ValueError(
                f"Unable to retrieve database password for key [{secret_key}]"
            )
        return password

    @classmethod
    def _get_db_readonly_password(cls, *, database_key: SQLAlchemyDatabaseKey) -> str:
        secret_key = f"{cls._secret_manager_prefix(database_key)}_db_readonly_password"
        password = secrets.get_secret(secret_key)
        if password is None:
            raise ValueError(
                f"Unable to retrieve database readonly password for key [{secret_key}]"
            )
        return password

    @classmethod
    def _get_db_readonly_user(cls, *, database_key: SQLAlchemyDatabaseKey) -> str:
        secret_key = f"{cls._secret_manager_prefix(database_key)}_db_readonly_user"
        user = secrets.get_secret(secret_key)
        if user is None:
            raise ValueError(
                f"Unable to retrieve database readonly user for key [{secret_key}]"
            )
        return user

    @classmethod
    def _get_db_user(
        cls,
        *,
        database_key: SQLAlchemyDatabaseKey,
        secret_prefix_override: Optional[str],
    ) -> str:
        secret_key = f"{secret_prefix_override or cls._secret_manager_prefix(database_key)}_db_user"
        user = secrets.get_secret(secret_key)
        if user is None:
            raise ValueError(f"Unable to retrieve database user for key [{secret_key}]")
        return user

    @classmethod
    def _get_cloudsql_instance_id_key(
        cls,
        schema_type: SchemaType,
        secret_prefix_override: Optional[str],
    ) -> str:
        return f"{secret_prefix_override or cls._secret_manager_prefix_for_type(schema_type)}_cloudsql_instance_id"

    @classmethod
    def get_full_cloudsql_instance_id(
        cls, schema_type: SchemaType, secret_prefix_override: Optional[str] = None
    ) -> str:
        """Rerturns the full instance id stored in secrets with the form
        project_id:region:instance_id.

        For example:
            recidiviz-staging:us-east1:dev-pathways-data
        """
        instance_id_key = cls._get_cloudsql_instance_id_key(
            schema_type=schema_type, secret_prefix_override=secret_prefix_override
        )
        instance_id_full = secrets.get_secret(instance_id_key)

        if instance_id_full is None:
            raise ValueError(
                f"Unable to retrieve instance id for schema type [{schema_type}]"
            )
        return instance_id_full

    @classmethod
    def get_cloudsql_instance_region(cls, schema_type: SchemaType) -> str:
        """The full instance id stored in secrets has the form
        project_id:region:instance_id.

        This returns just the region, for example, 'us-east1' or
        'us-central1'.
        """
        instance_id_full = cls.get_full_cloudsql_instance_id(schema_type)

        # Expected format "project_id:region:instance_id"
        _, region, _ = instance_id_full.split(":")

        return region

    @classmethod
    def get_stripped_cloudsql_instance_id(cls, schema_type: SchemaType) -> str:
        """The full instance id stored in secrets has the form
        project_id:region:instance_id.

        This returns just the final instance_id, for example, 'dev-data' or
        'prod-state-data'.

        Should be used when using the sqladmin_client().
        """
        instance_id_full = cls.get_full_cloudsql_instance_id(schema_type)

        # Expected format "project_id:region:instance_id"
        _, _, instance_id = instance_id_full.split(":")

        return instance_id

    @classmethod
    def get_all_stripped_cloudsql_instance_ids(cls) -> List[str]:
        """Returns stripped instance ids for all sql instances in this project.

        See get_stripped_cloudsql_instance_id() for more info.
        """
        ids_for_all_schemas = [
            cls.get_stripped_cloudsql_instance_id(schema_type)
            for schema_type in SchemaType
            if schema_type.has_cloud_sql_instance
        ]
        return [
            instance_id
            for instance_id in ids_for_all_schemas
            if instance_id is not None
        ]

    @classmethod
    def update_sqlalchemy_env_vars(
        cls,
        database_key: SQLAlchemyDatabaseKey,
        readonly_user: bool = False,
        using_proxy: bool = False,
        secret_prefix_override: Optional[str] = None,
    ) -> Dict[str, Optional[str]]:
        """Updates the appropriate env vars for SQLAlchemy to talk to the postgres instance associated with the schema.

        It returns the old set of env variables that were overridden.
        """

        sqlalchemy_vars = [
            SQLALCHEMY_DB_NAME,
            SQLALCHEMY_DB_HOST,
            SQLALCHEMY_DB_USER,
            SQLALCHEMY_DB_PASSWORD,
        ]
        original_values = {
            env_var: os.environ.get(env_var) for env_var in sqlalchemy_vars
        }

        os.environ[SQLALCHEMY_DB_NAME] = database_key.db_name

        if using_proxy:
            os.environ[SQLALCHEMY_DB_HOST] = CLOUDSQL_PROXY_HOST
            os.environ[SQLALCHEMY_DB_PORT] = str(CLOUDSQL_PROXY_MIGRATION_PORT)
        else:
            os.environ[SQLALCHEMY_DB_HOST] = cls._get_db_host(
                database_key=database_key, secret_prefix_override=secret_prefix_override
            )

        if readonly_user:
            os.environ[SQLALCHEMY_DB_USER] = cls._get_db_readonly_user(
                database_key=database_key
            )
            os.environ[SQLALCHEMY_DB_PASSWORD] = cls._get_db_readonly_password(
                database_key=database_key
            )
        else:
            os.environ[SQLALCHEMY_DB_USER] = cls._get_db_user(
                database_key=database_key, secret_prefix_override=secret_prefix_override
            )
            os.environ[SQLALCHEMY_DB_PASSWORD] = cls._get_db_password(
                database_key=database_key, secret_prefix_override=secret_prefix_override
            )

        return original_values

    @classmethod
    def _get_db_port(cls) -> int:
        return int(os.environ.get(SQLALCHEMY_DB_PORT, "5432"))
