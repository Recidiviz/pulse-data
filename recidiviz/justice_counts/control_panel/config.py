# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Flask configs for different environments."""

import json
import os
from typing import Callable, Optional

import attr

from recidiviz.justice_counts.auth_utils import on_successful_authorization
from recidiviz.justice_counts.exceptions import JusticeCountsAuthorizationError
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.utils.auth.auth0 import (
    Auth0Config,
    build_auth0_authorization_decorator,
    passthrough_authorization_decorator,
)
from recidiviz.utils.environment import in_ci, in_development
from recidiviz.utils.secrets import get_local_secret

JUSTICE_COUNTS_DATABASE_KEY = SQLAlchemyDatabaseKey.for_schema(
    SchemaType.JUSTICE_COUNTS
)
JUSTICE_COUNTS_DEVELOPMENT_POSTGRES_URL = "JUSTICE_COUNTS_DEVELOPMENT_POSTGRES_URL"


@attr.define
class Config:
    """Config class builds database and authentication objects for justice counts app"""

    DATABASE_KEY: SQLAlchemyDatabaseKey = JUSTICE_COUNTS_DATABASE_KEY
    # Indicates whether CSRF protection is enabled for the whole app. Should be set to False for tests.
    WTF_CSRF_ENABLED: bool = True
    DB_URL: str = attr.field()
    AUTH0_CONFIGURATION: Auth0Config = attr.field()
    AUTH_DECORATOR: Callable = attr.field()

    @DB_URL.default
    def _db_url_factory(self) -> str:
        if in_development():
            return os.environ[JUSTICE_COUNTS_DEVELOPMENT_POSTGRES_URL]

        return SQLAlchemyEngineManager.get_server_postgres_instance_url(
            database_key=self.DATABASE_KEY
        )

    @AUTH_DECORATOR.default
    def _auth_decorator_factory(self) -> Callable:
        if in_ci():
            return passthrough_authorization_decorator()

        return build_auth0_authorization_decorator(
            self.AUTH0_CONFIGURATION, on_successful_authorization
        )

    @AUTH0_CONFIGURATION.default
    def _auth_configuration_factory(self) -> Optional[Auth0Config]:
        if in_ci():
            # In our GH Actions CI test workflow, there's no easy way to get
            # access to a working Auth0 secret, and we don't need one to
            # prove that the server is working, so just return None
            return None

        local_path = os.path.join(
            os.path.realpath(os.path.dirname(os.path.realpath(__file__))), "local"
        )
        auth0_configuration = get_local_secret(local_path, "justice_counts_auth0")
        if not auth0_configuration:
            raise JusticeCountsAuthorizationError(
                code="no_justice_counts_access",
                description="You are not authorized to access this application",
            )
        return Auth0Config.from_config_json(json.loads(auth0_configuration))
