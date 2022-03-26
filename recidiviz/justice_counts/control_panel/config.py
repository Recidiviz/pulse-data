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

import os

import attr

from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    SQLAlchemyEngineManager,
)
from recidiviz.utils.environment import in_development

JUSTICE_COUNTS_DATABASE_KEY = SQLAlchemyDatabaseKey.for_schema(
    SchemaType.JUSTICE_COUNTS
)
JUSTICE_COUNTS_DEVELOPMENT_POSTGRES_URL = "JUSTICE_COUNTS_DEVELOPMENT_POSTGRES_URL"


@attr.define
class Config:
    DATABASE_KEY: SQLAlchemyDatabaseKey = JUSTICE_COUNTS_DATABASE_KEY
    # Indicates whether CSRF protection is enabled for the whole app. Should be set to False for tests.
    WTF_CSRF_ENABLED: bool = True
    DB_URL: str = attr.field()

    @DB_URL.default
    def _db_url_factory(self) -> str:
        if in_development():
            return os.environ[JUSTICE_COUNTS_DEVELOPMENT_POSTGRES_URL]

        return SQLAlchemyEngineManager.get_server_postgres_instance_url(
            database_key=self.DATABASE_KEY
        )
