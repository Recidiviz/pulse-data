# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""
Application settings for the resource search system.

To use, import Settings and instantiate it. It inherits
other settings for our database and external API handling.
"""

from typing import Literal

from pydantic import Field, computed_field
from pydantic_settings import BaseSettings

from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    URL,
    SQLAlchemyEngineManager,
)
from recidiviz.utils import secrets
from recidiviz.utils.environment import in_gcp_production, in_gcp_staging

PROJECT_ID = "recidiviz-staging" if in_gcp_production() is False else "recidiviz-123"

# For classes w/ Pydantic, define all fields as class attributes with type hints
# and a either a default value or Field(...).
# Note: Use @computed_field for values that are derived from other fields.


class DatabaseSettings(BaseSettings):
    db_name: str = Field(..., description="The name of the database to connect to.")
    schema_type: SchemaType = SchemaType.RESOURCE_SEARCH

    @computed_field
    def database_key(self) -> SQLAlchemyDatabaseKey:
        return SQLAlchemyDatabaseKey(schema_type=self.schema_type, db_name=self.db_name)

    @computed_field
    def postgres_uri(self) -> URL:
        if in_gcp_staging() or in_gcp_production():
            return SQLAlchemyEngineManager.get_server_postgres_instance_url(
                database_key=self.database_key(),
            )

        raise ValueError(
            "Local postgres has not yet been configured for Resource Search!"
        )


# TODO(#46942): Secrets will need to be accessed from recidiviz-rnd-planner or copied to recidiviz-staging
SecretApiKey = lambda key: Field(  # pylint: disable=C3001
    default_factory=lambda: secrets.get_secret(key, project_id=PROJECT_ID) or key
)


# TODO(#46942): Ensure these secrets exist for staging for each API and ensure
# We have appropriate action for testing.
class ExternalAPISettings(BaseSettings):
    google_api_key: str = SecretApiKey("google_maps_api_key")
    google_places_api_key: str = google_api_key
    google_geocoding_api_key: str = google_api_key
    google_routes_api_key: str = google_api_key
    openai_api_key: str = SecretApiKey("open_ai_api_key")


class Settings(DatabaseSettings, ExternalAPISettings):
    """Application settings for the resource search system."""

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO" if in_gcp_staging() or in_gcp_production() else "DEBUG"
    )
