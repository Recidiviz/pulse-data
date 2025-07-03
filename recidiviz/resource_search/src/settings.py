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

"""Application settings for the resource search system."""

from typing import Literal

from pydantic import Field
from pydantic_settings import BaseSettings

from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.database.sqlalchemy_engine_manager import (
    URL,
    SQLAlchemyEngineManager,
)
from recidiviz.tools.postgres import local_postgres_helpers
from recidiviz.utils import secrets
from recidiviz.utils.environment import in_gcp_production, in_gcp_staging

PROJECT_ID = "recidiviz-staging" if in_gcp_production() is False else "recidiviz-123"


class Settings(BaseSettings):
    """
    Application settings for the resource search system.

    This class defines the settings for the resource search system, including the API keys for the Google Maps and OpenAI APIs, as well as the database connection settings.
    """

    google_api_key: str = Field(
        default_factory=lambda: secrets.get_secret(
            "google_maps_api_key", project_id=PROJECT_ID
        )
        or "google_maps_api_key"
    )
    google_places_api_key: str = google_api_key
    google_geocoding_api_key: str = google_api_key
    google_routes_api_key: str = google_api_key
    schema_type: SchemaType = SchemaType.RESOURCE_SEARCH
    database_key: SQLAlchemyDatabaseKey = SQLAlchemyDatabaseKey.for_schema(schema_type)
    postgres_uri: URL = (
        local_postgres_helpers.on_disk_postgres_db_url()
        if not in_gcp_staging() or not in_gcp_production()
        else SQLAlchemyEngineManager.get_server_postgres_instance_url(
            database_key=database_key,
            secret_prefix_override=schema_type.value,
        )
    )

    # Open AI is used by default by llama index
    openai_api_key: str = Field(
        default_factory=lambda: secrets.get_secret(
            "open_ai_api_key", project_id=PROJECT_ID
        )
        or "open_ai_api_key",
    )
    # Logger
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO"
    )


settings = Settings()
