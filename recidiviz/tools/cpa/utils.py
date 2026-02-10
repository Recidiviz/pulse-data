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
"""Shared utilities for CPA tools."""

from google.cloud.secretmanager_v1 import SecretManagerServiceClient
from sqlalchemy import create_engine
from sqlalchemy.engine import URL, Engine

GCP_PROJECT_ID = "recidiviz-rnd-planner"
DATABASE_CONNECTION_STRING_SECRET_NAME = (
    "RECIDIVIZ_POSTGRES_PROD_CONNECTION_STRING"  # nosec B105
)
DATABASE_PASSWORD_SECRET_NAME = "RECIDIVIZ_POSTGRES_PASSWORD_PROD"  # nosec B105
DATABASE_USERNAME_SECRET_NAME = "RECIDIVIZ_POSTGRES_USERNAME"  # nosec B105
SERVICE_ACCOUNT_KEY_SECRET_NAME = "ut-cpa-service-account-key"  # nosec B105
DEFAULT_DATABASE_NAME = "recidiviz"
PROXY_HOST = "127.0.0.1"
PROXY_PORT = 5441
BQ_PROJECT = "recidiviz-123"
CLIENT_BQ_TABLE = "recidiviz-123.reentry.client_materialized"


def get_secret(secret_name: str) -> str:
    """Fetches a secret value from Secret Manager."""
    client = SecretManagerServiceClient()
    full_name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=full_name)
    return response.payload.data.decode("UTF-8")


def create_db_engine(
    db_password: str,
    db_username: str,
) -> Engine:
    """Creates a SQLAlchemy engine for the CPA database."""
    url = URL.create(
        drivername="postgresql",
        username=db_username,
        password=db_password,
        host=PROXY_HOST,
        port=PROXY_PORT,
        database=DEFAULT_DATABASE_NAME,
    )
    return create_engine(url)
