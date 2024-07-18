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
"""Cloud SQL utilities for use in Airflow"""

import datetime

from recidiviz.persistence.database.schema_type import SchemaType


def cloud_sql_conn_id_for_schema_type(schema_type: SchemaType) -> str:
    """These connection IDs will correspond to an Airflow Connection JSON or URI string
    stored in the Secrets Manager under `airflow-connections-{this connection id}`."""
    return f"{schema_type.value.lower()}_postgres_conn_id"


def postgres_formatted_current_datetime_utc_str() -> str:
    return postgres_formatted_datetime_with_tz(datetime.datetime.now(tz=datetime.UTC))


def postgres_formatted_datetime_with_tz(dt: datetime.datetime) -> str:
    if not dt.tzinfo:
        raise ValueError(f"Must provide a timezone, none found on {dt}")

    return dt.strftime("%Y-%m-%d %H:%M:%S.%f %Z")
