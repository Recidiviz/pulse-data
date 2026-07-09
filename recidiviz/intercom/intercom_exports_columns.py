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
"""Column name constants and schema definitions for a BQ table that tracks Intercom export cloud run job."""

from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.enums import SqlTypeNames

EXPORT_DATETIME_COLUMN_NAME = "export_datetime"
EXPORT_WINDOW_START_INCLUSIVE_COLUMN_NAME = "export_window_start_inclusive"
EXPORT_WINDOW_END_INCLUSIVE_COLUMN_NAME = "export_window_end_inclusive"
STATUS_COLUMN_NAME = "status"


def build_intercom_export_tracker_schema() -> list[SchemaField]:
    """Returns the schema for BQ table that tracks Intercom export cloud run job."""

    return [
        SchemaField(
            name=EXPORT_DATETIME_COLUMN_NAME,
            field_type=SqlTypeNames.TIMESTAMP,
            mode="REQUIRED",
            description="The datetime of the cloud run export job",
        ),
        SchemaField(
            name=EXPORT_WINDOW_START_INCLUSIVE_COLUMN_NAME,
            field_type=SqlTypeNames.TIMESTAMP,
            mode="REQUIRED",
            description="The timestamp for start of export window, inclusive",
        ),
        SchemaField(
            name=EXPORT_WINDOW_END_INCLUSIVE_COLUMN_NAME,
            field_type=SqlTypeNames.TIMESTAMP,
            mode="REQUIRED",
            description="The timestamp for end of export window, inclusive",
        ),
        SchemaField(
            name=STATUS_COLUMN_NAME,
            field_type=SqlTypeNames.STRING,
            mode="REQUIRED",
            description="The state of the export job (SUCCESS or FAILURE)",
        ),
    ]
