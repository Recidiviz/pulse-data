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
"""Column name constants and schema definitions for BQ tables that tracks Intercom export cloud run job and Intercom tickets."""

from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.enums import SqlTypeNames

# export_tracker table constants
EXPORT_DATETIME_COLUMN_NAME = "export_datetime"
EXPORT_WINDOW_START_INCLUSIVE_COLUMN_NAME = "export_window_start_inclusive"
EXPORT_WINDOW_END_INCLUSIVE_COLUMN_NAME = "export_window_end_inclusive"
STATUS_COLUMN_NAME = "status"
# tickets table constants
TICKET_ID_COLUMN_NAME = "ticket_id"
TICKET_CATEGORY_COLUMN_NAME = "ticket_category"
TICKET_NAME_COLUMN_NAME = "ticket_name"
DESCRIPTION_COLUMN_NAME = "description"
AUTHOR_NAME_COLUMN_NAME = "author_name"
AUTHOR_EMAIL_COLUMN_NAME = "author_email"
CREATED_AT_COLUMN_NAME = "created_at"
UPDATED_AT_COLUMN_NAME = "updated_at"
TICKET_STATE_COLUMN_NAME = "ticket_state"
RAW_TICKET_JSON_COLUMN_NAME = "raw_ticket_json"


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


def build_intercom_export_tickets_schema() -> list[SchemaField]:
    """Returns the schema for BQ table for Intercom tickets."""

    return [
        SchemaField(
            name=TICKET_ID_COLUMN_NAME,
            field_type=SqlTypeNames.STRING,
            mode="REQUIRED",
            description="The ID of the ticket",
        ),
        SchemaField(
            name=TICKET_CATEGORY_COLUMN_NAME,
            field_type=SqlTypeNames.STRING,
            mode="REQUIRED",
            description="The category of the ticket",
        ),
        SchemaField(
            name=TICKET_NAME_COLUMN_NAME,
            field_type=SqlTypeNames.STRING,
            mode="REQUIRED",
            description="The name of the ticket",
        ),
        SchemaField(
            name=DESCRIPTION_COLUMN_NAME,
            field_type=SqlTypeNames.STRING,
            mode="REQUIRED",
            description="The ticket's description",
        ),
        SchemaField(
            name=AUTHOR_NAME_COLUMN_NAME,
            field_type=SqlTypeNames.STRING,
            mode="REQUIRED",
            description="The name of the author of the ticket",
        ),
        SchemaField(
            name=AUTHOR_EMAIL_COLUMN_NAME,
            field_type=SqlTypeNames.STRING,
            mode="REQUIRED",
            description="The email of the author of the ticket",
        ),
        SchemaField(
            name=CREATED_AT_COLUMN_NAME,
            field_type=SqlTypeNames.TIMESTAMP,
            mode="REQUIRED",
            description="The timestamp for the date and time the ticket was created",
        ),
        SchemaField(
            name=UPDATED_AT_COLUMN_NAME,
            field_type=SqlTypeNames.TIMESTAMP,
            mode="REQUIRED",
            description="The timestamp for the last date and time the ticket was updated",
        ),
        SchemaField(
            name=TICKET_STATE_COLUMN_NAME,
            field_type=SqlTypeNames.STRING,
            mode="REQUIRED",
            description="The state of the ticket",
        ),
        SchemaField(
            name=RAW_TICKET_JSON_COLUMN_NAME,
            field_type=SqlTypeNames.STRING,
            mode="REQUIRED",
            description="The raw JSON of the original ticket response",
        ),
    ]
