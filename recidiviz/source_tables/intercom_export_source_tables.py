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
"""Build the BQ table to track Intercom outbound content data export jobs."""

from recidiviz.calculator.query.state.dataset_config import INTERCOM_EXPORT_DATASET
from recidiviz.intercom.intercom_export_columns import (
    build_intercom_export_contacts_schema,
    build_intercom_export_metadata_export_tracker_schema,
    build_intercom_export_tickets_schema,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)

INTERCOM_EXPORT_METADATA_DATASET = "intercom_export_metadata"
INTERCOM_EXPORT_METADATA_EXPORT_TRACKER_TABLE_ID = "export_tracker"
INTERCOM_EXPORT_TICKETS_TABLE_ID = "tickets"
INTERCOM_EXPORT_CONTACTS_TABLE_ID = "contacts"


def build_intercom_export_source_tables() -> SourceTableCollection:
    """Add Intercom tickets and contacts tables to the Intercom export source table collection"""

    intercom_export_collection = SourceTableCollection(
        dataset_id=INTERCOM_EXPORT_DATASET,
        update_config=SourceTableCollectionUpdateConfig.protected(),
        description="Dataset that contains user interaction and survey data from Intercom",
    )

    intercom_export_collection.add_source_table(
        table_id=INTERCOM_EXPORT_TICKETS_TABLE_ID,
        description="Contains inbound support tickets created when users reach out to us via Intercom",
        schema_fields=build_intercom_export_tickets_schema(),
    )

    intercom_export_collection.add_source_table(
        table_id=INTERCOM_EXPORT_CONTACTS_TABLE_ID,
        description="Contains contact information from Intercom tickets",
        schema_fields=build_intercom_export_contacts_schema(),
    )

    return intercom_export_collection


def build_intercom_export_metadata_source_tables() -> SourceTableCollection:
    """Add an Intercom export cloud run job tracker table to an Intercom metadata source table collection"""

    intercom_export_metadata_collection = SourceTableCollection(
        dataset_id=INTERCOM_EXPORT_METADATA_DATASET,
        update_config=SourceTableCollectionUpdateConfig.protected(),
        description="Dataset that contains metadata related to Intercom exports",
    )

    intercom_export_metadata_collection.add_source_table(
        table_id=INTERCOM_EXPORT_METADATA_EXPORT_TRACKER_TABLE_ID,
        description="Tracking for Intercom export cloud run job",
        schema_fields=build_intercom_export_metadata_export_tracker_schema(),
    )

    return intercom_export_metadata_collection
