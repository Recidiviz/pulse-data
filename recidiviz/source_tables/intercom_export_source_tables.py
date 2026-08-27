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
"""Build the BQ table that tracks Intercom export cloud run jobs."""

from recidiviz.intercom.intercom_export_columns import (
    build_intercom_export_metadata_export_tracker_schema,
)
from recidiviz.source_tables.source_table_config import (
    CALC_UPDATE_GROUPS,
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)

INTERCOM_EXPORT_METADATA_DATASET = "intercom_export_metadata"
INTERCOM_EXPORT_METADATA_EXPORT_TRACKER_TABLE_ID = "export_tracker"


def build_intercom_export_metadata_source_tables() -> SourceTableCollection:
    """Add an Intercom export cloud run job tracker table to an Intercom metadata source table collection"""

    intercom_export_metadata_collection = SourceTableCollection(
        update_groups=CALC_UPDATE_GROUPS,
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
