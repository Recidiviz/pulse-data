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
from recidiviz.intercom.intercom_exports_columns import (
    build_intercom_export_tracker_schema,
)
from recidiviz.source_tables.source_table_config import (
    SourceTableCollection,
    SourceTableCollectionUpdateConfig,
)

INTERCOM_EXPORT_TRACKER_TABLE_ID = "export_tracker"


def build_intercom_export_tracker_table() -> SourceTableCollection:
    """Add an Intercom export cloud run job tracker table to the Intercom export source table collection"""

    intercom_collection = SourceTableCollection(
        dataset_id=INTERCOM_EXPORT_DATASET,
        update_config=SourceTableCollectionUpdateConfig.protected(),
        description="Dataset that contains user interaction and survey data from Intercom",
    )

    intercom_collection.add_source_table(
        table_id=INTERCOM_EXPORT_TRACKER_TABLE_ID,
        description="Tracking for Intercom export cloud run job",
        schema_fields=build_intercom_export_tracker_schema(),
    )

    return intercom_collection
