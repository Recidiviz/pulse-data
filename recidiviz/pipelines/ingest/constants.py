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
"""Constants shared across ingest pipelines (activity, identity, transforms)."""
from google.cloud import bigquery

from recidiviz.ingest.direct.types.direct_ingest_constants import (
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)

INGEST_VIEW_RESULTS_SCHEMA_COLUMNS = [
    bigquery.SchemaField(
        UPPER_BOUND_DATETIME_COL_NAME,
        field_type=bigquery.enums.SqlTypeNames.DATETIME.value,
        mode="REQUIRED",
        description=(
            "Of all raw data inputs to this ingest view, this value is the most recent time that any "
            "of those raw data tables received new data (using update_datetime)."
        ),
    ),
    bigquery.SchemaField(
        MATERIALIZATION_TIME_COL_NAME,
        field_type=bigquery.enums.SqlTypeNames.DATETIME.value,
        mode="REQUIRED",
        description="This datetime (UTC) is when the data in this table was materialized.",
    ),
]
INGEST_VIEW_RESULTS_SCHEMA_COLUMN_NAMES = {
    c.name for c in INGEST_VIEW_RESULTS_SCHEMA_COLUMNS
}
