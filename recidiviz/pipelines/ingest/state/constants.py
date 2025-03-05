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
"""Constants for the ingest pipeline."""
from typing import Optional, Set, Tuple

from google.cloud import bigquery

from recidiviz.ingest.direct.types.direct_ingest_constants import (
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.persistence.entity.generate_primary_key import PrimaryKey

# Beam does not have a standard datetime coder that it uses to decode/encode between steps
# for datetime objects, therefore we will use UTC timestamps for any keys that require
# datetime objects.
UpperBoundDate = float

IngestViewName = str

ExternalId = str
ExternalIdType = str
ExternalIdKey = Tuple[str, str]
ExternalIdClusterEdge = Tuple[ExternalIdKey, Optional[ExternalIdKey]]
ExternalIdCluster = Tuple[ExternalIdKey, Set[ExternalIdKey]]

EntityClassName = str
EntityKey = Tuple[PrimaryKey, EntityClassName]
Error = str
UniqueConstraintName = str


INGEST_VIEW_RESULTS_SCHEMA_COLUMNS = [
    bigquery.SchemaField(
        UPPER_BOUND_DATETIME_COL_NAME,
        field_type=bigquery.enums.SqlTypeNames.DATETIME.value,
        mode="REQUIRED",
    ),
    bigquery.SchemaField(
        MATERIALIZATION_TIME_COL_NAME,
        field_type=bigquery.enums.SqlTypeNames.DATETIME.value,
        mode="REQUIRED",
    ),
]
INGEST_VIEW_RESULTS_SCHEMA_COLUMN_NAMES = {
    c.name for c in INGEST_VIEW_RESULTS_SCHEMA_COLUMNS
}
