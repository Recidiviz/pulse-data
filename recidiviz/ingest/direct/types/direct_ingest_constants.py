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
"""Constants used by the direct ingest system."""

# Recidiviz-managed raw data columns
FILE_ID_COL_NAME = "file_id"
FILE_ID_COL_DESCRIPTION = "The ID of the file this row was extracted from"

IS_DELETED_COL_NAME = "is_deleted"
IS_DELETED_COL_DESCRIPTION = (
    "Whether this row is inferred deleted via omission from more recent files"
)

UPDATE_DATETIME_COL_NAME = "update_datetime"
UPDATE_DATETIME_COL_DESCRIPTION = (
    "The timestamp of the file this row was extracted from"
)

# Recidiviz-managed ingest view results columns
UPPER_BOUND_DATETIME_COL_NAME = "__upper_bound_datetime_inclusive"
MATERIALIZATION_TIME_COL_NAME = "__materialization_time"

# Constants used in the raw data imports
DIRECT_INGEST_UNPROCESSED_PREFIX = "unprocessed"
DIRECT_INGEST_PROCESSED_PREFIX = "processed"
