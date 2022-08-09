# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Defines constants for use in the operations context."""

# DirectIngestInstance enum
direct_ingest_instance_primary = "PRIMARY"
direct_ingest_instance_secondary = "SECONDARY"

# DirectIngestStatus
direct_ingest_status_rerun_with_raw_data_import_started = (
    "RERUN_WITH_RAW_DATA_IMPORT_STARTED"
)
direct_ingest_status_standard_rerun_started = "STANDARD_RERUN_STARTED"
direct_ingest_status_raw_data_import_in_progress = "RAW_DATA_IMPORT_IN_PROGRESS"
direct_ingest_status_ingest_view_materialization_in_progress = (
    "INGEST_VIEW_MATERIALIZATION_IN_PROGRESS"
)
direct_ingest_status_extract_and_merge_in_progress = "EXTRACT_AND_MERGE_IN_PROGRESS"
direct_ingest_status_ready_to_flash = "READY_TO_FLASH"
direct_ingest_status_flash_in_progress = "FLASH_IN_PROGRESS"
direct_ingest_status_flash_completed = "FLASH_COMPLETED"
direct_ingest_status_up_to_date = "UP_TO_DATE"
direct_ingest_status_stale_raw_data = "STALE_RAW_DATA"
direct_ingest_status_no_rerun_in_progress = "NO_RERUN_IN_PROGRESS"
