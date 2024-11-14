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
direct_ingest_status_raw_data_reimport_started = "RAW_DATA_REIMPORT_STARTED"
direct_ingest_status_initial_state = "INITIAL_STATE"
direct_ingest_status_raw_data_import_in_progress = "RAW_DATA_IMPORT_IN_PROGRESS"
direct_ingest_status_ready_to_flash = "READY_TO_FLASH"
direct_ingest_status_flash_in_progress = "FLASH_IN_PROGRESS"
direct_ingest_status_flash_completed = "FLASH_COMPLETED"
direct_ingest_status_raw_data_reimport_canceled = "RAW_DATA_REIMPORT_CANCELED"
direct_ingest_status_raw_data_reimport_cancellation_in_progress = (
    "RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS"
)
direct_ingest_status_raw_data_up_to_date = "RAW_DATA_UP_TO_DATE"
direct_ingest_status_stale_raw_data = "STALE_RAW_DATA"
direct_ingest_status_no_raw_data_reimport_in_progress = (
    "NO_RAW_DATA_REIMPORT_IN_PROGRESS"
)

# DirectIngestLockActor
direct_ingest_lock_actor_process = "PROCESS"
direct_ingest_lock_actor_adhoc = "ADHOC"

# DirectIngestLockResource
direct_ingest_lock_resource_bucket = "BUCKET"
direct_ingest_lock_resource_operations_database = "OPERATIONS_DATABASE"
direct_ingest_lock_resource_big_query_raw_data_dataset = "BIG_QUERY_RAW_DATA_DATASET"

# DirectIngestRawFileImportStatus
direct_ingest_raw_file_import_status_started = "STARTED"
direct_ingest_raw_file_import_status_succeeded = "SUCCEEDED"
direct_ingest_raw_file_import_status_failed_unknown = "FAILED_UNKNOWN"
direct_ingest_raw_file_import_status_failed_pre_import_normalization_step = (
    "FAILED_PRE_IMPORT_NORMALIZATION_STEP"
)
direct_ingest_raw_file_import_status_failed_load_step = "FAILED_LOAD_STEP"
direct_ingest_raw_file_import_status_failed_validation_step = "FAILED_VALIDATION_STEP"
direct_ingest_raw_file_import_status_failed_import_blocked = "FAILED_IMPORT_BLOCKED"
