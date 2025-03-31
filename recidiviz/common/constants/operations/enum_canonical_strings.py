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

# DirectIngestLockActor
direct_ingest_lock_actor_process = "PROCESS"
direct_ingest_lock_actor_adhoc = "ADHOC"

# DirectIngestLockResource
direct_ingest_lock_resource_bucket = "BUCKET"
direct_ingest_lock_resource_operations_database = "OPERATIONS_DATABASE"
direct_ingest_lock_resource_big_query_raw_data_dataset = "BIG_QUERY_RAW_DATA_DATASET"

# DirectIngestRawFileImportStatus
direct_ingest_raw_file_import_status_started = "STARTED"
direct_ingest_raw_file_import_status_deferred = "DEFERRED"
direct_ingest_raw_file_import_status_succeeded = "SUCCEEDED"
direct_ingest_raw_file_import_status_failed_unknown = "FAILED_UNKNOWN"
direct_ingest_raw_file_import_status_failed_pre_import_normalization_step = (
    "FAILED_PRE_IMPORT_NORMALIZATION_STEP"
)
direct_ingest_raw_file_import_status_failed_load_step = "FAILED_LOAD_STEP"
direct_ingest_raw_file_import_status_failed_validation_step = "FAILED_VALIDATION_STEP"
direct_ingest_raw_file_import_status_failed_import_blocked = "FAILED_IMPORT_BLOCKED"
