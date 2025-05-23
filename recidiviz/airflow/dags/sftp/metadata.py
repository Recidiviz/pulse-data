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
"""Constants used to reference metadata fields in the SFTP DAG"""

from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataResourceLockResource,
)

REMOTE_FILE_PATH = "remote_file_path"
SFTP_TIMESTAMP = "sftp_timestamp"
DOWNLOADED_FILE_PATH = "downloaded_file_path"
POST_PROCESSED_FILE_PATH = "post_processed_file_path"
POST_PROCESSED_NORMALIZED_FILE_PATH = "post_processed_normalized_file_path"
INGEST_READY_FILE_PATH = "ingest_ready_file_path"
UPLOADED_FILE_PATH = "uploaded_file_path"

# task names
START_SFTP = "start_sftp"
END_SFTP = "end_sftp"


# SFTP / SSH errors tend to be transient, so we sometimes need to retry tasks in order
# to get them to succeed. this should ONLY be applied to fully idempotent tasks.
TASK_RETRIES = 3


# yaml config file names and location
SFTP_ENABLED_YAML_CONFIG = "sftp_enabled_in_airflow_config.yaml"
SFTP_EXCLUDED_PATHS_YAML_CONFIG = "sftp_excluded_remote_file_paths.yaml"


def get_configs_bucket(project_id: str) -> GcsfsBucketPath:
    return GcsfsBucketPath(f"{project_id}-configs")


# metadata for raw data resource locks
SFTP_REQUIRED_RESOURCES = [
    DirectIngestRawDataResourceLockResource.BUCKET,
]
SFTP_RESOURCE_LOCK_DESCRIPTION = (
    "Lock acquired for duration of ingest file upload during the SFTP DAG"
)
SFTP_RESOURCE_LOCK_TTL_SECONDS = 3 * 60 * 60  # 3 hours
