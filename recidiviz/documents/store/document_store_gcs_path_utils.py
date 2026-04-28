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
"""Utility functions for document store GCS path construction."""
from recidiviz.cloud_storage.gcsfs_path import (
    GcsfsBucketPath,
    GcsfsDirectoryPath,
    GcsfsFilePath,
)
from recidiviz.common.constants.states import StateCode


def document_blob_storage_bucket_name(project_id: str, state_code: StateCode) -> str:
    """Name of the bucket that stores all uploaded document contents."""
    return f"{project_id}-{state_code.lower_hyphened_code()}-document-blob-storage"


def temp_document_store_output_bucket_name(
    project_id: str, state_code: StateCode
) -> str:
    """Name of the bucket that stores temporary CSV output from document upload tasks."""
    return f"{project_id}-{state_code.lower_hyphened_code()}-temp-document-store-output"


def gcs_path_for_document(
    project_id: str, state_code: StateCode, document_contents_id: str
) -> GcsfsFilePath:
    """Returns the GCS path where a document's contents are stored."""
    return GcsfsFilePath.from_directory_and_file_name(
        dir_path=GcsfsBucketPath(
            document_blob_storage_bucket_name(project_id, state_code)
        ),
        file_name=f"{document_contents_id}.txt",
    )


def gcs_directory_for_task_output(
    project_id: str,
    state_code: StateCode,
    job_id: str,
) -> GcsfsDirectoryPath:
    return GcsfsDirectoryPath.from_bucket_and_blob_name(
        bucket_name=temp_document_store_output_bucket_name(project_id, state_code),
        blob_name=job_id,
    )


def gcs_path_for_task_output(
    project_id: str,
    state_code: StateCode,
    job_id: str,
    task_index: int,
    batch_index: int,
) -> GcsfsFilePath:
    """Returns the GCS path for a CSV output file produced by a document upload task."""
    return GcsfsFilePath.from_directory_and_file_name(
        dir_path=gcs_directory_for_task_output(project_id, state_code, job_id),
        file_name=f"task_{task_index}_{batch_index}.csv",
    )
