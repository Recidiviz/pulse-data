# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
from recidiviz.cloud_storage.gcsfs_path import GcsfsDirectoryPath, GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.utils.string import StrictStringFormatter

DocumentId = str

DOCUMENT_STORE_BUCKET = "gs://{project_id}-document-blob-storage"


def gcs_path_for_document(
    project_id: str, state_code: StateCode, document_id: str, sandbox_bucket: str | None
) -> GcsfsFilePath:
    bucket = (
        sandbox_bucket
        if sandbox_bucket
        else StrictStringFormatter().format(
            DOCUMENT_STORE_BUCKET, project_id=project_id
        )
    )

    dir_path = GcsfsDirectoryPath.from_bucket_and_blob_name(
        bucket_name=bucket, blob_name=state_code.value
    )
    return GcsfsFilePath.from_directory_and_file_name(
        dir_path=dir_path, file_name=f"{document_id}.txt"
    )
