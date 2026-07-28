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
"""GCS Upload for Intercom outbound data"""

from datetime import datetime

from recidiviz.cloud_storage.gcs_file_system import CSV_CONTENT_TYPE
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.io.local_file_contents_handle import LocalFileContentsHandle


# TODO(OBT-27442) move this logic to upload_local_file method on GCSFileSystem
def intercom_gcs_upload(
    intercom_source_path: str, destination_gcs_path: GcsfsFilePath
) -> None:
    """Upload the contents of a given Intercom CSV file to GCS"""
    gcs_fs = GcsfsFactory.build()
    gcs_fs.upload_from_contents_handle_stream(
        path=destination_gcs_path,
        contents_handle=LocalFileContentsHandle(
            intercom_source_path, cleanup_file=False
        ),
        content_type=CSV_CONTENT_TYPE,
    )


def generate_intercom_outbound_content_gcs_path(
    project_id: str, file_base_name: str, update_datetime: datetime
) -> GcsfsFilePath:
    return GcsfsFilePath.from_bucket_and_blob_name(
        bucket_name=f"{project_id}-intercom-export",
        blob_name=f"outbound-content/{update_datetime.isoformat()}/{file_base_name}.csv",
    )
