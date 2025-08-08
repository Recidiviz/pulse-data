# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Custom operators that wrap google's GCSListObjectsOperator to provide some extra
filtering
"""
from typing import Any, List

from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.utils.context import Context

from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.gcs.filename_parts import is_path_normalized
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    DIRECT_INGEST_PROCESSED_PREFIX,
    DIRECT_INGEST_UNPROCESSED_PREFIX,
)


class DirectIngestListNormalizedFileOperator(GCSListObjectsOperator):
    """Wraps GCSListObjectsOperator to filter paths down to file paths that match the
    direct ingest raw filename regex
    """

    def execute(self, context: Context) -> List[str]:
        file_paths = super().execute(context)
        gcsfs_file_paths = [
            GcsfsFilePath.from_bucket_and_blob_name(self.bucket, file_path)
            for file_path in file_paths
        ]

        return [
            gcsfs_file_path.abs_path()
            for gcsfs_file_path in gcsfs_file_paths
            if is_path_normalized(gcsfs_file_path)
        ]


class DirectIngestListNormalizedUnprocessedFilesOperator(
    DirectIngestListNormalizedFileOperator
):
    """Applies an unprocessed prefix and filters paths down to file paths that match
    the direct ingest raw filename regex
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(prefix=DIRECT_INGEST_UNPROCESSED_PREFIX, *args, **kwargs)


class DirectIngestListNormalizedProcessedFilesOperator(
    DirectIngestListNormalizedFileOperator
):
    """Applies a processed prefix and filters paths down to file paths that match
    the direct ingest raw filename regex
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(prefix=DIRECT_INGEST_PROCESSED_PREFIX, *args, **kwargs)
