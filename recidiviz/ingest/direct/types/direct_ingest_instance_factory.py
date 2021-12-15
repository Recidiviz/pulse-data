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
"""Factory with class methods for creating DirectIngestInstance values.

Separated out into its own file to avoid introducing cloud storage / cloud function
imports to the DirectIngestInstance definition file.
"""

from recidiviz.cloud_functions.direct_ingest_bucket_name_utils import (
    is_primary_ingest_bucket,
    is_secondary_ingest_bucket,
)
from recidiviz.cloud_storage.gcsfs_path import GcsfsBucketPath
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance


class DirectIngestInstanceFactory:
    """Factory with class methods for creating DirectIngestInstance values."""

    @classmethod
    def for_ingest_bucket(cls, ingest_bucket: GcsfsBucketPath) -> DirectIngestInstance:
        if is_primary_ingest_bucket(ingest_bucket.bucket_name):
            return DirectIngestInstance.PRIMARY
        if is_secondary_ingest_bucket(ingest_bucket.bucket_name):
            return DirectIngestInstance.SECONDARY
        raise ValueError(f"Unexpected ingest bucket [{ingest_bucket.bucket_name}]")
