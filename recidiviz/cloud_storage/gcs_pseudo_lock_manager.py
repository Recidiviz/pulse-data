# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Creates pseudo lock manager class built on GCSFS.
This isn't a traditional lock and is not suitable for all the general use cases."""

from datetime import datetime
from typing import Optional
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.utils import metadata

EXPORT_RUNNING_LOCK_NAME = 'EXPORT_PROCESS_RUNNING'
INGEST_RUNNING_LOCK_PREFIX = 'INGEST_PROCESS_RUNNING_'


class GCSPseudoLockManager:
    """Class implementing pseudo lock manager using GCS File System. Not a general locks class - may have race
    conditions when locks state altered by multiple processes. A single lock is only ever acquired and released by one
    process, but its presence may be read or examined by another process"""

    _TIME_FORMAT = "%m/%d/%Y, %H:%M:%S"

    def __init__(self, project_id: Optional[str] = None):
        if not project_id:
            project_id = metadata.project_id()
        self.fs = GcsfsFactory.build()
        self.bucket_name = f'{project_id}-gcslock'

    def no_active_locks_with_prefix(self, prefix: str) -> bool:
        """Checks to see if any locks exist with prefix"""
        return len(self.fs.ls_with_blob_prefix(blob_prefix=prefix, bucket_name=self.bucket_name)) == 0

    def lock(self, name: str, contents: Optional[str] = None) -> None:
        """"Locks @param name by generating new file"""
        if not self.is_locked(name):
            path = GcsfsFilePath(bucket_name=self.bucket_name, blob_name=name)
            if contents is None:
                contents = datetime.now().strftime(self._TIME_FORMAT)
            self.fs.upload_from_string(path, contents, 'text/plain')
        else:
            raise GCSPseudoLockAlreadyExists(f"Lock with the name {name} already exists in the bucket "
                                             f"{self.bucket_name}")

    def unlock(self, name: str) -> None:
        """Unlocks @param name by deleting file with name"""
        if self.is_locked(name):
            path = GcsfsFilePath(bucket_name=self.bucket_name, blob_name=name)
            self.fs.delete(path)
        else:
            raise GCSPseudoLockDoesNotExist(f"Lock with the name {name} does not yet exist in the bucket "
                                            f"{self.bucket_name}")

    def is_locked(self, name: str) -> bool:
        """Checks if @param name is locked by checking if file exists. Returns true if locked, false if unlocked"""
        path = GcsfsFilePath(bucket_name=self.bucket_name, blob_name=name)
        return self.fs.exists(path)


class GCSPseudoLockAlreadyExists(ValueError):
    pass


class GCSPseudoLockDoesNotExist(ValueError):
    pass
