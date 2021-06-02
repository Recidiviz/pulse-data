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
import json
import logging
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, Iterator, Optional

import attr
import dateutil.parser

from recidiviz.cloud_storage.gcs_file_system import GCSBlobDoesNotExistError
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.utils import metadata

LOCK_TIME_KEY = "lock_time"
CONTENTS_KEY = "contents"
EXPIRATION_IN_SECONDS_KEY = "expiration_in_seconds"


POSTGRES_TO_BQ_EXPORT_RUNNING_LOCK_NAME = "EXPORT_PROCESS_RUNNING_"


def postgres_to_bq_lock_name_for_schema(schema: SchemaType) -> str:
    return POSTGRES_TO_BQ_EXPORT_RUNNING_LOCK_NAME + schema.value.upper()


@attr.s(auto_attribs=True, frozen=True)
class GCSPseudoLockContents:
    """Defines the schema for the contents of the lock used by GCSPseudoLockManager."""

    lock_time: datetime
    contents: Optional[str] = attr.ib(default=None)
    expiration_in_seconds: Optional[int] = attr.ib(default=None)

    def to_json(self) -> Dict[str, Any]:
        output_json: Dict[str, Any] = {
            LOCK_TIME_KEY: self.lock_time,
        }
        if self.contents is not None:
            output_json[CONTENTS_KEY] = self.contents
        if self.expiration_in_seconds is not None:
            output_json[EXPIRATION_IN_SECONDS_KEY] = self.expiration_in_seconds
        return output_json

    @staticmethod
    def from_json_string(lock_contents: str) -> Optional["GCSPseudoLockContents"]:
        """Returns a GCSPseudoLockContents object from a json string.

        If the string contents do not match the known structure, it logs a warning
        and then passively returns None."""

        try:
            json_contents = json.loads(lock_contents)
        except json.decoder.JSONDecodeError:
            logging.warning(
                "Could not decode lock's internal json. "
                "Could be the result of a schema change, so returning None"
            )
            return None

        if not isinstance(json_contents, dict):
            logging.warning(
                "Could not decode lock's internal json. "
                "Could be the result of a schema change, so returning None"
            )
            return None

        if LOCK_TIME_KEY not in json_contents:
            logging.warning(
                "Could not find lock_time key in lock's internal json. "
                "Could be the result of a schema change, so returning None"
            )
            return None

        try:
            lock_time = dateutil.parser.parse(json_contents[LOCK_TIME_KEY])
        except (TypeError, ValueError):
            logging.warning(
                "lock_time key did not correspond to valid datetime in lock's internal json. "
                "Could be the result of a schema change, so returning None"
            )
            return None

        contents = json_contents.get(CONTENTS_KEY)
        if contents is not None and not isinstance(contents, str):
            logging.warning(
                "contents key did not correspond to valid string in lock's internal json. "
                "Could be the result of a schema change, so returning None"
            )
            return None

        expiration_in_seconds = json_contents.get(EXPIRATION_IN_SECONDS_KEY)
        if expiration_in_seconds is not None and not isinstance(
            expiration_in_seconds, int
        ):
            logging.warning(
                "expiration_in_seconds key did not correspond to valid int in lock's internal json. "
                "Could be the result of a schema change, so returning None"
            )
            return None

        return GCSPseudoLockContents(
            lock_time=lock_time,
            contents=contents,
            expiration_in_seconds=expiration_in_seconds,
        )


class GCSPseudoLockManager:
    """Class implementing pseudo lock manager using GCS File System. Not a general locks class - may have race
    conditions when locks state altered by multiple processes. A single lock is only ever acquired and released by one
    process, but its presence may be read or examined by another process"""

    def __init__(self, project_id: Optional[str] = None):
        if not project_id:
            project_id = metadata.project_id()
        self.fs = GcsfsFactory.build()
        self.bucket_name = f"{project_id}-gcslock"

    def no_active_locks_with_prefix(self, prefix: str) -> bool:
        """Checks to see if any locks exist with prefix"""
        return (
            len(
                self.fs.ls_with_blob_prefix(
                    bucket_name=self.bucket_name, blob_prefix=prefix
                )
            )
            == 0
        )

    def unlock_locks_with_prefix(self, prefix: str) -> None:
        locks_with_prefix = self.fs.ls_with_blob_prefix(
            bucket_name=self.bucket_name, blob_prefix=prefix
        )
        if len(locks_with_prefix) == 0:
            raise GCSPseudoLockDoesNotExist(
                f"No locks with the prefix {prefix} exist in the bucket "
                f"{self.bucket_name}"
            )
        for lock in locks_with_prefix:
            if isinstance(lock, GcsfsFilePath):
                self.fs.delete(lock)

    def lock(
        self,
        name: str,
        contents: Optional[str] = None,
        expiration_in_seconds: Optional[int] = None,
    ) -> None:
        """Locks @param name by generating new file. The body of the lock is json-encoded and contains
        the lock time, the caller's custom @param contents (if provided), and the
        @param expiration_in_seconds (if provided).
        """
        if self.is_locked(name):
            raise GCSPseudoLockAlreadyExists(
                f"Lock with the name {name} already exists in the bucket "
                f"{self.bucket_name}"
            )

        lock_contents = GCSPseudoLockContents(
            lock_time=datetime.now(),
            contents=contents,
            expiration_in_seconds=expiration_in_seconds,
        )
        path = GcsfsFilePath(bucket_name=self.bucket_name, blob_name=name)
        self.fs.upload_from_string(
            path, json.dumps(lock_contents.to_json(), default=str), "text/plain"
        )
        logging.debug("Created lock file with name: %s", name)

    def unlock(self, name: str) -> None:
        """Unlocks @param name by deleting file with name"""
        path = GcsfsFilePath(bucket_name=self.bucket_name, blob_name=name)

        # We are not using `is_locked` here because we want to delete expired
        # locks explicitly.
        if self.fs.exists(path):
            self.fs.delete(path)
            logging.debug("Deleting lock file with name: %s", name)
        else:
            raise GCSPseudoLockDoesNotExist(
                f"Lock with the name {name} does not yet exist in the bucket "
                f"{self.bucket_name}"
            )

    def is_locked(self, name: str) -> bool:
        """Checks if @param name is locked by checking if file exists. Returns true if locked, false if unlocked"""
        path = GcsfsFilePath(bucket_name=self.bucket_name, blob_name=name)
        try:
            lock_string = self.fs.download_as_string(path)
        except GCSBlobDoesNotExistError:
            return False
        gcs_pseudo_lock_contents = GCSPseudoLockContents.from_json_string(lock_string)
        return (
            not gcs_pseudo_lock_contents
            or not gcs_pseudo_lock_contents.expiration_in_seconds
            or gcs_pseudo_lock_contents.lock_time
            + timedelta(seconds=gcs_pseudo_lock_contents.expiration_in_seconds)
            > datetime.now()
        )

    def get_lock_contents(self, name: str) -> Optional[str]:
        """Returns contents of specified lock as string"""
        path = GcsfsFilePath(bucket_name=self.bucket_name, blob_name=name)
        try:
            lock_string = self.fs.download_as_string(path)
        except GCSBlobDoesNotExistError as e:
            raise GCSPseudoLockDoesNotExist(
                f"Lock with the name {name} does not yet exist in the bucket "
                f"{self.bucket_name}"
            ) from e
        gcs_pseudo_lock_contents = GCSPseudoLockContents.from_json_string(lock_string)
        if not gcs_pseudo_lock_contents:
            return None
        return gcs_pseudo_lock_contents.contents

    @contextmanager
    def using_lock(
        self,
        name: str,
        contents: Optional[str] = None,
        expiration_in_seconds: Optional[int] = None,
    ) -> Iterator[None]:
        self.lock(name, contents, expiration_in_seconds)
        try:
            yield
        finally:
            self.unlock(name)


class GCSPseudoLockAlreadyExists(ValueError):
    pass


class GCSPseudoLockDoesNotExist(ValueError):
    pass
