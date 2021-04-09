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
"""Manages acquiring and releasing the lock for the ingest process that writes
data to Postgres for a given region.
"""

from contextlib import contextmanager
from typing import List, Iterator

from recidiviz.cloud_storage.gcs_pseudo_lock_manager import (
    GCSPseudoLockManager,
)

GCS_TO_POSTGRES_INGEST_RUNNING_LOCK_NAME = "INGEST_PROCESS_RUNNING_"


class DirectIngestRegionLockManager:
    """Manages acquiring and releasing the lock for the ingest process that writes
    data to Postgres for a given region.
    """

    def __init__(self, region_code: str, blocking_locks: List[str]) -> None:
        """
        Args:
            region_code: The region code for the region to lock / unlock ingest for.
            blocking_locks: Any locks that, if present, mean ingest into Postgres
                cannot proceed for this region.
        """
        self.region_code = region_code
        self.blocking_locks = blocking_locks
        self.lock_manager = GCSPseudoLockManager()

    def is_locked(self) -> bool:
        """Returns True if the ingest lock is held for the region associated with this
        lock manager.
        """
        return self.lock_manager.is_locked(
            self._ingest_lock_name_for_region_code(self.region_code)
        )

    def can_proceed(self) -> bool:
        """Returns True if ingest can proceed for the region associated with this
        lock manager.
        """
        for lock in self.blocking_locks:
            if self.lock_manager.is_locked(lock):
                return False
        return True

    @contextmanager
    def using_region_lock(
        self,
        *,
        expiration_in_seconds: int,
    ) -> Iterator[None]:
        """A context manager for acquiring the lock for a given region. Usage:
        with lock_manager.using_region_lock(expiration_in_seconds=60):
           ... do work requiring the lock
        """
        with self.lock_manager.using_lock(
            self._ingest_lock_name_for_region_code(self.region_code),
            expiration_in_seconds=expiration_in_seconds,
        ):
            yield

    @staticmethod
    def _ingest_lock_name_for_region_code(region_code: str) -> str:
        return GCS_TO_POSTGRES_INGEST_RUNNING_LOCK_NAME + region_code.upper()
