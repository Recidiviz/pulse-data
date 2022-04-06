# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Manages acquiring and releasing the lock for the normalized_state update process."""
import logging

from recidiviz.cloud_storage.gcs_pseudo_lock_manager import (
    GCSPseudoLockAlreadyExists,
    GCSPseudoLockDoesNotExist,
    GCSPseudoLockManager,
    postgres_to_bq_lock_name_for_schema,
)
from recidiviz.persistence.database.schema_utils import SchemaType

NORMALIZED_STATE_UPDATE_LOCK_NAME = "NORMALIZED_STATE_UPDATE_PROCESS"


class NormalizedStateUpdateLockManager:
    """Manages acquiring and releasing the lock for updating the normalized_state
    dataset, as well as determining if the update can proceed given other ongoing
    processes.
    """

    def __init__(self) -> None:
        self.lock_manager = GCSPseudoLockManager()

    def acquire_lock(self, lock_id: str) -> None:
        """Acquires the normalized_state update lock, or refreshes the timeout of the
        lock if a lock with the given |lock_id| already exists. The presence of the
        lock tells other ongoing processes to yield until the lock has been released.

        Acquiring the lock does NOT tell us if we can proceed with the update. You
        must call can_proceed() to determine if all blocking processes have
        successfully yielded.

        Throws if a lock with a different lock_id exists for this update.
        """
        lock_name = NORMALIZED_STATE_UPDATE_LOCK_NAME
        try:
            self.lock_manager.lock(
                lock_name,
                payload=lock_id,
                expiration_in_seconds=self._export_lock_timeout_for_update(),
            )
        except GCSPseudoLockAlreadyExists as e:
            previous_lock_id = self.lock_manager.get_lock_payload(lock_name)
            logging.info("Lock contents: %s", previous_lock_id)
            if lock_id != previous_lock_id:
                raise GCSPseudoLockAlreadyExists(
                    f"UUID {lock_id} does not match existing lock's UUID {previous_lock_id}"
                ) from e

    def can_proceed(self) -> bool:
        """Returns True if all blocking processes have stopped, and we can proceed with
        the export, False otherwise.
        """

        if not self.is_locked():
            raise GCSPseudoLockDoesNotExist(
                f"Must acquire the lock for [{NORMALIZED_STATE_UPDATE_LOCK_NAME}] "
                f"before checking if can proceed"
            )

        no_blocking_locks = self.lock_manager.no_active_locks_with_prefix(
            postgres_to_bq_lock_name_for_schema(SchemaType.STATE)
        )
        return no_blocking_locks

    def release_lock(self) -> None:
        """Releases the normalized_state update lock."""
        self.lock_manager.unlock(NORMALIZED_STATE_UPDATE_LOCK_NAME)

    def is_locked(self) -> bool:
        return self.lock_manager.is_locked(NORMALIZED_STATE_UPDATE_LOCK_NAME)

    @staticmethod
    def _export_lock_timeout_for_update() -> int:
        """Defines the exported lock timeouts permitted. Currently set to one hour in
        length.

        Update jobs are not expected to take longer than the alotted time, but if they
        do so, they will de facto relinquish their hold on the acquired lock."""
        return 3600
