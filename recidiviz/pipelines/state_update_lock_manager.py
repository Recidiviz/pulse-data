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
"""Manages acquiring and releasing the lock for the state update processes."""
import logging
import time
from typing import Optional

from recidiviz.cloud_storage.gcs_pseudo_lock_manager import (
    GCSPseudoLockAlreadyExists,
    GCSPseudoLockManager,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

STATE_AGNOSTIC_STATE_UPDATE_PROCESS = "STATE_AGNOSTIC_STATE_UPDATE_PROCESS"
STATE_SPECIFIC_STATE_UPDATE_PROCESS = "STATE_SPECIFIC_STATE_UPDATE_PROCESS"

LOCK_WAIT_SLEEP_INTERVAL_SECONDS = 60  # 1 minute
LOCK_WAIT_SLEEP_MAXIMUM_TIMEOUT = 60 * 60 * 4  # 4 hours


def state_update_lock_name(
    ingest_instance: DirectIngestInstance,
    state_code: Optional[StateCode],
) -> str:
    if state_code:
        return f"{STATE_SPECIFIC_STATE_UPDATE_PROCESS}_{state_code.value.upper()}_{ingest_instance.value.upper()}"

    return f"{STATE_AGNOSTIC_STATE_UPDATE_PROCESS}_{ingest_instance.value.upper()}"


class StateUpdateLockManager:
    """Manages acquiring and releasing the lock for state update
    datasets, as well as determining if the update can proceed given other ongoing
    processes.
    """

    def __init__(
        self,
        ingest_instance: DirectIngestInstance,
        state_code_filter: Optional[StateCode] = None,
    ) -> None:
        self.lock_manager = GCSPseudoLockManager()
        self.lock_name = state_update_lock_name(ingest_instance, state_code_filter)
        self.ingest_instance = ingest_instance

    def acquire_lock(
        self, lock_id: str, lock_wait_timeout: Optional[int] = None
    ) -> None:
        """
        Acquires the state update lock, or refreshes the timeout of the
        lock if a lock with the given |lock_id| already exists. The presence of the
        lock tells other ongoing processes to yield until the lock has been released.

        Can set a timeout to try to lock for before giving up. If no timeout is set, will try for up to 4 hours.
        """

        if lock_wait_timeout is None:
            lock_wait_timeout = LOCK_WAIT_SLEEP_MAXIMUM_TIMEOUT

        secs_waited = 0
        can_proceed = False
        while secs_waited < lock_wait_timeout:
            if can_proceed := self._can_proceed_with_lock(lock_id):
                break
            logging.info(
                "Waiting to acquire lock [%s]...",
                self.lock_name,
            )
            time.sleep(LOCK_WAIT_SLEEP_INTERVAL_SECONDS)
            secs_waited += LOCK_WAIT_SLEEP_INTERVAL_SECONDS

        if not can_proceed:
            raise ValueError(
                f"Could not acquire lock after waiting {LOCK_WAIT_SLEEP_MAXIMUM_TIMEOUT} seconds for {self.lock_name}."
            )

        try:
            self.lock_manager.lock(
                self.lock_name,
                payload=lock_id,
                expiration_in_seconds=self._export_lock_timeout_for_update(),
            )
        except GCSPseudoLockAlreadyExists as e:
            previous_lock_id = self.lock_manager.get_lock_payload(self.lock_name)
            logging.info("Lock contents: %s", previous_lock_id)
            if lock_id != previous_lock_id:
                raise GCSPseudoLockAlreadyExists(
                    f"UUID {lock_id} does not match existing lock's UUID {previous_lock_id}"
                ) from e

    def _can_proceed_with_lock(self, lock_id: str) -> bool:
        """
        Returns True if all blocking processes have stopped and lock doesn't already exist for another lock id,
        allowing us to proceed with locking.
        """

        if self.is_locked():
            previous_lock_id = self.lock_manager.get_lock_payload(self.lock_name)
            return lock_id == previous_lock_id

        if self.lock_name.startswith(STATE_AGNOSTIC_STATE_UPDATE_PROCESS):
            no_blocking_locks = self.lock_manager.no_active_locks_with_prefix(
                STATE_SPECIFIC_STATE_UPDATE_PROCESS, self.ingest_instance.value
            )
            return no_blocking_locks

        no_blocking_locks = self.lock_manager.no_active_locks_with_prefix(
            STATE_AGNOSTIC_STATE_UPDATE_PROCESS,
            self.ingest_instance.value,
        )

        return no_blocking_locks

    def release_lock(self) -> None:
        """Releases the state update lock."""
        self.lock_manager.unlock(self.lock_name)

    def is_locked(self) -> bool:
        return self.lock_manager.is_locked(self.lock_name)

    @staticmethod
    def _export_lock_timeout_for_update() -> int:
        """Defines the exported lock timeouts permitted. Currently set to one hour in
        length.

        Update jobs are not expected to take longer than the alotted time, but if they
        do so, they will de facto relinquish their hold on the acquired lock."""
        return 3600
