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
from typing import Iterator, List

from recidiviz.cloud_storage.gcs_pseudo_lock_manager import (
    GCSPseudoLockManager,
    postgres_to_bq_lock_name_for_schema,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.controllers.direct_ingest_instance import (
    DirectIngestInstance,
)
from recidiviz.persistence.database.schema_utils import (
    DirectIngestSchemaType,
    SchemaType,
)

GCS_TO_POSTGRES_INGEST_RUNNING_LOCK_PREFIX = "INGEST_PROCESS_RUNNING_"
STATE_GCS_TO_POSTGRES_INGEST_RUNNING_LOCK_PREFIX = (
    f"{GCS_TO_POSTGRES_INGEST_RUNNING_LOCK_PREFIX}STATE_"
)
JAILS_GCS_TO_POSTGRES_INGEST_RUNNING_LOCK_PREFIX = (
    f"{GCS_TO_POSTGRES_INGEST_RUNNING_LOCK_PREFIX}JAILS_"
)


class DirectIngestRegionLockManager:
    """Manages acquiring and releasing the lock for the ingest process that writes
    data to Postgres for a given region's ingest instance.
    """

    def __init__(
        self,
        region_code: str,
        ingest_instance: DirectIngestInstance,
        blocking_locks: List[str],
    ) -> None:
        """
        Args:
            region_code: The region code for the region to lock / unlock ingest for.
            blocking_locks: Any locks that, if present, mean ingest into Postgres
                cannot proceed for this region.
        """
        self.region_code = region_code
        self.ingest_instance = ingest_instance
        self.blocking_locks = blocking_locks
        self.lock_manager = GCSPseudoLockManager()

    def is_locked(self) -> bool:
        """Returns True if the ingest lock is held for the region associated with this
        lock manager.
        """
        return self.lock_manager.is_locked(self._ingest_lock_name_for_instance())

    def can_proceed(self) -> bool:
        """Returns True if ingest can proceed for the region associated with this
        lock manager.
        """
        for lock in self.blocking_locks:
            if self.lock_manager.is_locked(lock):
                return False
        return True

    def acquire_lock(self) -> None:
        self.lock_manager.lock(self._ingest_lock_name_for_instance())

    def release_lock(self) -> None:
        self.lock_manager.unlock(self._ingest_lock_name_for_instance())

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
            self._ingest_lock_name_for_instance(),
            expiration_in_seconds=expiration_in_seconds,
        ):
            yield

    @staticmethod
    def for_state_ingest(
        state_code: StateCode, ingest_instance: DirectIngestInstance
    ) -> "DirectIngestRegionLockManager":
        return DirectIngestRegionLockManager.for_direct_ingest(
            region_code=state_code.value,
            ingest_instance=ingest_instance,
            schema_type=SchemaType.STATE,
        )

    @staticmethod
    def for_direct_ingest(
        region_code: str,
        ingest_instance: DirectIngestInstance,
        schema_type: DirectIngestSchemaType,
    ) -> "DirectIngestRegionLockManager":
        return DirectIngestRegionLockManager(
            region_code=region_code,
            ingest_instance=ingest_instance,
            blocking_locks=[
                postgres_to_bq_lock_name_for_schema(schema_type),
                postgres_to_bq_lock_name_for_schema(SchemaType.OPERATIONS),
            ],
        )

    def _ingest_lock_name_for_instance(self) -> str:
        if StateCode.is_state_code(self.region_code):
            return (
                STATE_GCS_TO_POSTGRES_INGEST_RUNNING_LOCK_PREFIX
                + self.region_code.upper()
                + f"_{self.ingest_instance.name}"
            )
        return (
            JAILS_GCS_TO_POSTGRES_INGEST_RUNNING_LOCK_PREFIX + self.region_code.upper()
        )
