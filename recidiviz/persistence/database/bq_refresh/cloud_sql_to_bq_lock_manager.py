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
"""Manages acquiring and releasing the lock for the Cloud SQL -> BQ refresh."""
import logging

from recidiviz.cloud_storage.gcs_pseudo_lock_manager import (
    GCSPseudoLockManager,
    GCSPseudoLockAlreadyExists,
    GCSPseudoLockDoesNotExist,
)
from recidiviz.ingest.direct.controllers.direct_ingest_region_lock_manager import (
    GCS_TO_POSTGRES_INGEST_RUNNING_LOCK_NAME,
)
from recidiviz.persistence.database.schema_utils import SchemaType


POSTGRES_TO_BQ_EXPORT_RUNNING_LOCK_NAME = "EXPORT_PROCESS_RUNNING_"


def postgres_to_bq_lock_name_for_schema(schema: SchemaType) -> str:
    return POSTGRES_TO_BQ_EXPORT_RUNNING_LOCK_NAME + schema.value.upper()


class CloudSqlToBQLockManager:
    """Manages acquiring and releasing the lock for the Cloud SQL -> BQ refresh, as well
    as determining if the refresh can proceed given other ongoing processes.
    """

    def __init__(self) -> None:
        self.lock_manager = GCSPseudoLockManager()

    def acquire_lock(self, lock_id: str, schema_type: SchemaType) -> None:
        """Acquires the CloudSQL -> BQ refresh lock for a given schema, or refreshes the
         timeout of the lock if a lock with the given |lock_id| already exists. The
         presence of the lock tells other ongoing processes to yield until the lock has
         been released.

         Acquiring the lock does NOT tell us if we can proceed with the refresh. You
         must call can_proceed() to determine if all blocking processes have
         successfully yielded.

        Throws if a lock with a different lock_id exists for this schema.
        """
        lock_name = postgres_to_bq_lock_name_for_schema(schema_type)
        try:
            self.lock_manager.lock(
                lock_name,
                contents=lock_id,
                expiration_in_seconds=self._export_lock_timeout_for_schema(schema_type),
            )
        except GCSPseudoLockAlreadyExists as e:
            previous_lock_id = self.lock_manager.get_lock_contents(lock_name)
            logging.info("Lock contents: %s", previous_lock_id)
            if lock_id != previous_lock_id:
                raise GCSPseudoLockAlreadyExists(
                    f"UUID {lock_id} does not match existing lock's UUID {previous_lock_id}"
                ) from e

    def can_proceed(self, schema_type: SchemaType) -> bool:
        """Returns True if all blocking processes have stopped and we can proceed with
        the export, False otherwise.
        """

        if not self.is_locked(schema_type):
            raise GCSPseudoLockDoesNotExist(
                f"Must acquire the lock for [{schema_type}] befor checking if can proceed"
            )

        if schema_type not in (
            SchemaType.STATE,
            SchemaType.JAILS,
            SchemaType.OPERATIONS,
        ):
            return True

        no_regions_running = self.lock_manager.no_active_locks_with_prefix(
            GCS_TO_POSTGRES_INGEST_RUNNING_LOCK_NAME
        )
        return no_regions_running

    def release_lock(self, schema_type: SchemaType) -> None:
        """Releases the CloudSQL -> BQ refresh lock for a given schema."""
        self.lock_manager.unlock(postgres_to_bq_lock_name_for_schema(schema_type))

    def is_locked(self, schema_type: SchemaType) -> bool:
        return self.lock_manager.is_locked(
            postgres_to_bq_lock_name_for_schema(schema_type)
        )

    @staticmethod
    def _export_lock_timeout_for_schema(_schema_type: SchemaType) -> int:
        """Defines the exported lock timeouts permitted based on the schema arg.
        For the moment all lock timeouts are set to one hour in length.

        Export jobs may take longer than the alotted time, but if they do so, they
        will de facto relinquish their hold on the acquired lock."""
        return 3600
