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
"""Class for managing reads and writes to DirectIngestRawDataResourceLock"""
import datetime
from typing import Dict, List, Optional

from more_itertools import one
from sqlalchemy import text

from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataLockActor,
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.cloud_sql_connection_mixin import (
    CloudSqlConnectionMixin,
)
from recidiviz.persistence.database.schema.operations import schema
from recidiviz.persistence.database.schema_entity_converter.schema_entity_converter import (
    convert_schema_object_to_entity,
)
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations import entities
from recidiviz.persistence.errors import (
    DirectIngestRawDataResourceLockAlreadyReleasedError,
    DirectIngestRawDataResourceLockHeldError,
)


class DirectIngestRawDataResourceLockManager(CloudSqlConnectionMixin):
    """Class for managing reads and writes to DirectIngestRawDataResourceLock

    n.b. while this class manages interaction w/ our file metadata tables, they are also
    queried in the airflow context with raw sql. all relevant updates to the table's
    logic must also be reflected in the raw data import dag's query logic.
    """

    def __init__(
        self,
        *,
        region_code: str,
        raw_data_source_instance: DirectIngestInstance,
        with_proxy: bool,
    ) -> None:
        self.region_code = region_code.upper()
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        self.raw_data_source_instance = raw_data_source_instance
        self.with_proxy = with_proxy

    def _get_unreleased_locks_for_resources(
        self, resources: List[DirectIngestRawDataResourceLockResource], session: Session
    ) -> List[schema.DirectIngestRawDataResourceLock]:
        """Returns locks that have released set to False for the provided |resources|,
        regardless if that lock is expired.
        """

        return (
            session.query(schema.DirectIngestRawDataResourceLock)
            .filter(
                schema.DirectIngestRawDataResourceLock.lock_resource.in_(
                    list(map(lambda x: x.value, resources))
                ),
                schema.DirectIngestRawDataResourceLock.region_code == self.region_code,
                schema.DirectIngestRawDataResourceLock.raw_data_source_instance
                == self.raw_data_source_instance.value,
                # pylint: disable=singleton-comparison
                schema.DirectIngestRawDataResourceLock.released == False,
            )
            .all()
        )

    @staticmethod
    def _is_lock_unreleased_but_expired(
        lock: schema.DirectIngestRawDataResourceLock,
    ) -> bool:
        return (
            not lock.released
            and lock.lock_ttl_seconds is not None
            and (
                datetime.datetime.now(tz=datetime.UTC) - lock.lock_acquisition_time
            ).total_seconds()
            > lock.lock_ttl_seconds
        )

    def _update_unreleased_but_expired_locks_for_resources(
        self, resources: List[DirectIngestRawDataResourceLockResource], session: Session
    ) -> List[schema.DirectIngestRawDataResourceLock]:
        """Returns all locks for the provided |resources| that are unreleased,
        releasing but not returning any locks that were unreleased but expired.
        """
        unreleased_locks = self._get_unreleased_locks_for_resources(resources, session)

        unreleased_not_expired_locks: List[schema.DirectIngestRawDataResourceLock] = []
        for unreleased_lock in unreleased_locks:
            if self._is_lock_unreleased_but_expired(unreleased_lock):
                unreleased_lock.released = True
            else:
                unreleased_not_expired_locks.append(unreleased_lock)

        return unreleased_not_expired_locks

    def _register_new_locks(
        self,
        resources: List[DirectIngestRawDataResourceLockResource],
        actor: DirectIngestRawDataLockActor,
        description: str,
        ttl_seconds: Optional[int],
        session: Session,
    ) -> List[schema.DirectIngestRawDataResourceLock]:
        """Creates and returns new locks with the provided information."""
        new_locks: List[schema.DirectIngestRawDataResourceLock] = []
        for resource in resources:
            new_lock = schema.DirectIngestRawDataResourceLock(
                lock_actor=actor.value,
                lock_resource=resource.value,
                region_code=self.region_code,
                raw_data_source_instance=self.raw_data_source_instance.value,
                released=False,
                lock_acquisition_time=datetime.datetime.now(tz=datetime.UTC),
                lock_ttl_seconds=ttl_seconds,
                lock_description=description,
            )

            session.add(new_lock)
            new_locks.append(new_lock)
        return new_locks

    def _get_lock_by_id(
        self, lock_id: int, session: Session
    ) -> schema.DirectIngestRawDataResourceLock:
        """Looks up a lock by |lock_id|"""
        lock = (
            session.query(schema.DirectIngestRawDataResourceLock)
            .filter_by(lock_id=lock_id)
            .all()
        )
        if not lock:
            raise LookupError(f"Could not find lock with id [{lock_id}]")

        return one(lock)

    def get_current_lock_summary(
        self,
    ) -> Dict[
        DirectIngestRawDataResourceLockResource, Optional[DirectIngestRawDataLockActor]
    ]:
        """Returns a map of the lock resource to the actor who holds it."""
        resources = list(DirectIngestRawDataResourceLockResource)
        lock_resource_to_holder = {
            lock.lock_resource: (lock.lock_actor if not lock.released else None)
            for lock in self.get_most_recent_locks_for_resources(resources)
        }

        return {
            resource: lock_resource_to_holder.get(resource) for resource in resources
        }

    def get_most_recent_locks_for_all_resources(
        self,
    ) -> List[entities.DirectIngestRawDataResourceLock]:
        """Returns the most recent locks (as dictated by lock lock_acquisition_time), if
        they exist, for all resources."""
        return self.get_most_recent_locks_for_resources(
            list(DirectIngestRawDataResourceLockResource)
        )

    def get_most_recent_locks_for_resources(
        self, resources: List[DirectIngestRawDataResourceLockResource]
    ) -> List[entities.DirectIngestRawDataResourceLock]:
        """Returns the most recent locks (as dictated by lock lock_acquisition_time), if
        they exist, for the provided |resources|."""
        resource_str = ",".join([f"'{resource.value}'" for resource in resources])

        with self.get_session(
            database_key=self.database_key, with_proxy=self.with_proxy
        ) as session:
            self._update_unreleased_but_expired_locks_for_resources(resources, session)
            # flush here so changes are propagated for us to read w/ our next query
            session.flush()

            query = f"""
              SELECT
                lock_id,
                lock_resource,
                lock_actor,
                region_code,
                raw_data_source_instance,
                released,
                lock_acquisition_time,
                lock_ttl_seconds,
                lock_description
              FROM (
                SELECT
                  *,
                  ROW_NUMBER() OVER (PARTITION BY lock_resource ORDER BY lock_acquisition_time DESC) as recency_rank
                FROM direct_ingest_raw_data_resource_lock
                WHERE region_code = '{self.region_code}'
                AND raw_data_source_instance = '{self.raw_data_source_instance.value}'
                AND lock_resource in ({resource_str})
              ) a
              WHERE recency_rank = 1
            """

            results = session.execute(text(query))

            if not results:
                return []

            return [
                convert_schema_object_to_entity(
                    schema.DirectIngestRawDataResourceLock(**result),
                    entities.DirectIngestRawDataResourceLock,
                    populate_direct_back_edges=False,
                )
                for result in results
            ]

    def get_lock_by_id(self, lock_id: int) -> entities.DirectIngestRawDataResourceLock:
        """Returns a lock with the provided |lock_id|, updating the lock's released
        status if it is unreleased but expired. If the lock does not exist, a
        LookupError will be raised.
        """
        with self.get_session(
            database_key=self.database_key, with_proxy=self.with_proxy
        ) as session:
            lock = self._get_lock_by_id(lock_id, session)

            if self._is_lock_unreleased_but_expired(lock):
                lock.released = True

            return convert_schema_object_to_entity(
                lock,
                entities.DirectIngestRawDataResourceLock,
                populate_direct_back_edges=False,
            )

    def release_lock_by_id(
        self, lock_id: int
    ) -> entities.DirectIngestRawDataResourceLock:
        """Releases the lock associated with the provided |lock_id|. If the lock does
        not exist, a LookupError will be raised. If the lock is already released,
        a DirectIngestRawDataResourceLockAlreadyReleasedError will be raised.
        """
        with self.get_session(
            database_key=self.database_key, with_proxy=self.with_proxy
        ) as session:
            lock = self._get_lock_by_id(lock_id, session)
            if lock.released:
                raise DirectIngestRawDataResourceLockAlreadyReleasedError(
                    f"Lock [{lock_id}] is already released so cannot release it"
                )

            lock.released = True
            return convert_schema_object_to_entity(
                lock,
                entities.DirectIngestRawDataResourceLock,
                populate_direct_back_edges=False,
            )

    def acquire_all_locks(
        self,
        actor: DirectIngestRawDataLockActor,
        description: str,
        ttl_seconds: Optional[int] = None,
    ) -> List[entities.DirectIngestRawDataResourceLock]:
        """Attempts to acquire all resource locks. If successful, returns the newly
        created locks; otherwise raises DirectIngestRawDataResourceLockHeldError.
        """
        return self.acquire_lock_for_resources(
            list(DirectIngestRawDataResourceLockResource),
            actor=actor,
            description=description,
            ttl_seconds=ttl_seconds,
        )

    def acquire_lock_for_resources(
        self,
        resources: List[DirectIngestRawDataResourceLockResource],
        actor: DirectIngestRawDataLockActor,
        description: str,
        ttl_seconds: Optional[int] = None,
    ) -> List[entities.DirectIngestRawDataResourceLock]:
        """Attempts to acquire the resource locks for the provided |resources|.
        If successful, returns the newly created locks; otherwise raises
        DirectIngestRawDataResourceLockHeldError.
        """
        with self.get_session(
            database_key=self.database_key, with_proxy=self.with_proxy, autocommit=False
        ) as session:
            unreleased_locks = self._update_unreleased_but_expired_locks_for_resources(
                resources, session
            )

            if unreleased_locks:
                raise DirectIngestRawDataResourceLockHeldError(
                    f"Failed to acquire locks in [{self.region_code}]"
                    f"[{self.raw_data_source_instance.value}]. Locks "
                    f"{[lock.lock_id for lock in unreleased_locks]} are unreleased for "
                    f"{[lock.lock_resource for lock in unreleased_locks]} respectively"
                )

            new_locks = self._register_new_locks(
                resources, actor, description, ttl_seconds, session
            )
            # commit here so that we can get the lock_id populated on the object
            try:
                session.commit()
            except Exception as e:
                session.rollback()
                raise e

            return [
                convert_schema_object_to_entity(
                    new_lock,
                    entities.DirectIngestRawDataResourceLock,
                    populate_direct_back_edges=False,
                )
                for new_lock in new_locks
            ]
