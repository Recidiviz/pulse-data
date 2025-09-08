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
"""Implements tests for the DirectIngestRawDataResourceLockManager."""
import datetime
from itertools import chain
from typing import Any
from unittest import TestCase

import pytest
from freezegun import freeze_time
from more_itertools import one
from sqlalchemy.exc import IntegrityError

from recidiviz.common.constants.operations.direct_ingest_raw_data_resource_lock import (
    DirectIngestRawDataLockActor,
    DirectIngestRawDataResourceLockResource,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.metadata import (
    direct_ingest_raw_data_resource_lock_manager,
)
from recidiviz.ingest.direct.metadata.direct_ingest_raw_data_resource_lock_manager import (
    DirectIngestRawDataResourceLockAlreadyReleasedError,
    DirectIngestRawDataResourceLockHeldError,
    DirectIngestRawDataResourceLockManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tests.utils.patch_helpers import after
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers
from recidiviz.tools.postgres.local_postgres_helpers import OnDiskPostgresLaunchResult

LOCK_MANAGER_PACKAGE_NAME = direct_ingest_raw_data_resource_lock_manager.__name__


@pytest.mark.uses_db
class DirectIngestRawDataResourceLockManagerTest(TestCase):
    """Implements tests for DirectIngestRawDataResourceLockManager."""

    # Stores the location of the postgres DB for this test run
    postgres_launch_result: OnDiskPostgresLaunchResult

    @classmethod
    def setUpClass(cls) -> None:
        cls.postgres_launch_result = (
            local_postgres_helpers.start_on_disk_postgresql_database()
        )

    def setUp(self) -> None:
        self.operations_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_persistence_helpers.use_on_disk_postgresql_database(
            self.postgres_launch_result, self.operations_key
        )
        self.us_xx_manager = DirectIngestRawDataResourceLockManager(
            region_code=StateCode.US_XX.value,
            raw_data_source_instance=DirectIngestInstance.PRIMARY,
            with_proxy=False,
        )
        self.all_resources = [
            DirectIngestRawDataResourceLockResource.BIG_QUERY_RAW_DATA_DATASET,
            DirectIngestRawDataResourceLockResource.OPERATIONS_DATABASE,
            DirectIngestRawDataResourceLockResource.BUCKET,
        ]
        self.raw_data = [
            DirectIngestRawDataResourceLockResource.BIG_QUERY_RAW_DATA_DATASET
        ]

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.operations_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.postgres_launch_result
        )

    def test_acquire_lock_no_existing(self) -> None:
        single_lock_list = self.us_xx_manager.acquire_lock_for_resources(
            self.all_resources[:1],
            DirectIngestRawDataLockActor.ADHOC,
            "testing-testing-123",
        )

        assert len(single_lock_list) == 1

        two_lock_list = self.us_xx_manager.acquire_lock_for_resources(
            self.all_resources[1:],
            DirectIngestRawDataLockActor.ADHOC,
            "testing-testing-123",
        )

        assert len(two_lock_list) == 2

        all_written_locks = list(chain(single_lock_list, two_lock_list))

        persisted_locks = self.us_xx_manager.get_most_recent_locks_for_resources(
            self.all_resources
        )

        assert persisted_locks is not None
        assert len(persisted_locks) == 3

        written_locks_ids = {lock.lock_id for lock in all_written_locks}
        persisted_locks_ids = {lock.lock_id for lock in persisted_locks}

        assert written_locks_ids == persisted_locks_ids

    def test_acquire_lock_existing_unreleased_unexpired_no_ttl(self) -> None:
        raw_data_locks = self.us_xx_manager.acquire_lock_for_resources(
            self.raw_data,
            DirectIngestRawDataLockActor.ADHOC,
            "testing-testing-123",
        )

        assert len(raw_data_locks) == 1

        # fails for the same resource
        with self.assertRaisesRegex(
            DirectIngestRawDataResourceLockHeldError,
            r"Failed to acquire locks in \[US_XX\]\[PRIMARY\]. Locks \[\d+\] are "
            r"unreleased for \['BIG_QUERY_RAW_DATA_DATASET'\] respectively",
        ):
            _ = self.us_xx_manager.acquire_lock_for_resources(
                self.raw_data,
                DirectIngestRawDataLockActor.ADHOC,
                "testing-testing-123",
            )

        # also fails for all resources
        with self.assertRaisesRegex(
            DirectIngestRawDataResourceLockHeldError,
            r"Failed to acquire locks in \[US_XX\]\[PRIMARY\]. Locks \[\d+\] are "
            r"unreleased for \['BIG_QUERY_RAW_DATA_DATASET'\] respectively",
        ):
            _ = self.us_xx_manager.acquire_lock_for_resources(
                self.all_resources,
                DirectIngestRawDataLockActor.ADHOC,
                "testing-testing-123",
            )

    def test_acquire_lock_existing_unreleased_unexpired_with_ttl(self) -> None:
        active_lock = self.us_xx_manager.acquire_lock_for_resources(
            self.raw_data,
            DirectIngestRawDataLockActor.PROCESS,
            "testing-testing-123",
            ttl_seconds=10 * 24 * 60 * 60,  # 10 days
        )

        assert len(active_lock) == 1

        # fails for the same resource
        with self.assertRaisesRegex(
            DirectIngestRawDataResourceLockHeldError,
            r"Failed to acquire locks in \[US_XX\]\[PRIMARY\]. Locks \[\d+\] are "
            r"unreleased for \['BIG_QUERY_RAW_DATA_DATASET'\] respectively",
        ):
            _ = self.us_xx_manager.acquire_lock_for_resources(
                self.raw_data,
                DirectIngestRawDataLockActor.ADHOC,
                "testing-testing-123",
            )

        # also fails for all resources
        with self.assertRaisesRegex(
            DirectIngestRawDataResourceLockHeldError,
            r"Failed to acquire locks in \[US_XX\]\[PRIMARY\]. Locks \[\d+\] are "
            r"unreleased for \['BIG_QUERY_RAW_DATA_DATASET'\] respectively",
        ):
            _ = self.us_xx_manager.acquire_lock_for_resources(
                self.all_resources,
                DirectIngestRawDataLockActor.ADHOC,
                "testing-testing-123",
            )

    def test_acquire_lock_existing_expired(self) -> None:
        old_time = datetime.datetime(2024, 1, 1, 1, 1, 1)
        with freeze_time(old_time):
            _ = self.us_xx_manager.acquire_lock_for_resources(
                self.raw_data,
                DirectIngestRawDataLockActor.PROCESS,
                "testing-testing-123",
                ttl_seconds=10 * 24 * 60 * 60,  # 10 days
            )

        active_lock = self.us_xx_manager.acquire_lock_for_resources(
            self.raw_data,
            DirectIngestRawDataLockActor.ADHOC,
            "testing-testing-123",
        )

        assert active_lock[0].released is False

    def test_lock_process_must_supply_ttl(self) -> None:
        with self.assertRaisesRegex(
            IntegrityError,
            r"\(.*CheckViolation\) new row for relation \"direct_ingest_raw_data_resource_lock\" "
            r"violates check constraint \"all_process_actors_must_specify_ttl_seconds\".*",
        ):
            _ = self.us_xx_manager.acquire_lock_for_resources(
                self.raw_data,
                DirectIngestRawDataLockActor.PROCESS,
                "testing-testing-123",
            )

    def test_release_lock(self) -> None:
        with self.assertRaisesRegex(LookupError, r"Could not find lock with id \[0\]"):
            _ = self.us_xx_manager.release_lock_by_id(0)

        active_locks = self.us_xx_manager.acquire_lock_for_resources(
            self.raw_data,
            DirectIngestRawDataLockActor.ADHOC,
            "testing-testing-123",
        )

        assert len(active_locks) == 1
        active_lock = active_locks[0]

        assert active_lock.released is False

        released_lock = self.us_xx_manager.release_lock_by_id(
            lock_id=active_lock.lock_id
        )

        assert released_lock.released is True

        with self.assertRaisesRegex(
            DirectIngestRawDataResourceLockAlreadyReleasedError,
            r"Lock \[\d+\] is already released so cannot release it",
        ):
            _ = self.us_xx_manager.release_lock_by_id(lock_id=released_lock.lock_id)

    def test_get_lock_by_id_reflects_update(self) -> None:
        old_time = datetime.datetime(2024, 1, 1, 1, 1, 1)
        with freeze_time(old_time):
            expired_locks_in_jan = self.us_xx_manager.acquire_lock_for_resources(
                self.raw_data,
                DirectIngestRawDataLockActor.PROCESS,
                "testing-testing-123",
                ttl_seconds=60 * 60,
            )

        assert len(expired_locks_in_jan) == 1
        expired_lock_in_jan = expired_locks_in_jan[0]
        assert expired_lock_in_jan.released is False

        current_lock = self.us_xx_manager.get_lock_by_id(expired_lock_in_jan.lock_id)

        assert current_lock.lock_id == expired_lock_in_jan.lock_id
        assert current_lock.released is True

    def test_get_most_recent_lock_for_resources_reflects_update(self) -> None:
        old_time = datetime.datetime(2024, 1, 1, 1, 1, 1)
        with freeze_time(old_time):
            expired_lock_in_jan = self.us_xx_manager.acquire_lock_for_resources(
                self.all_resources,
                DirectIngestRawDataLockActor.PROCESS,
                "testing-testing-123",
                ttl_seconds=60 * 60,
            )

        current_locks = self.us_xx_manager.get_most_recent_locks_for_resources(
            self.all_resources
        )

        expired_lock_ids = {lock.lock_id for lock in expired_lock_in_jan}
        assert len(current_locks) == 3
        for lock in current_locks:
            assert lock.released is True
            assert lock.lock_id in expired_lock_ids

    def test_get_current_lock_summary(self) -> None:
        current_locks = self.us_xx_manager.get_current_lock_summary()
        assert current_locks == {
            DirectIngestRawDataResourceLockResource.BIG_QUERY_RAW_DATA_DATASET: None,
            DirectIngestRawDataResourceLockResource.BUCKET: None,
            DirectIngestRawDataResourceLockResource.OPERATIONS_DATABASE: None,
        }

        old_time = datetime.datetime(2024, 1, 1, 1, 1, 1)
        with freeze_time(old_time):
            _ = self.us_xx_manager.acquire_lock_for_resources(
                self.all_resources,
                DirectIngestRawDataLockActor.PROCESS,
                "testing-testing-123",
                ttl_seconds=60 * 60,
            )

        current_locks = self.us_xx_manager.get_current_lock_summary()

        assert current_locks == {
            DirectIngestRawDataResourceLockResource.BIG_QUERY_RAW_DATA_DATASET: None,
            DirectIngestRawDataResourceLockResource.BUCKET: None,
            DirectIngestRawDataResourceLockResource.OPERATIONS_DATABASE: None,
        }

        held_locks = self.us_xx_manager.acquire_lock_for_resources(
            self.all_resources,
            DirectIngestRawDataLockActor.PROCESS,
            "testing-testing-123",
            ttl_seconds=60 * 100,
        )

        current_locks = self.us_xx_manager.get_current_lock_summary()

        assert current_locks == {
            DirectIngestRawDataResourceLockResource.BIG_QUERY_RAW_DATA_DATASET: DirectIngestRawDataLockActor.PROCESS,
            DirectIngestRawDataResourceLockResource.BUCKET: DirectIngestRawDataLockActor.PROCESS,
            DirectIngestRawDataResourceLockResource.OPERATIONS_DATABASE: DirectIngestRawDataLockActor.PROCESS,
        }

        released = one(
            filter(
                lambda x: x.lock_resource
                == DirectIngestRawDataResourceLockResource.BUCKET,
                held_locks,
            )
        )
        self.us_xx_manager.release_lock_by_id(released.lock_id)

        current_locks_two = self.us_xx_manager.get_current_lock_summary()

        assert current_locks_two == {
            DirectIngestRawDataResourceLockResource.BIG_QUERY_RAW_DATA_DATASET: DirectIngestRawDataLockActor.PROCESS,
            DirectIngestRawDataResourceLockResource.BUCKET: None,
            DirectIngestRawDataResourceLockResource.OPERATIONS_DATABASE: DirectIngestRawDataLockActor.PROCESS,
        }

    def test_lock_acquire_race_condition_on_commit(self) -> None:
        """Simulates a race condition, wherein two proceeses try to commit new locks.
        Worker 1 creates a new lock but before it commits its transaction, a
        second worker process swoop in to create a new lock and commits it. The
        expected behavior here is to have the second worker's acquisition complete and
        the first worker who, despite technically being first to make the initial query
        fail because they were second to committing their transaction
        """

        persisted_lock = []

        def worker_two(*_args: Any, **_kwargs: Any) -> Any:
            nonlocal persisted_lock
            persisted_lock = self.us_xx_manager.acquire_lock_for_resources(
                self.raw_data,
                DirectIngestRawDataLockActor.ADHOC,
                "technically i was here second... but gimme the lock pls ¯\\_(ツ)_/¯",
            )

        with after(
            f"{LOCK_MANAGER_PACKAGE_NAME}.DirectIngestRawDataResourceLockManager._register_new_locks",
            worker_two,
        ):
            with self.assertRaisesRegex(
                IntegrityError,
                r"\(.*UniqueViolation\) duplicate key value violates unique constraint "
                r'"at_most_one_active_lock_per_resource_region_and_instance"',
            ):
                _ = self.us_xx_manager.acquire_lock_for_resources(
                    self.raw_data,
                    DirectIngestRawDataLockActor.ADHOC,
                    "i actually was here first :/",
                )

        most_recent_lock = self.us_xx_manager.get_most_recent_locks_for_resources(
            self.raw_data
        )

        assert len(most_recent_lock) == 1
        assert persisted_lock
        assert most_recent_lock[0].lock_id == persisted_lock[0].lock_id

    def test_lock_acquire_race_condition_with_active_expired_after_update_commit(
        self,
    ) -> None:
        """simulates a race condition wherein two process create locks but at the same
        time but one commits its locks before the other. the expected behavior here is
        to have the second worker's acquisition complete and have the first worker
        who, despite technically being first to make the initial query, will fail
        because they were second to committing their transaction
        """

        old_time = datetime.datetime(2024, 1, 1, 1, 1, 1)
        with freeze_time(old_time):
            expired_lock_in_jan = self.us_xx_manager.acquire_lock_for_resources(
                self.raw_data,
                DirectIngestRawDataLockActor.PROCESS,
                "testing-testing-123",
                ttl_seconds=60 * 60,
            )

        persisted_lock = []

        def worker_two(*_args: Any, **_kwargs: Any) -> Any:
            nonlocal persisted_lock
            persisted_lock = self.us_xx_manager.acquire_lock_for_resources(
                self.raw_data,
                DirectIngestRawDataLockActor.ADHOC,
                "technically i was here second... but gimme the lock pls ¯\\_(ツ)_/¯",
            )

        with after(
            f"{LOCK_MANAGER_PACKAGE_NAME}.DirectIngestRawDataResourceLockManager._update_unreleased_but_expired_locks_for_resources",
            worker_two,
        ):
            with self.assertRaisesRegex(
                IntegrityError,
                r"\(.*UniqueViolation\) duplicate key value violates unique constraint "
                r'"at_most_one_active_lock_per_resource_region_and_instance"',
            ):
                _ = self.us_xx_manager.acquire_lock_for_resources(
                    self.raw_data,
                    DirectIngestRawDataLockActor.ADHOC,
                    "i actually was here first :/",
                )

        # make sure old lock got unset
        old_lock = self.us_xx_manager.get_lock_by_id(expired_lock_in_jan[0].lock_id)
        assert old_lock.released is True

        most_recent_lock = self.us_xx_manager.get_most_recent_locks_for_resources(
            self.raw_data
        )

        assert len(most_recent_lock) == 1
        assert persisted_lock
        assert most_recent_lock[0].lock_id == persisted_lock[0].lock_id

    def test_lock_acquire_race_condition_with_active_expired_before_update_commit(
        self,
    ) -> None:
        """simulates a "non-repeatable read" situation wherein worker 1 reads an
        unreleased but expired lock and attempts to update it. after reading, but before
        worker 1 can commit the update, worker 2 reads the same unreleased but expired
        lock and commits an update to it and adds a new lock. worker 1 then tries to
        write its own new lock and commit its update to the release the unreleased but
        expired lock, which fails due to a unique violation.
        """

        old_time = datetime.datetime(2024, 1, 1, 1, 1, 1)
        with freeze_time(old_time):
            expired_lock_in_jan = self.us_xx_manager.acquire_lock_for_resources(
                self.raw_data,
                DirectIngestRawDataLockActor.PROCESS,
                "testing-testing-123",
                ttl_seconds=60 * 60,
            )

        persisted_lock = []

        def worker_two(*_args: Any, **_kwargs: Any) -> Any:
            nonlocal persisted_lock
            persisted_lock = self.us_xx_manager.acquire_lock_for_resources(
                self.raw_data,
                DirectIngestRawDataLockActor.ADHOC,
                "technically i was here second... but gimme the lock pls ¯\\_(ツ)_/¯",
            )

        with after(
            f"{LOCK_MANAGER_PACKAGE_NAME}.DirectIngestRawDataResourceLockManager._get_unreleased_locks_for_resources",
            worker_two,
        ):
            with self.assertRaisesRegex(
                IntegrityError,
                r"\(.*UniqueViolation\) duplicate key value violates unique constraint "
                r'"at_most_one_active_lock_per_resource_region_and_instance"',
            ):
                _ = self.us_xx_manager.acquire_lock_for_resources(
                    self.raw_data,
                    DirectIngestRawDataLockActor.ADHOC,
                    "i actually was here first :/",
                )

        # make sure old lock got unset
        old_lock = self.us_xx_manager.get_lock_by_id(expired_lock_in_jan[0].lock_id)
        assert old_lock.released is True

        most_recent_lock = self.us_xx_manager.get_most_recent_locks_for_resources(
            self.raw_data
        )

        assert len(most_recent_lock) == 1
        assert persisted_lock
        assert most_recent_lock[0].lock_id == persisted_lock[0].lock_id
