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
"""Implements tests for the DirectIngestInstanceStatusManager."""
import datetime
from datetime import timedelta
from typing import List, Optional
from unittest.case import TestCase

import pytest
import pytz
from freezegun import freeze_time
from more_itertools import one
from parameterized import parameterized

from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    DirectIngestInstanceStatusManager,
    get_human_intervention_statuses,
    get_invalid_statuses,
    get_valid_current_status_transitions,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.operations.entities import DirectIngestInstanceStatus
from recidiviz.tools.postgres import local_persistence_helpers, local_postgres_helpers


# TODO(#28239) remove once raw data import dag is fully rolled out
@pytest.mark.uses_db
class DirectIngestInstanceStatusManagerTest(TestCase):
    """Implements tests for DirectIngestInstanceStatusManager."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    # Lists all valid ingest status enum values.
    all_status_enum_values = list(DirectIngestStatus)

    @staticmethod
    def _add_instance_statuses_in_hour_increments(
        start_timestamp: datetime.datetime,
        status_manager: DirectIngestInstanceStatusManager,
        statuses: List[DirectIngestStatus],
    ) -> None:
        for i, status in enumerate(statuses):
            with freeze_time(start_timestamp + timedelta(hours=i)):
                # Each status gets added with increasing timestamps
                status_manager.add_instance_status(status)

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.operations_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_persistence_helpers.use_on_disk_postgresql_database(self.operations_key)
        self.us_xx_primary_manager = DirectIngestInstanceStatusManager(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
        )
        self.us_xx_secondary_manager = DirectIngestInstanceStatusManager(
            StateCode.US_XX.value,
            DirectIngestInstance.SECONDARY,
        )
        # Set initial statuses for PRIMARY and SECONDARY.
        self.us_xx_primary_manager.add_initial_status()
        self.us_xx_secondary_manager.add_initial_status()

    def tearDown(self) -> None:
        local_persistence_helpers.teardown_on_disk_postgresql_database(
            self.operations_key
        )

    @classmethod
    def tearDownClass(cls) -> None:
        local_postgres_helpers.stop_and_clear_on_disk_postgresql_database(
            cls.temp_db_dir
        )

    def test_coverage_of_status_transition_validations(self) -> None:
        """Confirm that the status transition validations cover all possible statuses that are applicable and
        not applicable for a given instance."""
        all_enum_values = list(DirectIngestStatus)

        primary_differences = list(
            set(all_enum_values)
            - set(get_invalid_statuses(DirectIngestInstance.PRIMARY))
            - set(
                get_valid_current_status_transitions(
                    DirectIngestInstance.PRIMARY
                ).keys()
            )
        )
        self.assertEqual(len(primary_differences), 0)

        secondary_differences = list(
            set(all_enum_values)
            - set(get_invalid_statuses(DirectIngestInstance.SECONDARY))
            - set(
                get_valid_current_status_transitions(
                    DirectIngestInstance.SECONDARY
                ).keys()
            )
        )
        self.assertEqual(len(secondary_differences), 0)

    def test_change_status_to_invalid_transitions(self) -> None:
        """Ensure that all invalid transitions raise the correct error."""
        for instance in DirectIngestInstance:
            invalid_statuses_for_instance = get_invalid_statuses(instance)
            valid_status_transitions = get_valid_current_status_transitions(instance)
            us_yy_manager = DirectIngestInstanceStatusManager(
                StateCode.US_YY.value, instance
            )
            for new_status, valid_previous_statuses in valid_status_transitions.items():
                invalid_previous_statuses = list(
                    set(self.all_status_enum_values)
                    - set(valid_previous_statuses)
                    - set(invalid_statuses_for_instance)
                )

                if (
                    new_status in invalid_previous_statuses
                    and new_status not in get_human_intervention_statuses(instance)
                ):
                    # Remove from invalid previous statuses any status that is expected to be able to transition to
                    # itself
                    invalid_previous_statuses.remove(new_status)

                for invalid_previous_status in invalid_previous_statuses:
                    us_yy_manager.add_instance_status(invalid_previous_status)
                    with self.assertRaisesRegex(
                        ValueError, "Can only transition from the following"
                    ):
                        us_yy_manager.change_status_to(new_status)

    def test_change_status_to_invalid_instance_specific_statuses(self) -> None:
        """Ensure that all invalid statuses raise the correct error."""

        for instance in DirectIngestInstance:
            invalid_statuses = get_invalid_statuses(instance)
            us_yy_manager = DirectIngestInstanceStatusManager(
                StateCode.US_YY.value, instance
            )
            us_yy_manager.add_initial_status()

            for invalid_status in invalid_statuses:
                with self.assertRaisesRegex(
                    ValueError,
                    f"The status={invalid_status.value} is an invalid status to transition to in "
                    f"instance={instance.value}",
                ):
                    us_yy_manager.change_status_to(invalid_status)

    def test_validate_statuses_transition_to_themselves(self) -> None:
        for instance in DirectIngestInstance:
            us_yy_manager = DirectIngestInstanceStatusManager(
                StateCode.US_YY.value, instance
            )

            # Statuses that can transition to themselves
            valid_statuses = list(
                set(self.all_status_enum_values)
                - set(get_invalid_statuses(instance))
                - set(get_human_intervention_statuses(instance))
            )

            for status in valid_statuses:
                us_yy_manager.validate_transition(instance, status, status)

    @parameterized.expand(
        [
            (
                "UTC",
                datetime.datetime(2020, 1, 2, 3, 4, 5, 6, tzinfo=pytz.UTC),
            ),
            (
                "EST",
                datetime.datetime(
                    2020, 1, 2, 3, 4, 5, 6, tzinfo=pytz.timezone("America/New_York")
                ),
            ),
            (
                "PST",
                datetime.datetime(
                    2020, 1, 2, 3, 4, 5, 6, tzinfo=pytz.timezone("America/Los_Angeles")
                ),
            ),
        ]
    )
    def test_read_write_timestamps(
        self, _name: str, status_date: datetime.datetime
    ) -> None:
        # Use a new status manager for US_YY that doesn't have initial statuses added
        # in setUp.
        us_yy_primary_manager = DirectIngestInstanceStatusManager(
            StateCode.US_YY.value,
            DirectIngestInstance.PRIMARY,
        )
        with freeze_time(status_date):
            us_yy_primary_manager.add_initial_status()
        status = one(us_yy_primary_manager.get_all_statuses())
        self.assertEqual(status_date, status.status_timestamp)

    def test_duplicate_statuses_are_not_added_twice(
        self,
    ) -> None:
        us_yy_manager = DirectIngestInstanceStatusManager(
            StateCode.US_YY.value,
            DirectIngestInstance.PRIMARY,
        )
        us_yy_manager.add_instance_status(
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS
        )
        reused_status = DirectIngestStatus.FLASH_IN_PROGRESS
        us_yy_manager.change_status_to(new_status=reused_status)
        us_yy_manager.change_status_to(new_status=reused_status)

        added_statuses = [status.status for status in us_yy_manager.get_all_statuses()]
        expected_statuses = [
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
            reused_status,
        ]
        self.assertCountEqual(added_statuses, expected_statuses)

    def _run_test_for_status_transitions(
        self,
        manager: DirectIngestInstanceStatusManager,
        statuses: List[DirectIngestStatus],
    ) -> None:
        for status in statuses:
            manager.change_status_to(new_status=status)
            self.assertEqual(status, manager.get_current_status())

    def test_do_not_allow_empty_status(self) -> None:
        us_ww_manager = DirectIngestInstanceStatusManager(
            StateCode.US_WW.value,
            DirectIngestInstance.PRIMARY,
        )
        with self.assertRaisesRegex(
            ValueError, "Initial statuses for a state must be set via a migration."
        ):
            us_ww_manager.change_status_to(DirectIngestStatus.INITIAL_STATE)

    def test_happy_path_primary_raw_data_import_flow(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_primary_manager,
            [
                DirectIngestStatus.INITIAL_STATE,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.RAW_DATA_UP_TO_DATE,
            ],
        )

    def test_happy_path_secondary_raw_data_import_flow(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_secondary_manager,
            [
                DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
                DirectIngestStatus.RAW_DATA_REIMPORT_STARTED,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
                DirectIngestStatus.FLASH_IN_PROGRESS,
                DirectIngestStatus.FLASH_COMPLETED,
                DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
                # Test that we can start a reimport again after
                DirectIngestStatus.RAW_DATA_REIMPORT_STARTED,
            ],
        )

    def test_happy_path_secondary_rerun_flow_RAW_DATA_REIMPORT_CANCELED(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_secondary_manager,
            [
                DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
                DirectIngestStatus.RAW_DATA_REIMPORT_STARTED,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
                DirectIngestStatus.RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS,
                DirectIngestStatus.RAW_DATA_REIMPORT_CANCELED,
                DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
            ],
        )

    def test_primary_started_no_data(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_primary_manager,
            [
                DirectIngestStatus.INITIAL_STATE,
                # We will transition straight to RAW_DATA_UP_TO_DATE if there is no data
                # to process.
                DirectIngestStatus.RAW_DATA_UP_TO_DATE,
            ],
        )

    def test_initial_status_secondary_no_rerun_in_progress(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_secondary_manager,
            [
                DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
                DirectIngestStatus.RAW_DATA_REIMPORT_STARTED,
            ],
        )

    def test_secondary_started_no_data_in_secondary(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_secondary_manager,
            [
                DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
                DirectIngestStatus.RAW_DATA_REIMPORT_STARTED,
                # This would happen if a reimport was started but we hadn't transferred
                # raw data to secondary yet.
                DirectIngestStatus.STALE_RAW_DATA,
            ],
        )

    def test_primary_receiving_new_raw_data_after_up_to_date(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_primary_manager,
            [
                DirectIngestStatus.INITIAL_STATE,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.RAW_DATA_UP_TO_DATE,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.RAW_DATA_UP_TO_DATE,
            ],
        )

    def test_secondary_receiving_new_raw_data_after_reimport_finishes(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_secondary_manager,
            [
                DirectIngestStatus.RAW_DATA_REIMPORT_STARTED,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
                # Some raw data could come in PRIMARY, making SECONDARY stale
                DirectIngestStatus.STALE_RAW_DATA,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
                # More raw data is dropped in SECONDARY
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
            ],
        )

    def test_primary_flashed_from_secondary(self) -> None:
        primary_standard_flow_statuses = [
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
            DirectIngestStatus.RAW_DATA_UP_TO_DATE,
        ]

        # Flashing could start when ingest is at any point in PRIMARY - it doesn't
        # matter because everything in PRIMARY will be overwritten.
        for i in range(1, len(primary_standard_flow_statuses)):
            us_xx_primary_manager = DirectIngestInstanceStatusManager(
                StateCode.US_XX.value,
                DirectIngestInstance.PRIMARY,
            )
            statuses_before_flashing = primary_standard_flow_statuses[0:i]

            self._run_test_for_status_transitions(
                us_xx_primary_manager,
                [
                    *statuses_before_flashing,
                    DirectIngestStatus.FLASH_IN_PROGRESS,
                    DirectIngestStatus.FLASH_COMPLETED,
                    DirectIngestStatus.RAW_DATA_UP_TO_DATE,
                ],
            )

    def test_any_secondary_status_valid_to_reimport_cancellation_in_progress(
        self,
    ) -> None:
        valid_secondary_statuses = list(
            (set(DirectIngestStatus))
            - set(get_invalid_statuses(DirectIngestInstance.SECONDARY))
        )
        for status in valid_secondary_statuses:
            us_yy_secondary_manager = DirectIngestInstanceStatusManager(
                StateCode.US_YY.value,
                DirectIngestInstance.SECONDARY,
            )
            us_yy_secondary_manager.validate_transition(
                DirectIngestInstance.SECONDARY,
                status,
                DirectIngestStatus.RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS,
            )

    def test_get_current_ingest_rerun_start_timestamp_no_rerun_in_progress(
        self,
    ) -> None:
        """Confirm that there is no rerun start timestamp when no rerun is in progress."""
        start_timestamp = datetime.datetime(2022, 7, 1, 1, 2, 3, 0, pytz.UTC)
        statuses = [DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS]
        # Create a new us_yy manager that does not have any pre-seeded data.
        us_yy_secondary_manager = DirectIngestInstanceStatusManager(
            StateCode.US_YY.value,
            DirectIngestInstance.SECONDARY,
        )
        self._add_instance_statuses_in_hour_increments(
            start_timestamp, us_yy_secondary_manager, statuses
        )

        self.assertIsNone(
            us_yy_secondary_manager.get_current_ingest_rerun_start_timestamp()
        )

    def test_get_current_ingest_reimport_start_timestamp_many_secondary_starts_and_stops(
        self,
    ) -> None:
        """Confirm that there the correct timestamp is chosen when there are multiple ingest starts and stops in
        SECONDARY."""
        start_timestamp = datetime.datetime(2022, 7, 1, 1, 2, 3, 0, pytz.UTC)
        statuses = [
            DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
            DirectIngestStatus.RAW_DATA_REIMPORT_STARTED,
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
            DirectIngestStatus.RAW_DATA_REIMPORT_CANCELLATION_IN_PROGRESS,
            DirectIngestStatus.RAW_DATA_REIMPORT_CANCELED,
            DirectIngestStatus.NO_RAW_DATA_REIMPORT_IN_PROGRESS,
            # Start a second reimport after cancelling a flash -- this is 6 statuses after start timestamp
            DirectIngestStatus.RAW_DATA_REIMPORT_STARTED,
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
            DirectIngestStatus.READY_TO_FLASH,
        ]
        us_yy_secondary_manager = DirectIngestInstanceStatusManager(
            StateCode.US_YY.value,
            DirectIngestInstance.SECONDARY,
        )
        self._add_instance_statuses_in_hour_increments(
            start_timestamp, us_yy_secondary_manager, statuses
        )

        self.assertEqual(
            start_timestamp + timedelta(hours=6),
            us_yy_secondary_manager.get_current_ingest_rerun_start_timestamp(),
        )

    def test_get_flash_timestamps(
        self,
    ) -> None:
        start_timestamp = datetime.datetime(2022, 7, 1, 0, tzinfo=pytz.UTC)
        statuses = [
            DirectIngestStatus.INITIAL_STATE,
            DirectIngestStatus.RAW_DATA_UP_TO_DATE,
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
            DirectIngestStatus.RAW_DATA_UP_TO_DATE,
            DirectIngestStatus.FLASH_IN_PROGRESS,
            DirectIngestStatus.FLASH_COMPLETED,
            DirectIngestStatus.RAW_DATA_UP_TO_DATE,
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
            DirectIngestStatus.FLASH_IN_PROGRESS,
            DirectIngestStatus.FLASH_COMPLETED,
            DirectIngestStatus.RAW_DATA_UP_TO_DATE,
        ]
        us_yy_primary_manager = DirectIngestInstanceStatusManager(
            StateCode.US_YY.value,
            DirectIngestInstance.PRIMARY,
        )
        self._add_instance_statuses_in_hour_increments(
            start_timestamp, us_yy_primary_manager, statuses
        )

        self.assertEqual(
            [
                DirectIngestInstanceStatus(
                    region_code=StateCode.US_YY.value,
                    status_timestamp=datetime.datetime(2022, 7, 1, 10, tzinfo=pytz.UTC),
                    instance=DirectIngestInstance.PRIMARY,
                    status=DirectIngestStatus.RAW_DATA_UP_TO_DATE,
                ),
                DirectIngestInstanceStatus(
                    region_code=StateCode.US_YY.value,
                    status_timestamp=datetime.datetime(2022, 7, 1, 9, tzinfo=pytz.UTC),
                    instance=DirectIngestInstance.PRIMARY,
                    status=DirectIngestStatus.FLASH_COMPLETED,
                ),
                DirectIngestInstanceStatus(
                    region_code=StateCode.US_YY.value,
                    status_timestamp=datetime.datetime(2022, 7, 1, 8, tzinfo=pytz.UTC),
                    instance=DirectIngestInstance.PRIMARY,
                    status=DirectIngestStatus.FLASH_IN_PROGRESS,
                ),
            ],
            # Only get the last three statuses
            us_yy_primary_manager.get_statuses_since(
                datetime.datetime(2022, 7, 1, 7, tzinfo=pytz.UTC)
            ),
        )
