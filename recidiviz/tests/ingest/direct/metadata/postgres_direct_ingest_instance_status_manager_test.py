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
"""Implements tests for the PostgresDirectIngestInstanceStatusManager."""
from typing import List, Optional
from unittest.case import TestCase

import pytest

from recidiviz.common.constants.operations.direct_ingest_instance_status import (
    DirectIngestStatus,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.metadata.direct_ingest_instance_status_manager import (
    HUMAN_INTERVENTION_STATUSES,
    INVALID_STATUSES,
    VALID_CURRENT_STATUS_TRANSITIONS,
    VALID_INITIAL_STATUSES,
)
from recidiviz.ingest.direct.metadata.postgres_direct_ingest_instance_status_manager import (
    PostgresDirectIngestInstanceStatusManager,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.tools.postgres import local_postgres_helpers


@pytest.mark.uses_db
class PostgresDirectIngestInstanceStatusManagerManagerTest(TestCase):
    """Implements tests for PostgresDirectIngestInstanceStatusManager."""

    # Stores the location of the postgres DB for this test run
    temp_db_dir: Optional[str]

    # Lists all valid ingest status enum values.
    all_status_enum_values = list(DirectIngestStatus)

    @classmethod
    def setUpClass(cls) -> None:
        cls.temp_db_dir = local_postgres_helpers.start_on_disk_postgresql_database()

    def setUp(self) -> None:
        self.operations_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.OPERATIONS)
        local_postgres_helpers.use_on_disk_postgresql_database(self.operations_key)

        self.us_xx_primary_manager = PostgresDirectIngestInstanceStatusManager(
            StateCode.US_XX.value,
            DirectIngestInstance.PRIMARY,
        )

        self.us_xx_secondary_manager = PostgresDirectIngestInstanceStatusManager(
            StateCode.US_XX.value,
            DirectIngestInstance.SECONDARY,
        )

    def tearDown(self) -> None:
        local_postgres_helpers.teardown_on_disk_postgresql_database(self.operations_key)

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
            - set(INVALID_STATUSES[DirectIngestInstance.PRIMARY])
            - set(VALID_CURRENT_STATUS_TRANSITIONS[DirectIngestInstance.PRIMARY].keys())
        )
        self.assertEqual(len(primary_differences), 0)

        secondary_differences = list(
            set(all_enum_values)
            - set(INVALID_STATUSES[DirectIngestInstance.SECONDARY])
            - set(
                VALID_CURRENT_STATUS_TRANSITIONS[DirectIngestInstance.SECONDARY].keys()
            )
        )
        self.assertEqual(len(secondary_differences), 0)

    def test_get_raw_data_source_instance_primary_standard_rerun_raw_data_source_instance(
        self,
    ) -> None:
        """Assert that regardless of whether a rerun has been kicked off in PRIMARY, the raw data source is always
        PRIMARY."""

        # Arrange
        self.us_xx_primary_manager.add_instance_status(
            DirectIngestStatus.STANDARD_RERUN_STARTED
        )

        # Act
        raw_data_source_instance = (
            self.us_xx_primary_manager.get_raw_data_source_instance()
        )

        # Assert
        self.assertEqual(raw_data_source_instance, DirectIngestInstance.PRIMARY)

        # Arrange
        self.us_xx_primary_manager.change_status_to(DirectIngestStatus.UP_TO_DATE)

        # Act
        raw_data_source_instance = (
            self.us_xx_primary_manager.get_raw_data_source_instance()
        )

        # Assert
        self.assertEqual(raw_data_source_instance, DirectIngestInstance.PRIMARY)

    def test_get_raw_data_source_instance_secondary_standard_rerun_raw_data_source_instance(
        self,
    ) -> None:
        """Assert that the raw data source for secondary rerun is set to PRIMARY when a STANDARD_RERUN_STARTED
        is kicked off in SECONDARY."""
        # Arrange
        self.us_xx_secondary_manager.add_instance_status(
            DirectIngestStatus.STANDARD_RERUN_STARTED
        )

        # Act
        raw_data_source_instance = (
            self.us_xx_secondary_manager.get_raw_data_source_instance()
        )

        # Assert
        self.assertEqual(raw_data_source_instance, DirectIngestInstance.PRIMARY)

    def test_get_raw_data_source_instance_secondary_rerun_with_raw_data_import_raw_data_source_instance(
        self,
    ) -> None:
        """Assert that the raw data source for secondary rerun is set to PRIMARY when a
        RERUN_WITH_RAW_DATA_IMPORT_STARTED is kicked off in SECONDARY."""

        # Set initial status for SECONDARY US_XX instance to RERUN_WITH_RAW_DATA_IMPORT_STARTED.
        self.us_xx_secondary_manager.add_instance_status(
            DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
        )

        raw_data_source_instance = (
            self.us_xx_secondary_manager.get_raw_data_source_instance()
        )
        self.assertEqual(raw_data_source_instance, DirectIngestInstance.SECONDARY)

    def test_change_status_to_invalid_transitions(self) -> None:
        """Ensure that all invalid transitions raise the correct error."""
        for (
            instance,
            valid_status_transitions,
        ) in VALID_CURRENT_STATUS_TRANSITIONS.items():
            us_yy_manager = PostgresDirectIngestInstanceStatusManager(
                StateCode.US_YY.value, instance
            )
            for new_status in VALID_CURRENT_STATUS_TRANSITIONS[instance].keys():
                invalid_previous_statuses = list(
                    set(self.all_status_enum_values)
                    - set(valid_status_transitions[new_status])
                )

                if (
                    new_status in invalid_previous_statuses
                    and new_status not in HUMAN_INTERVENTION_STATUSES[instance]
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
        for instance, invalid_statuses in INVALID_STATUSES.items():
            us_yy_manager = PostgresDirectIngestInstanceStatusManager(
                StateCode.US_YY.value, instance
            )

            for invalid_status in invalid_statuses:
                with self.assertRaisesRegex(
                    ValueError,
                    f"The status={invalid_status.value} is an invalid status to transition to in "
                    f"instance={instance.value}",
                ):
                    us_yy_manager.change_status_to(invalid_status)

    def test_validate_statuses_transition_to_themselves(self) -> None:
        for instance in DirectIngestInstance:
            us_yy_manager = PostgresDirectIngestInstanceStatusManager(
                StateCode.US_YY.value, instance
            )

            # Statuses that can transition to themselves
            valid_statuses = list(
                set(self.all_status_enum_values)
                - set(INVALID_STATUSES[instance])
                - set(HUMAN_INTERVENTION_STATUSES[instance])
            )

            for status in valid_statuses:
                us_yy_manager.validate_transition(instance, status, status)

    def test_change_status_to_invalid_initial_statuses(self) -> None:
        """Ensure that all invalid initial statuses raise the correct error."""
        for instance, valid_initial_statuses in VALID_INITIAL_STATUSES.items():
            us_yy_manager = PostgresDirectIngestInstanceStatusManager(
                StateCode.US_YY.value, instance
            )

            # Valid initial statuses validated after invalid statuses.
            invalid_initial_statuses = list(
                set(self.all_status_enum_values)
                - set(INVALID_STATUSES[instance])
                - set(valid_initial_statuses)
            )
            for invalid_initial_status in invalid_initial_statuses:
                with self.assertRaisesRegex(ValueError, "Invalid initial status."):

                    us_yy_manager.change_status_to(invalid_initial_status)

    def _run_test_for_status_transitions(
        self,
        manager: PostgresDirectIngestInstanceStatusManager,
        statuses: List[DirectIngestStatus],
        expected_raw_data_source: Optional[DirectIngestInstance] = None,
    ) -> None:
        for status in statuses:
            manager.change_status_to(new_status=status)
            self.assertEqual(status, manager.get_current_status())
            if expected_raw_data_source:
                # Only assert raw data source if not at terminating status.
                if (
                    # Terminating status in PRIMARY is UP_TO_DATE.
                    manager.ingest_instance == DirectIngestInstance.PRIMARY
                    and status != DirectIngestStatus.UP_TO_DATE
                ) or (
                    # Terminating status in SECONDARY is NO_RERUN_IN_PROGRESS.
                    manager.ingest_instance == DirectIngestInstance.SECONDARY
                    and status != DirectIngestStatus.NO_RERUN_IN_PROGRESS
                ):
                    self.assertEqual(
                        expected_raw_data_source, manager.get_raw_data_source_instance()
                    )

    def test_happy_path_primary_rerun_flow(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_primary_manager,
            [
                DirectIngestStatus.STANDARD_RERUN_STARTED,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                DirectIngestStatus.UP_TO_DATE,
            ],
            expected_raw_data_source=DirectIngestInstance.PRIMARY,
        )

    def test_happy_path_secondary_rerun_flow(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_secondary_manager,
            [
                DirectIngestStatus.STANDARD_RERUN_STARTED,
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
                DirectIngestStatus.FLASH_IN_PROGRESS,
                DirectIngestStatus.FLASH_COMPLETED,
                DirectIngestStatus.NO_RERUN_IN_PROGRESS,
            ],
            expected_raw_data_source=DirectIngestInstance.PRIMARY,
        )

    def test_happy_path_secondary_rerun_flow_raw_data_in_secondary(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_secondary_manager,
            [
                DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
                DirectIngestStatus.FLASH_IN_PROGRESS,
                DirectIngestStatus.FLASH_COMPLETED,
                DirectIngestStatus.NO_RERUN_IN_PROGRESS,
            ],
            expected_raw_data_source=DirectIngestInstance.SECONDARY,
        )

    def test_happy_path_secondary_rerun_flow_flash_canceled(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_secondary_manager,
            [
                DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
                DirectIngestStatus.FLASH_CANCELED,
                DirectIngestStatus.NO_RERUN_IN_PROGRESS,
            ],
            expected_raw_data_source=DirectIngestInstance.SECONDARY,
        )

    def test_primary_started_no_data(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_primary_manager,
            [
                DirectIngestStatus.STANDARD_RERUN_STARTED,
                # We will transition straight to UP_TO_DATE if there is no data to
                # process.
                DirectIngestStatus.UP_TO_DATE,
            ],
            expected_raw_data_source=DirectIngestInstance.PRIMARY,
        )

    def test_secondary_started_no_data_in_secondary(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_secondary_manager,
            [
                DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
                # This would happen if a rerun was started but we hadn't transferred
                # raw data to secondary yet.
                DirectIngestStatus.STALE_RAW_DATA,
            ],
            expected_raw_data_source=DirectIngestInstance.SECONDARY,
        )

    def test_primary_rerun_receiving_new_raw_data_after_rerun_finishes(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_primary_manager,
            [
                DirectIngestStatus.STANDARD_RERUN_STARTED,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                DirectIngestStatus.UP_TO_DATE,
                # Some raw data could come in that is not used in ingest views
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.UP_TO_DATE,
                # Now we get raw data that is used in ingest views
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                DirectIngestStatus.UP_TO_DATE,
            ],
            expected_raw_data_source=DirectIngestInstance.PRIMARY,
        )

    def test_secondary_rerun_receiving_new_raw_data_after_rerun_finishes(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_secondary_manager,
            [
                DirectIngestStatus.STANDARD_RERUN_STARTED,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
                # Some raw data could come in that is not used in ingest views
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
                # Now we get raw data that is used in ingest views
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
            ],
            expected_raw_data_source=DirectIngestInstance.PRIMARY,
        )

    def test_primary_new_raw_data_interrupts_processing(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_primary_manager,
            [
                DirectIngestStatus.STANDARD_RERUN_STARTED,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                # New raw data while doing ingest view materialization
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                # Return to ingest view materialization
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                # Start processing ingest view results
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                # New raw data while doing extract and merge that isn't used in ingest
                # views.
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                # Return to processing ingest view results
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                # New raw data while doing extract and merge that IS used in ingest
                # views
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                # Generate more views then continue
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                DirectIngestStatus.UP_TO_DATE,
            ],
            expected_raw_data_source=DirectIngestInstance.PRIMARY,
        )

    def test_secondary_new_raw_data_interrupts_processing(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_secondary_manager,
            [
                DirectIngestStatus.STANDARD_RERUN_STARTED,
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                # New raw data while doing ingest view materialization
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                # Return to ingest view materialization
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                # Start processing ingest view results
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                # New raw data while doing extract and merge that isn't used in ingest
                # views.
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                # Return to processing ingest view results
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                # New raw data while doing extract and merge that IS used in ingest
                # views
                DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
                # Generate more views then continue
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
            ],
            expected_raw_data_source=DirectIngestInstance.PRIMARY,
        )

    def test_happy_path_secondary_rerun_flow_start_new_rerun(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_secondary_manager,
            [
                DirectIngestStatus.STANDARD_RERUN_STARTED,
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
                DirectIngestStatus.FLASH_IN_PROGRESS,
                DirectIngestStatus.FLASH_COMPLETED,
                DirectIngestStatus.NO_RERUN_IN_PROGRESS,
                DirectIngestStatus.STANDARD_RERUN_STARTED,
            ],
            expected_raw_data_source=DirectIngestInstance.PRIMARY,
        )

    def test_initial_status_secondary_no_rerun_in_progress(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_secondary_manager,
            [
                DirectIngestStatus.NO_RERUN_IN_PROGRESS,
                DirectIngestStatus.STANDARD_RERUN_STARTED,
            ],
            expected_raw_data_source=DirectIngestInstance.PRIMARY,
        )

    def test_initial_status_primary_up_to_date(self) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_primary_manager,
            [
                DirectIngestStatus.UP_TO_DATE,
                DirectIngestStatus.STANDARD_RERUN_STARTED,
            ],
            expected_raw_data_source=DirectIngestInstance.PRIMARY,
        )

    def test_happy_path_secondary_rerun_flow_start_new_rerun_raw_data_secondary(
        self,
    ) -> None:
        self._run_test_for_status_transitions(
            self.us_xx_secondary_manager,
            [
                DirectIngestStatus.STANDARD_RERUN_STARTED,
                DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
                DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
                DirectIngestStatus.READY_TO_FLASH,
                DirectIngestStatus.FLASH_IN_PROGRESS,
                DirectIngestStatus.FLASH_COMPLETED,
                DirectIngestStatus.NO_RERUN_IN_PROGRESS,
            ],
            expected_raw_data_source=DirectIngestInstance.PRIMARY,
        )
        self._run_test_for_status_transitions(
            self.us_xx_secondary_manager,
            [
                DirectIngestStatus.RERUN_WITH_RAW_DATA_IMPORT_STARTED,
            ],
            expected_raw_data_source=DirectIngestInstance.SECONDARY,
        )

    def test_duplicate_statuses_are_not_added_twice(
        self,
    ) -> None:
        self.us_xx_secondary_manager.change_status_to(
            new_status=DirectIngestStatus.STANDARD_RERUN_STARTED
        )
        reused_status = DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS
        self.us_xx_secondary_manager.change_status_to(new_status=reused_status)
        self.us_xx_secondary_manager.change_status_to(new_status=reused_status)

        added_statuses = [
            status.status for status in self.us_xx_secondary_manager.get_all_statuses()
        ]
        expected_statuses = [
            DirectIngestStatus.STANDARD_RERUN_STARTED,
            reused_status,
        ]
        self.assertCountEqual(added_statuses, expected_statuses)

    def test_primary_flashed_from_secondary(self) -> None:
        primary_standard_flow_statuses = [
            DirectIngestStatus.STANDARD_RERUN_STARTED,
            DirectIngestStatus.RAW_DATA_IMPORT_IN_PROGRESS,
            DirectIngestStatus.INGEST_VIEW_MATERIALIZATION_IN_PROGRESS,
            DirectIngestStatus.EXTRACT_AND_MERGE_IN_PROGRESS,
            DirectIngestStatus.UP_TO_DATE,
        ]

        # Flashing could start when ingest is at any point in PRIMARY - it doesn't
        # matter because everything in PRIMARY will be overwritten.
        for i in range(1, len(primary_standard_flow_statuses)):
            us_xx_primary_manager = PostgresDirectIngestInstanceStatusManager(
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
                    DirectIngestStatus.UP_TO_DATE,
                ],
                expected_raw_data_source=DirectIngestInstance.PRIMARY,
            )
