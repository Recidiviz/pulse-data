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
"""Tests for validating ingested root entities."""
from datetime import date, datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.pipeline_test import assert_that
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import matches_all

from recidiviz.common.constants.state.state_staff_role_period import StateStaffRoleType
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entity_utils import (
    CoreEntityFieldIndex,
    set_backedges,
)
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StatePersonExternalId,
    StateStaff,
    StateStaffExternalId,
    StateStaffRolePeriod,
    StateSupervisionContact,
    StateSupervisionPeriod,
    StateTaskDeadline,
)
from recidiviz.pipelines.ingest.state.run_validations import RunValidations
from recidiviz.tests.pipelines.ingest.state.test_case import StateIngestPipelineTestCase


class TestRunValidations(StateIngestPipelineTestCase):
    """Pipeline tests for the ValidateRootEntities PTransform"""

    def setUp(self) -> None:
        super().setUp()
        apache_beam_pipeline_options = PipelineOptions()
        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = False
        self.test_pipeline = TestPipeline(options=apache_beam_pipeline_options)
        self.state_code = StateCode.US_XX
        self.field_index = CoreEntityFieldIndex()

    def test_validate_single_staff_entity(self) -> None:
        entities = [
            set_backedges(
                StateStaff(
                    state_code="US_XX",
                    staff_id=1234,
                    external_ids=[
                        StateStaffExternalId(
                            staff_external_id_id=22222,
                            state_code="US_XX",
                            external_id="12345",
                            id_type="US_ZZ_TYPE",
                        )
                    ],
                ),
                field_index=self.field_index,
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )
        output = input_entities | RunValidations(
            expected_output_entities=["state_staff", "state_staff_external_id"],
            field_index=self.field_index,
            state_code=self.state_code,
        )
        assert_that(output, matches_all(entities))
        self.test_pipeline.run()

    def test_validate_single_person_entity(self) -> None:
        entities = [
            set_backedges(
                StatePerson(
                    state_code="US_XX",
                    person_id=1234,
                    external_ids=[
                        StatePersonExternalId(
                            person_external_id_id=11111,
                            state_code="US_XX",
                            external_id="12345",
                            id_type="US_XX_TYPE",
                        )
                    ],
                ),
                field_index=self.field_index,
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        output = input_entities | RunValidations(
            expected_output_entities=["state_person", "state_person_external_id"],
            field_index=self.field_index,
            state_code=self.state_code,
        )
        assert_that(output, matches_all(entities))
        self.test_pipeline.run()

    def test_validate_mixed_root_entities(self) -> None:
        entities_without_backedges = [
            StatePerson(
                state_code="US_XX",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="US_XX_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1237,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_XX",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=22224,
                        state_code="US_XX",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            StatePerson(
                state_code="US_XX",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_XX",
                        external_id="4000",
                        id_type="US_XX_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                    ),
                ],
            ),
        ]
        entities = [
            set_backedges(e, field_index=self.field_index)
            for e in entities_without_backedges
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )
        output = input_entities | RunValidations(
            expected_output_entities=[
                "state_person",
                "state_person_external_id",
                "state_staff",
                "state_staff_external_id",
            ],
            field_index=self.field_index,
            state_code=self.state_code,
        )
        assert_that(output, matches_all(entities))
        self.test_pipeline.run()

    def test_validate_task_deadlines(self) -> None:
        """Exact sane task deadline on two different people should not violate
        constraints.
        """
        update_datetime = datetime(2023, 2, 1, 11, 19)
        eligible_date = date(2020, 9, 11)
        entities_without_backedges = [
            StatePerson(
                state_code="US_XX",
                person_id=1234,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="US_XX_TYPE",
                    )
                ],
                task_deadlines=[
                    StateTaskDeadline(
                        task_deadline_id=1,
                        state_code="US_XX",
                        task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                        eligible_date=eligible_date,
                        update_datetime=update_datetime,
                        task_metadata=None,
                    )
                ],
            ),
            StatePerson(
                state_code="US_XX",
                person_id=4567,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=22222,
                        state_code="US_XX",
                        external_id="45678",
                        id_type="US_XX_TYPE",
                    )
                ],
                task_deadlines=[
                    StateTaskDeadline(
                        task_deadline_id=2,
                        state_code="US_XX",
                        task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                        eligible_date=eligible_date,
                        update_datetime=update_datetime,
                        task_metadata=None,
                    )
                ],
            ),
        ]
        entities = [
            set_backedges(e, self.field_index) for e in entities_without_backedges
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        output = input_entities | RunValidations(
            expected_output_entities=[
                "state_person",
                "state_person_external_id",
                "state_task_deadline",
            ],
            field_index=self.field_index,
            state_code=self.state_code,
        )
        assert_that(output, matches_all(entities))
        self.test_pipeline.run()

    def test_missing_external_ids_staff_entity(self) -> None:
        entities = [
            set_backedges(
                StateStaff(state_code="US_XX", staff_id=1234, external_ids=[]),
                field_index=self.field_index,
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entities=["state_staff"],
            field_index=self.field_index,
            state_code=self.state_code,
        )

        with self.assertRaisesRegex(
            ValueError,
            r".*Found \[StateStaff\] with id \[1234\] missing an external_id:.*",
        ):
            self.test_pipeline.run()

    def test_missing_external_ids_person_entity(self) -> None:
        entities = [
            set_backedges(
                StatePerson(state_code="US_XX", person_id=1234, external_ids=[]),
                field_index=self.field_index,
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entities=["state_person"],
            field_index=self.field_index,
            state_code=self.state_code,
        )

        with self.assertRaisesRegex(
            ValueError,
            r".*Found \[StatePerson\] with id \[1234\] missing an external_id:.*",
        ):
            self.test_pipeline.run()

    def test_validate_mixed_root_entities_dup_staff_id(self) -> None:
        entities_without_backedges = [
            StatePerson(
                state_code="US_XX",
                person_id=1234,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="US_XX_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_XX",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=22224,
                        state_code="US_XX",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            StatePerson(
                state_code="US_XX",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_XX",
                        external_id="4000",
                        id_type="US_XX_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                    ),
                ],
            ),
        ]
        entities = [
            set_backedges(e, field_index=self.field_index)
            for e in entities_without_backedges
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entities=[
                "state_person",
                "state_person_external_id",
                "state_staff",
                "state_staff_external_id",
            ],
            field_index=self.field_index,
            state_code=self.state_code,
        )

        expected_error_message = (
            r"Found errors for root entity StateStaff\(staff_id=1234, "
            r"external_ids=\[StateStaffExternalId\(external_id='12345', "
            r"id_type='US_ZZ_TYPE', staff_external_id_id=22222\)\]\):\n"
            r"  \* More than one state_staff entity found with "
            r"\(staff_id=1234\).*"
        )

        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_validate_mixed_root_entities_dup_person_id(self) -> None:
        entities_without_backedges = [
            StatePerson(
                state_code="US_XX",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="US_XX_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1235,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_XX",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=22224,
                        state_code="US_XX",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            StatePerson(
                state_code="US_XX",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_XX",
                        external_id="4000",
                        id_type="US_XX_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                    ),
                ],
            ),
        ]
        entities = [
            set_backedges(e, field_index=self.field_index)
            for e in entities_without_backedges
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entities=[
                "state_person",
                "state_person_external_id",
                "state_staff",
                "state_staff_external_id",
            ],
            field_index=self.field_index,
            state_code=self.state_code,
        )

        expected_error_message = (
            r"Found errors for root entity StatePerson\(person_id=3000, "
            r"external_ids=\[StatePersonExternalId\(external_id='12345', "
            r"id_type='US_XX_TYPE', person_external_id_id=11111\)\]\):\n"
            r"  \* More than one state_person entity found with "
            r"\(person_id=3000\).*"
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_validate_simple_child_entities(self) -> None:
        entities_without_backedges = [
            StatePerson(
                state_code="US_XX",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="US_XX_TYPE",
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=2,
                        external_id="sp2",
                        start_date=date(2020, 1, 1),
                        termination_date=date(2020, 2, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=200,
                        external_id="sp2",
                        start_date=date(2020, 2, 1),
                        termination_date=date(2020, 3, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=300,
                        external_id="sp3",
                        start_date=date(2020, 3, 1),
                    ),
                ],
                supervision_contacts=[
                    StateSupervisionContact.new_with_defaults(
                        state_code="US_XX",
                        external_id="c1",
                        contact_date=date(2018, 4, 1),
                        supervision_contact_id=101,
                    )
                ],
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1235,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=222223,
                        state_code="US_XX",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_XX",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            StatePerson(
                state_code="US_XX",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_XX",
                        external_id="4000",
                        id_type="US_XX_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                    ),
                ],
            ),
        ]
        entities = [
            set_backedges(e, field_index=self.field_index)
            for e in entities_without_backedges
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )
        output = input_entities | RunValidations(
            expected_output_entities=[
                "state_person",
                "state_person_external_id",
                "state_supervision_period",
                "state_supervision_contact",
                "state_staff",
                "state_staff_external_id",
            ],
            field_index=self.field_index,
            state_code=self.state_code,
        )
        assert_that(output, matches_all(entities))
        self.test_pipeline.run()

    def test_validate_duplicate_id_same_child_entities(self) -> None:
        entities_without_backedges = [
            StatePerson(
                state_code="US_XX",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="US_XX_TYPE",
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=2,
                        external_id="sp2",
                        start_date=date(2020, 1, 1),
                        termination_date=date(2020, 2, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=200,
                        external_id="sp2",
                        start_date=date(2020, 2, 1),
                        termination_date=date(2020, 3, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=300,
                        external_id="sp3",
                        start_date=date(2020, 3, 1),
                    ),
                ],
                supervision_contacts=[
                    StateSupervisionContact.new_with_defaults(
                        state_code="US_XX",
                        external_id="c1",
                        contact_date=date(2018, 4, 1),
                        supervision_contact_id=101,
                    )
                ],
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=2224,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1235,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=2222,
                        state_code="US_XX",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=2223,
                        state_code="US_XX",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
                role_periods=[
                    StateStaffRolePeriod(
                        staff_role_period_id=1111,
                        state_code="US_XX",
                        external_id="5000",
                        role_type=StateStaffRoleType.SUPERVISION_OFFICER,
                        start_date=date(year=2020, day=1, month=2),
                    ),
                    StateStaffRolePeriod(
                        staff_role_period_id=1111,
                        state_code="US_XX",
                        external_id="4000",
                        role_type=StateStaffRoleType.SUPERVISION_OFFICER,
                        start_date=date(year=2018, day=1, month=2),
                    ),
                ],
            ),
            StatePerson(
                state_code="US_XX",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=1111,
                        state_code="US_XX",
                        external_id="4000",
                        id_type="US_XX_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=1112,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                    ),
                ],
            ),
        ]
        entities = [
            set_backedges(e, field_index=self.field_index)
            for e in entities_without_backedges
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entities=[
                "state_person",
                "state_person_external_id",
                "state_supervision_contact",
                "state_supervision_period",
                "state_staff",
                "state_staff_external_id",
                "state_staff_role_period",
            ],
            field_index=self.field_index,
            state_code=self.state_code,
        )

        expected_error_message = (
            r"Found errors for root entity StateStaff\(staff_id=1235, "
            r"external_ids=\[StateStaffExternalId\(external_id='2000', "
            r"id_type='US_ZZ_TYPE', staff_external_id_id=2222\),"
            r"StateStaffExternalId\(external_id='3000', id_type='MOD', staff_external_id_id=2223\)\]\):\n"
            r"  \* More than one state_staff_role_period entity found with "
            r"\(staff_role_period_id=1111\).*"
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_validate_duplicate_id_diff_child_entities(self) -> None:
        entities_without_backedges = [
            StatePerson(
                state_code="US_XX",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="US_XX_TYPE",
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=2,
                        external_id="sp2",
                        start_date=date(2020, 1, 1),
                        termination_date=date(2020, 2, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=200,
                        external_id="sp2",
                        start_date=date(2020, 2, 1),
                        termination_date=date(2020, 3, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=300,
                        external_id="sp3",
                        start_date=date(2020, 3, 1),
                    ),
                ],
                supervision_contacts=[
                    StateSupervisionContact.new_with_defaults(
                        state_code="US_XX",
                        external_id="c1",
                        contact_date=date(2018, 4, 1),
                        supervision_contact_id=101,
                    )
                ],
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=11112,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1235,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=2222,
                        state_code="US_XX",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=2223,
                        state_code="US_XX",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            StatePerson(
                state_code="US_XX",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=111222,
                        state_code="US_XX",
                        external_id="4000",
                        id_type="US_XX_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=111223,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                    ),
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=300,
                        external_id="sp3",
                        start_date=date(2020, 4, 4),
                    ),
                ],
            ),
        ]
        entities = [
            set_backedges(e, field_index=self.field_index)
            for e in entities_without_backedges
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entities=[
                "state_person",
                "state_person_external_id",
                "state_supervision_contact",
                "state_supervision_period",
                "state_staff",
                "state_staff_external_id",
            ],
            field_index=self.field_index,
            state_code=self.state_code,
        )

        expected_error_message = (
            r"Found errors for root entity StatePerson\(person_id=1237, "
            r"external_ids=\[StatePersonExternalId\(external_id='12345', "
            r"id_type='US_XX_TYPE', person_external_id_id=11111\)\]\):\n"
            r"  \* More than one state_supervision_period entity found with "
            r"\(supervision_period_id=300\).*"
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_validate_duplicate_id_multiple_child_entities(self) -> None:
        entities_without_backedges = [
            StatePerson(
                state_code="US_XX",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="US_XX_TYPE",
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=2,
                        external_id="sp2",
                        start_date=date(2020, 1, 1),
                        termination_date=date(2020, 2, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=200,
                        external_id="sp2",
                        start_date=date(2020, 2, 1),
                        termination_date=date(2020, 3, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=300,
                        external_id="sp3",
                        start_date=date(2020, 3, 1),
                    ),
                ],
                supervision_contacts=[
                    StateSupervisionContact.new_with_defaults(
                        state_code="US_XX",
                        external_id="c1",
                        contact_date=date(2018, 4, 1),
                        supervision_contact_id=101,
                    )
                ],
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22224,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1235,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_XX",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_XX",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            StatePerson(
                state_code="US_XX",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_XX",
                        external_id="4000",
                        id_type="US_XX_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                    ),
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=311,
                        external_id="sp3",
                        start_date=date(2020, 4, 4),
                    ),
                ],
            ),
            StatePerson(
                state_code="US_XX",
                person_id=3111,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11114,
                        state_code="US_XX",
                        external_id="4001",
                        id_type="US_XX_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11115,
                        state_code="US_XX",
                        external_id="5001",
                        id_type="US_YY_TYPE",
                    ),
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=311,
                        external_id="sp5",
                        start_date=date(2020, 4, 4),
                    ),
                ],
            ),
        ]
        entities = [
            set_backedges(e, field_index=self.field_index)
            for e in entities_without_backedges
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entities=[
                "state_person",
                "state_person_external_id",
                "state_supervision_contact",
                "state_supervision_period",
                "state_staff",
                "state_staff_external_id",
            ],
            field_index=self.field_index,
            state_code=self.state_code,
        )

        expected_error_message = (
            r"Found errors for root entity StatePerson\(person_id=3000, "
            r"external_ids=\[StatePersonExternalId\(external_id='4000', "
            r"id_type='US_XX_TYPE', person_external_id_id=11112\),"
            r"StatePersonExternalId\(external_id='5000', "
            r"id_type='US_YY_TYPE', person_external_id_id=11113\)\]\):\n"
            r"  \* More than one state_supervision_period entity found with "
            r"\(supervision_period_id=311\).*"
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_unique_constraint_state_person_external_id_simple(self) -> None:
        person1 = StatePerson(
            state_code="US_XX",
            person_id=1234,
        )
        person1.external_ids.append(
            StatePersonExternalId(
                person_external_id_id=11111,
                state_code="US_XX",
                external_id="12345",
                id_type="US_XX_TYPE",
                person=person1,
            ),
        )

        person2 = StatePerson(
            state_code="US_XX",
            person_id=1235,
        )

        person2.external_ids.append(
            StatePersonExternalId(
                person_external_id_id=11112,
                state_code="US_XX",
                external_id="12345",
                id_type="US_XX_TYPE",
                person=person2,
            )
        )
        entities = [
            set_backedges(e, field_index=self.field_index) for e in [person1, person2]
        ]

        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entities=["state_person", "state_person_external_id"],
            field_index=self.field_index,
            state_code=self.state_code,
        )

        expected_error_message = (
            r"Found errors for root entity StatePerson\(person_id=1234, "
            r"external_ids=\[StatePersonExternalId\(external_id='12345', "
            r"id_type='US_XX_TYPE', person_external_id_id=11111\)\]\):\n"
            r"  \* More than one state_person_external_id entity found with "
            r"\(state_code=US_XX, id_type=US_XX_TYPE, external_id=12345\).*"
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_unique_constraint_state_supervision_contact(self) -> None:
        person1 = StatePerson(
            state_code="US_XX",
            person_id=1237,
        )
        person1.external_ids.append(
            StatePersonExternalId(
                person_external_id_id=11111,
                state_code="US_XX",
                external_id="12345",
                id_type="US_XX_TYPE",
                person=person1,
            )
        )
        person1.supervision_periods.append(
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_XX",
                supervision_period_id=2,
                external_id="sp2",
                start_date=date(2020, 1, 1),
                termination_date=date(2020, 2, 1),
                person=person1,
            )
        )
        person1.supervision_periods.append(
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_XX",
                supervision_period_id=200,
                external_id="sp2",
                start_date=date(2020, 2, 1),
                termination_date=date(2020, 3, 1),
                person=person1,
            ),
        )
        person1.supervision_periods.append(
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_XX",
                supervision_period_id=300,
                external_id="sp3",
                start_date=date(2020, 3, 1),
                person=person1,
            ),
        )
        person1.supervision_contacts.append(
            StateSupervisionContact.new_with_defaults(
                state_code="US_XX",
                external_id="c1",
                contact_date=date(2018, 4, 1),
                supervision_contact_id=101,
                person=person1,
            )
        )
        person1.supervision_contacts.append(
            StateSupervisionContact.new_with_defaults(
                state_code="US_XX",
                external_id="c2",
                contact_date=date(2018, 4, 1),
                supervision_contact_id=102,
                person=person1,
            )
        )
        staff1 = StateStaff(
            state_code="US_XX",
            staff_id=1234,
        )
        staff1.external_ids.append(
            StateStaffExternalId(
                staff_external_id_id=22222,
                state_code="US_XX",
                external_id="12345",
                id_type="US_ZZ_TYPE",
                staff=staff1,
            )
        )

        person2 = StatePerson(
            state_code="US_XX",
            person_id=3000,
        )

        person2.external_ids.append(
            StatePersonExternalId(
                person_external_id_id=11112,
                state_code="US_XX",
                external_id="4000",
                id_type="US_XX_TYPE",
                person=person2,
            )
        )

        person2.external_ids.append(
            StatePersonExternalId(
                person_external_id_id=11113,
                state_code="US_XX",
                external_id="5000",
                id_type="US_YY_TYPE",
                person=person2,
            )
        )

        person2.supervision_contacts.append(
            StateSupervisionContact.new_with_defaults(
                state_code="US_YY",
                external_id="c2",
                contact_date=date(2020, 4, 1),
                supervision_contact_id=104,
                person=person2,
            ),
        )

        person2.supervision_contacts.append(
            StateSupervisionContact.new_with_defaults(
                state_code="US_XX",
                external_id="c2",
                contact_date=date(2020, 4, 1),
                supervision_contact_id=105,
                person=person2,
            ),
        )
        entities = [
            set_backedges(e, field_index=self.field_index)
            for e in [person1, staff1, person2]
        ]

        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entities=[
                "state_person",
                "state_person_external_id",
                "state_supervision_contact",
                "state_supervision_period",
                "state_staff",
                "state_staff_external_id",
            ],
            field_index=self.field_index,
            state_code=self.state_code,
        )

        expected_error_message = (
            r"Found errors for root entity StatePerson\(person_id=1237, "
            r"external_ids=\[StatePersonExternalId\(external_id='12345', "
            r"id_type='US_XX_TYPE', person_external_id_id=11111\)\]\):\n"
            r"  \* More than one state_supervision_contact entity found with "
            r"\(state_code=US_XX, external_id=c2\).*"
        )

        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_entity_tree_unique_constraints_simple_invalid(self) -> None:
        person = StatePerson(
            state_code="US_XX",
            person_id=3111,
            external_ids=[
                StatePersonExternalId(
                    person_external_id_id=11114,
                    state_code="US_XX",
                    external_id="4001",
                    id_type="US_XX_TYPE",
                ),
            ],
        )

        person.task_deadlines.append(
            StateTaskDeadline(
                task_deadline_id=1,
                state_code="US_XX",
                task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )

        person.task_deadlines.append(
            StateTaskDeadline(
                task_deadline_id=2,
                state_code="US_XX",
                task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )
        person.task_deadlines.append(
            StateTaskDeadline(
                task_deadline_id=3,
                state_code="US_XX",
                task_type=StateTaskType.INTERNAL_UNKNOWN,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )

        entities = [set_backedges(e, field_index=self.field_index) for e in [person]]

        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entities=[
                "state_person",
                "state_person_external_id",
                "state_task_deadline",
            ],
            field_index=self.field_index,
            state_code=self.state_code,
        )
        expected_error_message = (
            r"Found errors for root entity StatePerson\(person_id=3111, "
            r"external_ids=\[StatePersonExternalId\(external_id='4001', "
            r"id_type='US_XX_TYPE', person_external_id_id=11114\)\]\):\n"
            r"  \* Found \[2\] state_task_deadline entities with \(state_code=US_XX, "
            r"task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION, task_subtype=None, "
            r"task_metadata=\{\"external_id\": \"00000001-111123-371006\", \"sentence_type\": \"INCARCERATION\"\}, "
            r"update_datetime=2023-02-01 11:19:00\).*"
        )

        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_validate_non_zero_entities(self) -> None:
        entities = [
            set_backedges(
                StateStaff(
                    state_code="US_XX",
                    staff_id=1234,
                    external_ids=[
                        StateStaffExternalId(
                            staff_external_id_id=22222,
                            state_code="US_XX",
                            external_id="12345",
                            id_type="US_ZZ_TYPE",
                        )
                    ],
                ),
                field_index=self.field_index,
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entities=[
                "state_staff",
                "state_staff_external_id",
                "state_staff_role_period",
            ],
            field_index=self.field_index,
            state_code=self.state_code,
        )
        with self.assertRaisesRegex(
            ValueError,
            r".*Expected non-zero state_staff_role_period entities to be ingested, but none were ingested.",
        ):
            self.test_pipeline.run()
