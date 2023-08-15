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

    def test_validate_single_staff_entity(self) -> None:
        entities = [
            StateStaff(
                state_code="US_XX",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="EMP",
                    )
                ],
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )
        expected_entities = [
            StateStaff(
                state_code="US_XX",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="EMP",
                    )
                ],
            ),
            StateStaffExternalId(
                staff_external_id_id=22222,
                state_code="US_XX",
                external_id="12345",
                id_type="EMP",
            ),
        ]

        output = input_entities | RunValidations()
        assert_that(output, matches_all(expected_entities))
        self.test_pipeline.run()

    def test_validate_single_person_entity(self) -> None:
        entities = [
            StatePerson(
                state_code="US_XX",
                person_id=1234,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="PERSON",
                    )
                ],
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        expected_entities = [
            StatePerson(
                state_code="US_XX",
                person_id=1234,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="PERSON",
                    )
                ],
            ),
            StatePersonExternalId(
                person_external_id_id=11111,
                state_code="US_XX",
                external_id="12345",
                id_type="PERSON",
            ),
        ]
        output = input_entities | RunValidations()
        assert_that(output, matches_all(expected_entities))
        self.test_pipeline.run()

    def test_validate_mixed_root_entities(self) -> None:
        entities = [
            StatePerson(
                state_code="US_XX",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="PERSON",
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
                        id_type="EMP",
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
                        id_type="EMP",
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
                        id_type="PERSON",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="TEST",
                    ),
                ],
            ),
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        expected_entities = [
            StatePerson(
                state_code="US_XX",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="PERSON",
                    )
                ],
            ),
            StatePersonExternalId(
                person_external_id_id=11111,
                state_code="US_XX",
                external_id="12345",
                id_type="PERSON",
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="EMP",
                    )
                ],
            ),
            StateStaffExternalId(
                staff_external_id_id=22222,
                state_code="US_XX",
                external_id="12345",
                id_type="EMP",
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1235,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_XX",
                        external_id="2000",
                        id_type="EMP",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=22224,
                        state_code="US_XX",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            StateStaffExternalId(
                staff_external_id_id=22223,
                state_code="US_XX",
                external_id="2000",
                id_type="EMP",
            ),
            StateStaffExternalId(
                staff_external_id_id=22224,
                state_code="US_XX",
                external_id="3000",
                id_type="MOD",
            ),
            StatePerson(
                state_code="US_XX",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_XX",
                        external_id="4000",
                        id_type="PERSON",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="TEST",
                    ),
                ],
            ),
            StatePersonExternalId(
                person_external_id_id=11112,
                state_code="US_XX",
                external_id="4000",
                id_type="PERSON",
            ),
            StatePersonExternalId(
                person_external_id_id=11113,
                state_code="US_XX",
                external_id="5000",
                id_type="TEST",
            ),
        ]
        output = input_entities | RunValidations()
        assert_that(output, matches_all(expected_entities))
        self.test_pipeline.run()

    def test_missing_external_ids_staff_entity(self) -> None:
        expected_entities = [
            StateStaff(state_code="US_XX", staff_id=1234, external_ids=[])
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            expected_entities
        )

        _ = input_entities | RunValidations()

        with self.assertRaisesRegex(
            ValueError,
            r"^Found \[StateStaff\] with id \[1234\] missing an external_id:",
        ):
            self.test_pipeline.run()

    def test_missing_external_ids_person_entity(self) -> None:
        expected_entities = [
            StatePerson(state_code="US_XX", person_id=1234, external_ids=[])
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            expected_entities
        )

        _ = input_entities | RunValidations()

        with self.assertRaisesRegex(
            ValueError,
            r"^Found \[StatePerson\] with id \[1234\] missing an external_id:",
        ):
            self.test_pipeline.run()

    def test_validate_mixed_root_entities_dup_staff_id(self) -> None:
        expected_entities = [
            StatePerson(
                state_code="US_XX",
                person_id=1234,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="PERSON",
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
                        id_type="EMP",
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
                        id_type="EMP",
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
                        id_type="PERSON",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="TEST",
                    ),
                ],
            ),
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            expected_entities
        )

        _ = input_entities | RunValidations()

        with self.assertRaisesRegex(
            ValueError,
            r"More than one state_staff entity found with staff_id 1234",
        ):
            self.test_pipeline.run()

    def test_validate_mixed_root_entities_dup_person_id(self) -> None:
        expected_entities = [
            StatePerson(
                state_code="US_XX",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="PERSON",
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
                        id_type="EMP",
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
                        id_type="EMP",
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
                        id_type="PERSON",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="TEST",
                    ),
                ],
            ),
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            expected_entities
        )

        _ = input_entities | RunValidations()

        with self.assertRaisesRegex(
            ValueError,
            r"More than one state_person entity found with person_id 3000",
        ):
            self.test_pipeline.run()

    def test_validate_simple_child_entities(self) -> None:
        entities = [
            StatePerson(
                state_code="US_XX",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="PERSON",
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=2,
                        external_id="sp2",
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=200,
                        external_id="sp2",
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=300,
                        external_id="sp3",
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
                        id_type="EMP",
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
                        id_type="EMP",
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
                        id_type="PERSON",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="TEST",
                    ),
                ],
            ),
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        expected_entities = [
            StatePerson(
                state_code="US_XX",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="PERSON",
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=2,
                        external_id="sp2",
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=200,
                        external_id="sp2",
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=300,
                        external_id="sp3",
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
            StatePersonExternalId(
                person_external_id_id=11111,
                state_code="US_XX",
                external_id="12345",
                id_type="PERSON",
            ),
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_XX",
                supervision_period_id=2,
                external_id="sp2",
            ),
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_XX",
                supervision_period_id=200,
                external_id="sp2",
            ),
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_XX",
                supervision_period_id=300,
                external_id="sp3",
            ),
            StateSupervisionContact.new_with_defaults(
                state_code="US_XX",
                external_id="c1",
                contact_date=date(2018, 4, 1),
                supervision_contact_id=101,
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="EMP",
                    )
                ],
            ),
            StateStaffExternalId(
                staff_external_id_id=22222,
                state_code="US_XX",
                external_id="12345",
                id_type="EMP",
            ),
            StateStaff(
                state_code="US_XX",
                staff_id=1235,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=222223,
                        state_code="US_XX",
                        external_id="2000",
                        id_type="EMP",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_XX",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            StateStaffExternalId(
                staff_external_id_id=222223,
                state_code="US_XX",
                external_id="2000",
                id_type="EMP",
            ),
            StateStaffExternalId(
                staff_external_id_id=22223,
                state_code="US_XX",
                external_id="3000",
                id_type="MOD",
            ),
            StatePerson(
                state_code="US_XX",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_XX",
                        external_id="4000",
                        id_type="PERSON",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="TEST",
                    ),
                ],
            ),
            StatePersonExternalId(
                person_external_id_id=11112,
                state_code="US_XX",
                external_id="4000",
                id_type="PERSON",
            ),
            StatePersonExternalId(
                person_external_id_id=11113,
                state_code="US_XX",
                external_id="5000",
                id_type="TEST",
            ),
        ]
        output = input_entities | RunValidations()
        assert_that(output, matches_all(expected_entities))
        self.test_pipeline.run()

    def test_validate_duplicate_id_same_child_entities(self) -> None:
        expected_entities = [
            StatePerson(
                state_code="US_XX",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="PERSON",
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=2,
                        external_id="sp2",
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=200,
                        external_id="sp2",
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=300,
                        external_id="sp3",
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
                        id_type="EMP",
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
                        id_type="EMP",
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
                        id_type="PERSON",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=1112,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="TEST",
                    ),
                ],
            ),
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            expected_entities
        )

        _ = input_entities | RunValidations()

        with self.assertRaisesRegex(
            ValueError,
            r"More than one state_staff_role_period entity found with staff_role_period_id 1111",
        ):
            self.test_pipeline.run()

    def test_validate_duplicate_id_diff_child_entities(self) -> None:
        expected_entities = [
            StatePerson(
                state_code="US_XX",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="PERSON",
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=2,
                        external_id="sp2",
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=200,
                        external_id="sp2",
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=300,
                        external_id="sp3",
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
                        id_type="EMP",
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
                        id_type="EMP",
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
                        id_type="PERSON",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=111223,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="TEST",
                    ),
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=300,
                        external_id="sp3",
                    ),
                ],
            ),
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            expected_entities
        )

        _ = input_entities | RunValidations()

        with self.assertRaisesRegex(
            ValueError,
            r"More than one state_supervision_period entity found with supervision_period_id 300",
        ):
            self.test_pipeline.run()

    def test_validate_duplicate_id_multiple_child_entities(self) -> None:
        expected_entities = [
            StatePerson(
                state_code="US_XX",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="PERSON",
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=2,
                        external_id="sp2",
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=200,
                        external_id="sp2",
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=300,
                        external_id="sp3",
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
                        id_type="EMP",
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
                        id_type="EMP",
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
                        id_type="PERSON",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="TEST",
                    ),
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=311,
                        external_id="sp3",
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
                        id_type="PERSON",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11115,
                        state_code="US_XX",
                        external_id="5001",
                        id_type="TEST",
                    ),
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=311,
                        external_id="sp5",
                    ),
                ],
            ),
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            expected_entities
        )

        _ = input_entities | RunValidations()

        with self.assertRaisesRegex(
            ValueError,
            r"More than one state_supervision_period entity found with supervision_period_id 311",
        ):
            self.test_pipeline.run()

    def test_unique_constraint_state_person_external_id_simple(self) -> None:
        entities = [
            StatePerson(
                state_code="US_XX",
                person_id=1234,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="PERSON",
                    ),
                ],
            ),
            StatePerson(
                state_code="US_XX",
                person_id=1235,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="PERSON",
                    )
                ],
            ),
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations()

        with self.assertRaisesRegex(
            ValueError,
            r"More than one state_person_external_id entity found with state_code=US_XX, id_type=PERSON, external_id=12345, first entity found: \[person_external_id_id 11111\]",
        ):
            self.test_pipeline.run()

    def test_unique_constraint_state_supervision_contact(self) -> None:
        entities = [
            StatePerson(
                state_code="US_XX",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_XX",
                        external_id="12345",
                        id_type="PERSON",
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=2,
                        external_id="sp2",
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=200,
                        external_id="sp2",
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_XX",
                        supervision_period_id=300,
                        external_id="sp3",
                    ),
                ],
                supervision_contacts=[
                    StateSupervisionContact.new_with_defaults(
                        state_code="US_XX",
                        external_id="c1",
                        contact_date=date(2018, 4, 1),
                        supervision_contact_id=101,
                    ),
                    StateSupervisionContact.new_with_defaults(
                        state_code="US_XX",
                        external_id="c2",
                        contact_date=date(2018, 4, 1),
                        supervision_contact_id=102,
                    ),
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
                        id_type="EMP",
                    )
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
                        id_type="PERSON",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_XX",
                        external_id="5000",
                        id_type="TEST",
                    ),
                ],
                supervision_contacts=[
                    StateSupervisionContact.new_with_defaults(
                        state_code="US_YY",
                        external_id="c2",
                        contact_date=date(2020, 4, 1),
                        supervision_contact_id=104,
                    ),
                    StateSupervisionContact.new_with_defaults(
                        state_code="US_XX",
                        external_id="c2",
                        contact_date=date(2020, 4, 1),
                        supervision_contact_id=105,
                    ),
                ],
            ),
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations()

        with self.assertRaisesRegex(
            ValueError,
            r"More than one state_supervision_contact entity found with state_code=US_XX, external_id=c2, first entity found: \[supervision_contact_id 102\]",
        ):
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
                    id_type="PERSON",
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

        entities = [person]

        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations()

        with self.assertRaisesRegex(
            ValueError,
            r"More than one state_task_deadline entity found for root entity \[person_id 3111\] with state_code=US_XX, task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION, task_subtype=None, update_datetime=2023-02-01 11:19:00, first entity found: \[task_deadline_id 2\]",
        ):
            self.test_pipeline.run()
