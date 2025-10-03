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
"""Testing the CreatePersonIdToStaffIdMapping PTransform."""
import datetime

import apache_beam as beam
from apache_beam.pipeline_test import assert_that, equal_to

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StatePerson,
    StatePersonExternalId,
    StateStaff,
    StateStaffExternalId,
    StateStaffSupervisorPeriod,
    StateSupervisionPeriod,
)
from recidiviz.pipelines.ingest.state.create_root_entity_id_to_staff_id_mapping import (
    CreateRootEntityIdToStaffIdMapping,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline

STATE_STAFF_1 = StateStaff(
    staff_id=123,
    state_code=StateCode.US_XX.value,
    external_ids=[
        StateStaffExternalId(
            staff_external_id_id=1234,
            state_code=StateCode.US_XX.value,
            id_type="US_XX_STAFF_ID_TYPE",
            external_id="A123",
        )
    ],
    supervisor_periods=[
        StateStaffSupervisorPeriod(
            state_code=StateCode.US_XX.value,
            staff_supervisor_period_id=100,
            external_id="XXXX",
            start_date=datetime.date(2020, 1, 1),
            end_date=datetime.date(2021, 1, 1),
            supervisor_staff_external_id="A456",
            supervisor_staff_external_id_type="US_XX_STAFF_ID_TYPE",
        )
    ],
)
STATE_STAFF_2 = StateStaff(
    staff_id=456,
    state_code=StateCode.US_XX.value,
    external_ids=[
        StateStaffExternalId(
            staff_external_id_id=4567,
            state_code=StateCode.US_XX.value,
            id_type="US_XX_STAFF_ID_TYPE",
            external_id="A456",
        ),
        StateStaffExternalId(
            staff_external_id_id=4568,
            state_code=StateCode.US_XX.value,
            id_type="US_XX_STAFF_ID_TYPE_2",
            external_id="B456",
        ),
    ],
)

STATE_STAFF_1_CONFLICTING = StateStaff(
    staff_id=789,
    state_code=StateCode.US_XX.value,
    external_ids=[
        StateStaffExternalId(
            staff_external_id_id=1234,
            state_code=StateCode.US_XX.value,
            id_type="US_XX_STAFF_ID_TYPE",
            external_id="A123",
        )
    ],
)

STATE_PERSON_1 = StatePerson(
    person_id=789,
    state_code=StateCode.US_XX.value,
    external_ids=[
        StatePersonExternalId(
            person_external_id_id=7890,
            state_code=StateCode.US_XX.value,
            id_type="US_XX_ID_TYPE",
            external_id="A789",
        ),
    ],
    supervision_periods=[
        # Period with no staff mapping
        StateSupervisionPeriod(
            supervision_period_id=78910,
            external_id="SP_1",
            state_code=StateCode.US_XX.value,
            start_date=datetime.date(2020, 1, 1),
            termination_date=datetime.date(2020, 2, 1),
            supervising_officer_staff_external_id_type="US_XX_STAFF_ID_TYPE",
            supervising_officer_staff_external_id="A123",
        ),
        # Period with no staff mapping
        StateSupervisionPeriod(
            supervision_period_id=78911,
            external_id="SP_2",
            state_code=StateCode.US_XX.value,
            start_date=datetime.date(2020, 2, 2),
        ),
    ],
)


STATE_PERSON_2 = StatePerson(
    person_id=8910,
    state_code=StateCode.US_XX.value,
    external_ids=[
        StatePersonExternalId(
            person_external_id_id=89101,
            state_code=StateCode.US_XX.value,
            id_type="US_XX_ID_TYPE",
            external_id="A789",
        ),
    ],
    supervision_periods=[
        # Period with no staff mapping
        StateSupervisionPeriod(
            supervision_period_id=89102,
            external_id="SP_1",
            state_code=StateCode.US_XX.value,
            start_date=datetime.date(2020, 1, 1),
            termination_date=datetime.date(2020, 2, 1),
            supervising_officer_staff_external_id_type="US_XX_STAFF_ID_TYPE",
            supervising_officer_staff_external_id="A123",
        ),
        # Period with no staff mapping
        StateSupervisionPeriod(
            supervision_period_id=89103,
            external_id="SP_2",
            state_code=StateCode.US_XX.value,
            start_date=datetime.date(2020, 2, 2),
            supervising_officer_staff_external_id_type="US_XX_STAFF_ID_TYPE",
            supervising_officer_staff_external_id="A456",
        ),
    ],
    assessments=[
        StateAssessment(
            assessment_id=1111,
            external_id="A_1",
            state_code=StateCode.US_XX.value,
            assessment_date=datetime.date(2020, 3, 3),
            conducting_staff_external_id="B456",
            conducting_staff_external_id_type="US_XX_STAFF_ID_TYPE_2",
        )
    ],
)


class TestCreateRootEntityIdToStaffIdMapping(BigQueryEmulatorTestCase):
    """Tests the CreatePersonIdToStaffIdMapping PTransform."""

    def setUp(self) -> None:
        super().setUp()
        self.test_pipeline = create_test_pipeline()

    def test_create_root_entity_id_to_staff_id_mapping_empty(self) -> None:
        input_state_persons = (
            self.test_pipeline | "Create input StatePerson" >> beam.Create([])
        )
        input_referenced_state_staff = (
            self.test_pipeline | "Create input referenced StateStaff" >> beam.Create([])
        )

        output = (
            input_referenced_state_staff,
            input_state_persons,
        ) | CreateRootEntityIdToStaffIdMapping(root_entity_cls=StatePerson)

        assert_that(output, equal_to([]))
        self.test_pipeline.run()

    def test_create_root_entity_id_to_staff_id_mapping_no_people(self) -> None:
        input_state_persons = (
            self.test_pipeline | "Create input StatePerson" >> beam.Create([])
        )
        input_referenced_state_staff = (
            self.test_pipeline
            | "Create input referenced StateStaff"
            >> beam.Create([STATE_STAFF_1, STATE_STAFF_2])
        )

        output = (
            input_referenced_state_staff,
            input_state_persons,
        ) | CreateRootEntityIdToStaffIdMapping(root_entity_cls=StatePerson)

        # Output only is generated if there are people, but nothing crashes
        assert_that(output, equal_to([]))
        self.test_pipeline.run()

    def test_create_root_entity_id_to_staff_id_mapping_one_person(self) -> None:
        expected_output = [
            (
                # STATE_PERSON_1
                789,
                {
                    # STATE_STAFF_1 external id referenced by STATE_PERSON_1
                    ("A123", "US_XX_STAFF_ID_TYPE"): 123,
                },
            )
        ]
        input_state_persons = (
            self.test_pipeline
            | "Create input StatePerson" >> beam.Create([STATE_PERSON_1])
        )
        input_referenced_state_staff = (
            self.test_pipeline
            | "Create input referenced StateStaff"
            >> beam.Create([STATE_STAFF_1, STATE_STAFF_2])
        )

        output = (
            input_referenced_state_staff,
            input_state_persons,
        ) | CreateRootEntityIdToStaffIdMapping(root_entity_cls=StatePerson)

        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()

    def test_create_root_entity_id_to_staff_id_mapping_for_staff(self) -> None:
        expected_output = [
            (
                # STATE_STAFF_1
                123,
                {
                    # STATE_STAFF_2 external id referenced by STATE_PERSON_1
                    ("A456", "US_XX_STAFF_ID_TYPE"): 456,
                },
            ),
            # STATE_STAFF_2 missing as it has no supervisor staff references
        ]
        input_state_persons = (
            self.test_pipeline
            | "Create input StateStaff" >> beam.Create([STATE_STAFF_1, STATE_STAFF_2])
        )
        input_referenced_state_staff = (
            self.test_pipeline
            | "Create input referenced StateStaff"
            >> beam.Create([STATE_STAFF_1, STATE_STAFF_2])
        )

        output = (
            input_referenced_state_staff,
            input_state_persons,
        ) | CreateRootEntityIdToStaffIdMapping(root_entity_cls=StateStaff)

        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()

    def test_create_root_entity_id_to_staff_id_mapping_complex(self) -> None:
        expected_output = [
            (
                # STATE_PERSON_1
                789,
                {
                    # STATE_STAFF_1 external id referenced by STATE_PERSON_1
                    ("A123", "US_XX_STAFF_ID_TYPE"): 123,
                },
            ),
            (
                # STATE_PERSON_2
                8910,
                {
                    # STATE_STAFF_1 external id referenced by STATE_PERSON_2 on SP 1
                    ("A123", "US_XX_STAFF_ID_TYPE"): 123,
                    # STATE_STAFF_2 external id referenced by STATE_PERSON_2 on SP 2
                    ("A456", "US_XX_STAFF_ID_TYPE"): 456,
                    # STATE_STAFF_2 external id referenced by STATE_PERSON_2 on Assessment 1
                    ("B456", "US_XX_STAFF_ID_TYPE_2"): 456,
                },
            ),
        ]
        input_state_persons = (
            self.test_pipeline
            | "Create input StatePerson"
            >> beam.Create([STATE_PERSON_1, STATE_PERSON_2])
        )
        input_referenced_state_staff = (
            self.test_pipeline
            | "Create input referenced StateStaff"
            >> beam.Create([STATE_STAFF_1, STATE_STAFF_2])
        )

        output = (
            input_referenced_state_staff,
            input_state_persons,
        ) | CreateRootEntityIdToStaffIdMapping(root_entity_cls=StatePerson)
        assert_that(output, equal_to(expected_output))
        self.test_pipeline.run()

    def test_create_person_id_to_staff_id_missing_staff(self) -> None:
        input_state_persons = self.test_pipeline | "Create input StatePerson" >> beam.Create(
            [
                # This person references external ids of both STATE_STAFF_1 and
                # STATE_STAFF_2
                STATE_PERSON_2,
            ]
        )
        input_referenced_state_staff = self.test_pipeline | "Create input referenced StateStaff" >> beam.Create(
            [
                # STATE_STAFF_2 is missing from this list
                STATE_STAFF_1,
            ]
        )

        _ = (
            input_referenced_state_staff,
            input_state_persons,
        ) | CreateRootEntityIdToStaffIdMapping(root_entity_cls=StatePerson)

        with self.assertRaisesRegex(
            ValueError,
            r"Did not find any ingest StateStaff corresponding to "
            r"\(external_id, id_type\)=",
        ):
            self.test_pipeline.run()

    def test_create_person_id_to_staff_id_multiple_conflicting_staff(self) -> None:
        input_state_persons = (
            self.test_pipeline
            | "Create input StatePerson" >> beam.Create([STATE_PERSON_1])
        )
        input_referenced_state_staff = (
            self.test_pipeline
            | "Create input referenced StateStaff"
            >> beam.Create(
                [
                    STATE_STAFF_1,
                    STATE_STAFF_1_CONFLICTING,
                ]
            )
        )

        _ = (
            input_referenced_state_staff,
            input_state_persons,
        ) | CreateRootEntityIdToStaffIdMapping(root_entity_cls=StatePerson)

        with self.assertRaisesRegex(
            ValueError,
            r"Found more than one ingested StateStaff with \(external_id, id_type\)=",
        ):
            self.test_pipeline.run()
