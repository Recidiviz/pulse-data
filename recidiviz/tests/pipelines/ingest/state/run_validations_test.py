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
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactStatus,
)
from recidiviz.common.constants.state.state_task_deadline import StateTaskType
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity, RootEntity
from recidiviz.persistence.entity.entities_module_context_factory import (
    entities_module_context_for_module,
)
from recidiviz.persistence.entity.entity_utils import set_backedges
from recidiviz.persistence.entity.state import entities as state_entities
from recidiviz.persistence.entity.state import normalized_entities
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
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStatePersonExternalId,
    NormalizedStateStaff,
    NormalizedStateStaffExternalId,
    NormalizedStateStaffRolePeriod,
    NormalizedStateSupervisionContact,
    NormalizedStateSupervisionPeriod,
    NormalizedStateTaskDeadline,
)
from recidiviz.pipelines.ingest.state.run_validations import RunValidations
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


class TestRunValidationsPreNormalizationEntities(BigQueryEmulatorTestCase):
    """Pipeline tests for the ValidateRootEntities PTransform"""

    def setUp(self) -> None:
        super().setUp()
        apache_beam_pipeline_options = PipelineOptions()
        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = False
        self.test_pipeline = TestPipeline(options=apache_beam_pipeline_options)
        self.entities_module_context = entities_module_context_for_module(
            state_entities
        )

    def _set_backedges(self, element: Entity | RootEntity) -> Entity | RootEntity:
        return set_backedges(element, self.entities_module_context)

    @classmethod
    def state_code(cls) -> StateCode:
        return StateCode.US_DD

    def test_validate_single_staff_entity(self) -> None:
        entities = [
            self._set_backedges(
                StateStaff(
                    state_code="US_DD",
                    staff_id=1234,
                    external_ids=[
                        StateStaffExternalId(
                            staff_external_id_id=22222,
                            state_code="US_DD",
                            external_id="12345",
                            id_type="US_ZZ_TYPE",
                        )
                    ],
                )
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )
        output = input_entities | RunValidations(
            expected_output_entity_classes=[
                state_entities.StateStaff,
                state_entities.StateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=state_entities,
        )
        assert_that(output, matches_all(entities))
        self.test_pipeline.run()

    def test_validate_single_person_entity(self) -> None:
        entities = [
            self._set_backedges(
                StatePerson(
                    state_code="US_DD",
                    person_id=1234,
                    external_ids=[
                        StatePersonExternalId(
                            person_external_id_id=11111,
                            state_code="US_DD",
                            external_id="12345",
                            id_type="US_DD_TYPE",
                        )
                    ],
                )
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        output = input_entities | RunValidations(
            expected_output_entity_classes=[
                state_entities.StatePerson,
                state_entities.StatePersonExternalId,
            ],
            state_code=self.state_code(),
            entities_module=state_entities,
        )
        assert_that(output, matches_all(entities))
        self.test_pipeline.run()

    def test_validate_mixed_root_entities(self) -> None:
        entities_without_backedges = [
            StatePerson(
                state_code="US_DD",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_DD",
                staff_id=1237,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_DD",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=22224,
                        state_code="US_DD",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            StatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_DD",
                        external_id="4000",
                        id_type="US_DD_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_DD",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                    ),
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )
        output = input_entities | RunValidations(
            expected_output_entity_classes=[
                state_entities.StatePerson,
                state_entities.StatePersonExternalId,
                state_entities.StateStaff,
                state_entities.StateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=state_entities,
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
                state_code="US_DD",
                person_id=1234,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                    )
                ],
                task_deadlines=[
                    StateTaskDeadline(
                        task_deadline_id=1,
                        state_code="US_DD",
                        task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                        eligible_date=eligible_date,
                        update_datetime=update_datetime,
                        task_metadata=None,
                    )
                ],
            ),
            StatePerson(
                state_code="US_DD",
                person_id=4567,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=22222,
                        state_code="US_DD",
                        external_id="45678",
                        id_type="US_DD_TYPE",
                    )
                ],
                task_deadlines=[
                    StateTaskDeadline(
                        task_deadline_id=2,
                        state_code="US_DD",
                        task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                        eligible_date=eligible_date,
                        update_datetime=update_datetime,
                        task_metadata=None,
                    )
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        output = input_entities | RunValidations(
            expected_output_entity_classes=[
                state_entities.StatePerson,
                state_entities.StatePersonExternalId,
                state_entities.StateTaskDeadline,
            ],
            state_code=self.state_code(),
            entities_module=state_entities,
        )
        assert_that(output, matches_all(entities))
        self.test_pipeline.run()

    def test_missing_external_ids_staff_entity(self) -> None:
        entities = [
            self._set_backedges(
                StateStaff(state_code="US_DD", staff_id=1234, external_ids=[])
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[state_entities.StateStaff],
            state_code=self.state_code(),
            entities_module=state_entities,
        )

        with self.assertRaisesRegex(
            ValueError,
            r".*Found \[StateStaff\] with id \[1234\] missing an external_id:.*",
        ):
            self.test_pipeline.run()

    def test_missing_external_ids_person_entity(self) -> None:
        entities = [
            self._set_backedges(
                StatePerson(state_code="US_DD", person_id=1234, external_ids=[])
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[state_entities.StatePerson],
            state_code=self.state_code(),
            entities_module=state_entities,
        )

        with self.assertRaisesRegex(
            ValueError,
            r".*Found \[StatePerson\] with id \[1234\] missing an external_id:.*",
        ):
            self.test_pipeline.run()

    def test_validate_mixed_root_entities_dup_staff_id(self) -> None:
        entities_without_backedges = [
            StatePerson(
                state_code="US_DD",
                person_id=1234,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_DD",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=22224,
                        state_code="US_DD",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            StatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_DD",
                        external_id="4000",
                        id_type="US_DD_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_DD",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                    ),
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                state_entities.StatePerson,
                state_entities.StatePersonExternalId,
                state_entities.StateStaff,
                state_entities.StateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=state_entities,
        )

        expected_error_message = (
            r"Found errors for root entity StateStaff\(staff_id=1234, "
            r"external_ids=\[StateStaffExternalId\(external_id='12345', "
            r"id_type='US_ZZ_TYPE', staff_external_id_id=22222\)\]\):\n"
            r"  \* More than one StateStaff entity found with "
            r"\(staff_id=1234\).*"
        )

        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_validate_mixed_root_entities_dup_person_id(self) -> None:
        entities_without_backedges = [
            StatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_DD",
                staff_id=1235,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_DD",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=22224,
                        state_code="US_DD",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            StatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_DD",
                        external_id="4000",
                        id_type="US_DD_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_DD",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                    ),
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                state_entities.StatePerson,
                state_entities.StatePersonExternalId,
                state_entities.StateStaff,
                state_entities.StateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=state_entities,
        )

        expected_error_message = (
            r"Found errors for root entity StatePerson\(person_id=3000, "
            r"external_ids=\[StatePersonExternalId\(external_id='12345', "
            r"id_type='US_DD_TYPE', person_external_id_id=11111\)\]\):\n"
            r"  \* More than one StatePerson entity found with "
            r"\(person_id=3000\).*"
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_validate_simple_child_entities(self) -> None:
        entities_without_backedges = [
            StatePerson(
                state_code="US_DD",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_DD",
                        supervision_period_id=2,
                        external_id="sp2",
                        start_date=date(2020, 1, 1),
                        termination_date=date(2020, 2, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_DD",
                        supervision_period_id=200,
                        external_id="sp2",
                        start_date=date(2020, 2, 1),
                        termination_date=date(2020, 3, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_DD",
                        supervision_period_id=300,
                        external_id="sp3",
                        start_date=date(2020, 3, 1),
                    ),
                ],
                supervision_contacts=[
                    StateSupervisionContact.new_with_defaults(
                        state_code="US_DD",
                        external_id="c1",
                        contact_date=date(2018, 4, 1),
                        supervision_contact_id=101,
                        status=StateSupervisionContactStatus.COMPLETED,
                    )
                ],
            ),
            StateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_DD",
                staff_id=1235,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=222223,
                        state_code="US_DD",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_DD",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            StatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_DD",
                        external_id="4000",
                        id_type="US_DD_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_DD",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                    ),
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )
        output = input_entities | RunValidations(
            expected_output_entity_classes=[
                state_entities.StatePerson,
                state_entities.StatePersonExternalId,
                state_entities.StateSupervisionPeriod,
                state_entities.StateSupervisionContact,
                state_entities.StateStaff,
                state_entities.StateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=state_entities,
        )
        assert_that(output, matches_all(entities))
        self.test_pipeline.run()

    def test_validate_duplicate_id_same_child_entities(self) -> None:
        entities_without_backedges = [
            StatePerson(
                state_code="US_DD",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_DD",
                        supervision_period_id=2,
                        external_id="sp2",
                        start_date=date(2020, 1, 1),
                        termination_date=date(2020, 2, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_DD",
                        supervision_period_id=200,
                        external_id="sp2",
                        start_date=date(2020, 2, 1),
                        termination_date=date(2020, 3, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_DD",
                        supervision_period_id=300,
                        external_id="sp3",
                        start_date=date(2020, 3, 1),
                    ),
                ],
                supervision_contacts=[
                    StateSupervisionContact.new_with_defaults(
                        state_code="US_DD",
                        external_id="c1",
                        contact_date=date(2018, 4, 1),
                        supervision_contact_id=101,
                        status=StateSupervisionContactStatus.COMPLETED,
                    )
                ],
            ),
            StateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=2224,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_DD",
                staff_id=1235,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=2222,
                        state_code="US_DD",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=2223,
                        state_code="US_DD",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
                role_periods=[
                    StateStaffRolePeriod(
                        staff_role_period_id=1111,
                        state_code="US_DD",
                        external_id="5000",
                        role_type=StateStaffRoleType.SUPERVISION_OFFICER,
                        start_date=date(year=2020, day=1, month=2),
                    ),
                    StateStaffRolePeriod(
                        staff_role_period_id=1111,
                        state_code="US_DD",
                        external_id="4000",
                        role_type=StateStaffRoleType.SUPERVISION_OFFICER,
                        start_date=date(year=2018, day=1, month=2),
                    ),
                ],
            ),
            StatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=1111,
                        state_code="US_DD",
                        external_id="4000",
                        id_type="US_DD_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=1112,
                        state_code="US_DD",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                    ),
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                state_entities.StatePerson,
                state_entities.StatePersonExternalId,
                state_entities.StateSupervisionContact,
                state_entities.StateSupervisionPeriod,
                state_entities.StateStaff,
                state_entities.StateStaffExternalId,
                state_entities.StateStaffRolePeriod,
            ],
            state_code=self.state_code(),
            entities_module=state_entities,
        )

        expected_error_message = (
            r"Found errors for root entity StateStaff\(staff_id=1235, "
            r"external_ids=\[StateStaffExternalId\(external_id='2000', "
            r"id_type='US_ZZ_TYPE', staff_external_id_id=2222\),"
            r"StateStaffExternalId\(external_id='3000', id_type='MOD', staff_external_id_id=2223\)\]\):\n"
            r"  \* More than one StateStaffRolePeriod entity found with "
            r"\(staff_role_period_id=1111\).*"
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_validate_duplicate_id_diff_child_entities(self) -> None:
        entities_without_backedges = [
            StatePerson(
                state_code="US_DD",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_DD",
                        supervision_period_id=2,
                        external_id="sp2",
                        start_date=date(2020, 1, 1),
                        termination_date=date(2020, 2, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_DD",
                        supervision_period_id=200,
                        external_id="sp2",
                        start_date=date(2020, 2, 1),
                        termination_date=date(2020, 3, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_DD",
                        supervision_period_id=300,
                        external_id="sp3",
                        start_date=date(2020, 3, 1),
                    ),
                ],
                supervision_contacts=[
                    StateSupervisionContact.new_with_defaults(
                        state_code="US_DD",
                        external_id="c1",
                        contact_date=date(2018, 4, 1),
                        supervision_contact_id=101,
                        status=StateSupervisionContactStatus.COMPLETED,
                    )
                ],
            ),
            StateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=11112,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_DD",
                staff_id=1235,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=2222,
                        state_code="US_DD",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=2223,
                        state_code="US_DD",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            StatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=111222,
                        state_code="US_DD",
                        external_id="4000",
                        id_type="US_DD_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=111223,
                        state_code="US_DD",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                    ),
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_DD",
                        supervision_period_id=300,
                        external_id="sp3",
                        start_date=date(2020, 4, 4),
                    ),
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                state_entities.StatePerson,
                state_entities.StatePersonExternalId,
                state_entities.StateSupervisionContact,
                state_entities.StateSupervisionPeriod,
                state_entities.StateStaff,
                state_entities.StateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=state_entities,
        )

        expected_error_message = (
            r"Found errors for root entity StatePerson\(person_id=1237, "
            r"external_ids=\[StatePersonExternalId\(external_id='12345', "
            r"id_type='US_DD_TYPE', person_external_id_id=11111\)\]\):\n"
            r"  \* More than one StateSupervisionPeriod entity found with "
            r"\(supervision_period_id=300\).*"
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_validate_duplicate_id_multiple_child_entities(self) -> None:
        entities_without_backedges = [
            StatePerson(
                state_code="US_DD",
                person_id=1237,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                    )
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_DD",
                        supervision_period_id=2,
                        external_id="sp2",
                        start_date=date(2020, 1, 1),
                        termination_date=date(2020, 2, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_DD",
                        supervision_period_id=200,
                        external_id="sp2",
                        start_date=date(2020, 2, 1),
                        termination_date=date(2020, 3, 1),
                    ),
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_DD",
                        supervision_period_id=300,
                        external_id="sp3",
                        start_date=date(2020, 3, 1),
                    ),
                ],
                supervision_contacts=[
                    StateSupervisionContact.new_with_defaults(
                        state_code="US_DD",
                        external_id="c1",
                        contact_date=date(2018, 4, 1),
                        supervision_contact_id=101,
                        status=StateSupervisionContactStatus.COMPLETED,
                    )
                ],
            ),
            StateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22224,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            StateStaff(
                state_code="US_DD",
                staff_id=1235,
                external_ids=[
                    StateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_DD",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    StateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_DD",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            StatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_DD",
                        external_id="4000",
                        id_type="US_DD_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_DD",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                    ),
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_DD",
                        supervision_period_id=311,
                        external_id="sp3",
                        start_date=date(2020, 4, 4),
                    ),
                ],
            ),
            StatePerson(
                state_code="US_DD",
                person_id=3111,
                external_ids=[
                    StatePersonExternalId(
                        person_external_id_id=11114,
                        state_code="US_DD",
                        external_id="4001",
                        id_type="US_DD_TYPE",
                    ),
                    StatePersonExternalId(
                        person_external_id_id=11115,
                        state_code="US_DD",
                        external_id="5001",
                        id_type="US_YY_TYPE",
                    ),
                ],
                supervision_periods=[
                    StateSupervisionPeriod.new_with_defaults(
                        state_code="US_DD",
                        supervision_period_id=311,
                        external_id="sp5",
                        start_date=date(2020, 4, 4),
                    ),
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                state_entities.StatePerson,
                state_entities.StatePersonExternalId,
                state_entities.StateSupervisionContact,
                state_entities.StateSupervisionPeriod,
                state_entities.StateStaff,
                state_entities.StateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=state_entities,
        )

        expected_error_message = (
            r"Found errors for root entity StatePerson\(person_id=3000, "
            r"external_ids=\[StatePersonExternalId\(external_id='4000', "
            r"id_type='US_DD_TYPE', person_external_id_id=11112\),"
            r"StatePersonExternalId\(external_id='5000', "
            r"id_type='US_YY_TYPE', person_external_id_id=11113\)\]\):\n"
            r"  \* More than one StateSupervisionPeriod entity found with "
            r"\(supervision_period_id=311\).*"
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_unique_constraint_state_person_external_id_simple(self) -> None:
        person1 = StatePerson(
            state_code="US_DD",
            person_id=1234,
        )
        person1.external_ids.append(
            StatePersonExternalId(
                person_external_id_id=11111,
                state_code="US_DD",
                external_id="12345",
                id_type="US_DD_TYPE",
                person=person1,
            ),
        )

        person2 = StatePerson(
            state_code="US_DD",
            person_id=1235,
        )

        person2.external_ids.append(
            StatePersonExternalId(
                person_external_id_id=11112,
                state_code="US_DD",
                external_id="12345",
                id_type="US_DD_TYPE",
                person=person2,
            )
        )
        entities = [self._set_backedges(e) for e in [person1, person2]]

        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                state_entities.StatePerson,
                state_entities.StatePersonExternalId,
            ],
            state_code=self.state_code(),
            entities_module=state_entities,
        )

        expected_error_message = (
            r"Found errors for root entity StatePerson\(person_id=1234, "
            r"external_ids=\[StatePersonExternalId\(external_id='12345', "
            r"id_type='US_DD_TYPE', person_external_id_id=11111\)\]\):\n"
            r"  \* More than one StatePersonExternalId entity found with "
            r"\(state_code=US_DD, id_type=US_DD_TYPE, external_id=12345\).*"
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_unique_constraint_state_supervision_contact(self) -> None:
        person1 = StatePerson(
            state_code="US_DD",
            person_id=1237,
        )
        person1.external_ids.append(
            StatePersonExternalId(
                person_external_id_id=11111,
                state_code="US_DD",
                external_id="12345",
                id_type="US_DD_TYPE",
                person=person1,
            )
        )
        person1.supervision_periods.append(
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_DD",
                supervision_period_id=2,
                external_id="sp2",
                start_date=date(2020, 1, 1),
                termination_date=date(2020, 2, 1),
                person=person1,
            )
        )
        person1.supervision_periods.append(
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_DD",
                supervision_period_id=200,
                external_id="sp2",
                start_date=date(2020, 2, 1),
                termination_date=date(2020, 3, 1),
                person=person1,
            ),
        )
        person1.supervision_periods.append(
            StateSupervisionPeriod.new_with_defaults(
                state_code="US_DD",
                supervision_period_id=300,
                external_id="sp3",
                start_date=date(2020, 3, 1),
                person=person1,
            ),
        )
        person1.supervision_contacts.append(
            StateSupervisionContact.new_with_defaults(
                state_code="US_DD",
                external_id="c1",
                contact_date=date(2018, 4, 1),
                supervision_contact_id=101,
                person=person1,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        )
        person1.supervision_contacts.append(
            StateSupervisionContact.new_with_defaults(
                state_code="US_DD",
                external_id="c2",
                contact_date=date(2018, 4, 1),
                supervision_contact_id=102,
                person=person1,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        )
        staff1 = StateStaff(
            state_code="US_DD",
            staff_id=1234,
        )
        staff1.external_ids.append(
            StateStaffExternalId(
                staff_external_id_id=22222,
                state_code="US_DD",
                external_id="12345",
                id_type="US_ZZ_TYPE",
                staff=staff1,
            )
        )

        person2 = StatePerson(
            state_code="US_DD",
            person_id=3000,
        )

        person2.external_ids.append(
            StatePersonExternalId(
                person_external_id_id=11112,
                state_code="US_DD",
                external_id="4000",
                id_type="US_DD_TYPE",
                person=person2,
            )
        )

        person2.external_ids.append(
            StatePersonExternalId(
                person_external_id_id=11113,
                state_code="US_DD",
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
                status=StateSupervisionContactStatus.ATTEMPTED,
            ),
        )

        person2.supervision_contacts.append(
            StateSupervisionContact.new_with_defaults(
                state_code="US_DD",
                external_id="c2",
                contact_date=date(2020, 4, 1),
                supervision_contact_id=105,
                person=person2,
                status=StateSupervisionContactStatus.ATTEMPTED,
            ),
        )
        entities = [self._set_backedges(e) for e in [person1, staff1, person2]]

        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                state_entities.StatePerson,
                state_entities.StatePersonExternalId,
                state_entities.StateSupervisionContact,
                state_entities.StateSupervisionPeriod,
                state_entities.StateStaff,
                state_entities.StateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=state_entities,
        )

        expected_error_message = (
            r"Found errors for root entity StatePerson\(person_id=1237, "
            r"external_ids=\[StatePersonExternalId\(external_id='12345', "
            r"id_type='US_DD_TYPE', person_external_id_id=11111\)\]\):\n"
            r"  \* More than one StateSupervisionContact entity found with "
            r"\(state_code=US_DD, external_id=c2\).*"
        )

        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_entity_tree_unique_constraints_simple_invalid(self) -> None:
        person = StatePerson(
            state_code="US_DD",
            person_id=3111,
            external_ids=[
                StatePersonExternalId(
                    person_external_id_id=11114,
                    state_code="US_DD",
                    external_id="4001",
                    id_type="US_DD_TYPE",
                ),
            ],
        )

        person.task_deadlines.append(
            StateTaskDeadline(
                task_deadline_id=1,
                state_code="US_DD",
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
                state_code="US_DD",
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
                state_code="US_DD",
                task_type=StateTaskType.INTERNAL_UNKNOWN,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )

        entities = [self._set_backedges(e) for e in [person]]

        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                state_entities.StatePerson,
                state_entities.StatePersonExternalId,
                state_entities.StateTaskDeadline,
            ],
            state_code=self.state_code(),
            entities_module=state_entities,
        )
        expected_error_message = (
            r"Found errors for root entity StatePerson\(person_id=3111, "
            r"external_ids=\[StatePersonExternalId\(external_id='4001', "
            r"id_type='US_DD_TYPE', person_external_id_id=11114\)\]\):\n"
            r"  \* Found \[2\] StateTaskDeadline entities with \(state_code=US_DD, "
            r"task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION, task_subtype=None, "
            r"task_metadata=\{\"external_id\": \"00000001-111123-371006\", \"sentence_type\": \"INCARCERATION\"\}, "
            r"update_datetime=2023-02-01 11:19:00\).*"
        )

        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_validate_non_zero_entities(self) -> None:
        entities = [
            self._set_backedges(
                StateStaff(
                    state_code="US_DD",
                    staff_id=1234,
                    external_ids=[
                        StateStaffExternalId(
                            staff_external_id_id=22222,
                            state_code="US_DD",
                            external_id="12345",
                            id_type="US_ZZ_TYPE",
                        )
                    ],
                )
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                state_entities.StateStaff,
                state_entities.StateStaffExternalId,
                state_entities.StateStaffRolePeriod,
            ],
            state_code=self.state_code(),
            entities_module=state_entities,
        )
        with self.assertRaisesRegex(
            ValueError,
            r".*Expected non-zero StateStaffRolePeriod entities to be produced, but none were produced.",
        ):
            self.test_pipeline.run()

    def test_validate_only_expected_entities(self) -> None:
        entities = [
            self._set_backedges(
                StateStaff(
                    state_code="US_DD",
                    staff_id=1234,
                    external_ids=[
                        StateStaffExternalId(
                            staff_external_id_id=22222,
                            state_code="US_DD",
                            external_id="12345",
                            id_type="US_ZZ_TYPE",
                        )
                    ],
                )
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                state_entities.StateStaff,
                # Missing state_entities.StateStaffExternalId
            ],
            state_code=self.state_code(),
            entities_module=state_entities,
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Found output entities of type \[StateStaffExternalId\] that are not in "
            r"the expected entities list.",
        ):
            self.test_pipeline.run()


class TestRunValidationsNormalizedEntities(BigQueryEmulatorTestCase):
    """Pipeline tests for the ValidateRootEntities PTransform"""

    def setUp(self) -> None:
        super().setUp()
        apache_beam_pipeline_options = PipelineOptions()
        apache_beam_pipeline_options.view_as(SetupOptions).save_main_session = False
        self.test_pipeline = TestPipeline(options=apache_beam_pipeline_options)
        self.entities_module_context = entities_module_context_for_module(
            normalized_entities
        )

    @classmethod
    def state_code(cls) -> StateCode:
        return StateCode.US_DD

    def _set_backedges(self, element: Entity | RootEntity) -> Entity | RootEntity:
        return set_backedges(element, self.entities_module_context)

    def test_validate_single_staff_entity(self) -> None:
        entities = [
            self._set_backedges(
                NormalizedStateStaff(
                    state_code="US_DD",
                    staff_id=1234,
                    external_ids=[
                        NormalizedStateStaffExternalId(
                            staff_external_id_id=22222,
                            state_code="US_DD",
                            external_id="12345",
                            id_type="US_ZZ_TYPE",
                        )
                    ],
                )
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )
        output = input_entities | RunValidations(
            expected_output_entity_classes=[
                normalized_entities.NormalizedStateStaff,
                normalized_entities.NormalizedStateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )
        assert_that(output, matches_all(entities))
        self.test_pipeline.run()

    def test_validate_single_person_entity(self) -> None:
        entities = [
            self._set_backedges(
                NormalizedStatePerson(
                    state_code="US_DD",
                    person_id=1234,
                    external_ids=[
                        NormalizedStatePersonExternalId(
                            person_external_id_id=11111,
                            state_code="US_DD",
                            external_id="12345",
                            id_type="US_DD_TYPE",
                            is_current_display_id_for_type=True,
                            id_active_from_datetime=datetime(2020, 1, 1),
                            id_active_to_datetime=None,
                        )
                    ],
                )
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        output = input_entities | RunValidations(
            expected_output_entity_classes=[
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
            ],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )
        assert_that(output, matches_all(entities))
        self.test_pipeline.run()

    def test_validate_mixed_root_entities(self) -> None:
        entities_without_backedges: list[Entity] = [
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=1237,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    )
                ],
            ),
            NormalizedStateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            NormalizedStateStaff(
                state_code="US_DD",
                staff_id=1237,
                external_ids=[
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_DD",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=22224,
                        state_code="US_DD",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_DD",
                        external_id="4000",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    ),
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_DD",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2021, 1, 1),
                        id_active_to_datetime=None,
                    ),
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )
        output = input_entities | RunValidations(
            expected_output_entity_classes=[
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
                normalized_entities.NormalizedStateStaff,
                normalized_entities.NormalizedStateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )
        assert_that(output, matches_all(entities))
        self.test_pipeline.run()

    def test_validate_task_deadlines(self) -> None:
        """Exact sane task deadline on two different people should not violate
        constraints.
        """
        update_datetime = datetime(2023, 2, 1, 11, 19)
        eligible_date = date(2020, 9, 11)
        entities_without_backedges: list[Entity] = [
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=1234,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    )
                ],
                task_deadlines=[
                    NormalizedStateTaskDeadline(
                        task_deadline_id=1,
                        state_code="US_DD",
                        task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                        eligible_date=eligible_date,
                        update_datetime=update_datetime,
                        task_metadata=None,
                    )
                ],
            ),
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=4567,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=22222,
                        state_code="US_DD",
                        external_id="45678",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    )
                ],
                task_deadlines=[
                    NormalizedStateTaskDeadline(
                        task_deadline_id=2,
                        state_code="US_DD",
                        task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                        eligible_date=eligible_date,
                        update_datetime=update_datetime,
                        task_metadata=None,
                    )
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        output = input_entities | RunValidations(
            expected_output_entity_classes=[
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
                normalized_entities.NormalizedStateTaskDeadline,
            ],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )
        assert_that(output, matches_all(entities))
        self.test_pipeline.run()

    def test_missing_external_ids_staff_entity(self) -> None:
        entities = [
            self._set_backedges(
                NormalizedStateStaff(state_code="US_DD", staff_id=1234, external_ids=[])
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[normalized_entities.NormalizedStateStaff],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )

        with self.assertRaisesRegex(
            ValueError,
            r".*Found \[NormalizedStateStaff\] with id \[1234\] missing an external_id:.*",
        ):
            self.test_pipeline.run()

    def test_missing_external_ids_person_entity(self) -> None:
        entities = [
            self._set_backedges(
                NormalizedStatePerson(
                    state_code="US_DD", person_id=1234, external_ids=[]
                )
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[normalized_entities.NormalizedStatePerson],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )

        with self.assertRaisesRegex(
            ValueError,
            r".*Found \[NormalizedStatePerson\] with id \[1234\] missing an external_id:.*",
        ):
            self.test_pipeline.run()

    def test_validate_mixed_root_entities_dup_staff_id(self) -> None:
        entities_without_backedges: list[Entity] = [
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=1234,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    )
                ],
            ),
            NormalizedStateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            NormalizedStateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_DD",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=22224,
                        state_code="US_DD",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_DD",
                        external_id="4000",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    ),
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_DD",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    ),
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
                normalized_entities.NormalizedStateStaff,
                normalized_entities.NormalizedStateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )

        expected_error_message = (
            r"Found errors for root entity NormalizedStateStaff\(staff_id=1234, "
            r"external_ids=\[NormalizedStateStaffExternalId\(external_id='12345', "
            r"id_type='US_ZZ_TYPE', staff_external_id_id=22222\)\]\):\n"
            r"  \* More than one NormalizedStateStaff entity found with "
            r"\(staff_id=1234\).*"
        )

        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_validate_mixed_root_entities_dup_person_id(self) -> None:
        entities_without_backedges: list[Entity] = [
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    )
                ],
            ),
            NormalizedStateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            NormalizedStateStaff(
                state_code="US_DD",
                staff_id=1235,
                external_ids=[
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_DD",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=22224,
                        state_code="US_DD",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_DD",
                        external_id="4000",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    ),
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_DD",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    ),
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
                normalized_entities.NormalizedStateStaff,
                normalized_entities.NormalizedStateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )

        expected_error_message = (
            r"Found errors for root entity NormalizedStatePerson\(person_id=3000, "
            r"external_ids=\[NormalizedStatePersonExternalId\(external_id='12345', "
            r"id_type='US_DD_TYPE', person_external_id_id=11111\)\]\):\n"
            r"  \* More than one NormalizedStatePerson entity found with "
            r"\(person_id=3000\).*"
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_validate_simple_child_entities(self) -> None:
        entities_without_backedges: list[Entity] = [
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=1237,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    )
                ],
                supervision_periods=[
                    NormalizedStateSupervisionPeriod(
                        state_code="US_DD",
                        supervision_period_id=2,
                        external_id="sp2",
                        start_date=date(2020, 1, 1),
                        termination_date=date(2020, 2, 1),
                    ),
                    NormalizedStateSupervisionPeriod(
                        state_code="US_DD",
                        supervision_period_id=200,
                        external_id="sp2",
                        start_date=date(2020, 2, 1),
                        termination_date=date(2020, 3, 1),
                    ),
                    NormalizedStateSupervisionPeriod(
                        state_code="US_DD",
                        supervision_period_id=300,
                        external_id="sp3",
                        start_date=date(2020, 3, 1),
                    ),
                ],
                supervision_contacts=[
                    NormalizedStateSupervisionContact(
                        state_code="US_DD",
                        external_id="c1",
                        contact_date=date(2018, 4, 1),
                        supervision_contact_id=101,
                        status=StateSupervisionContactStatus.COMPLETED,
                    )
                ],
            ),
            NormalizedStateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            NormalizedStateStaff(
                state_code="US_DD",
                staff_id=1235,
                external_ids=[
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=222223,
                        state_code="US_DD",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_DD",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_DD",
                        external_id="4000",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    ),
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_DD",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    ),
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )
        output = input_entities | RunValidations(
            expected_output_entity_classes=[
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
                normalized_entities.NormalizedStateSupervisionPeriod,
                normalized_entities.NormalizedStateSupervisionContact,
                normalized_entities.NormalizedStateStaff,
                normalized_entities.NormalizedStateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )
        assert_that(output, matches_all(entities))
        self.test_pipeline.run()

    def test_validate_duplicate_id_same_child_entities(self) -> None:
        entities_without_backedges: list[Entity] = [
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=1237,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    )
                ],
                supervision_periods=[
                    NormalizedStateSupervisionPeriod(
                        state_code="US_DD",
                        supervision_period_id=2,
                        external_id="sp2",
                        start_date=date(2020, 1, 1),
                        termination_date=date(2020, 2, 1),
                    ),
                    NormalizedStateSupervisionPeriod(
                        state_code="US_DD",
                        supervision_period_id=200,
                        external_id="sp2",
                        start_date=date(2020, 2, 1),
                        termination_date=date(2020, 3, 1),
                    ),
                    NormalizedStateSupervisionPeriod(
                        state_code="US_DD",
                        supervision_period_id=300,
                        external_id="sp3",
                        start_date=date(2020, 3, 1),
                    ),
                ],
                supervision_contacts=[
                    NormalizedStateSupervisionContact(
                        state_code="US_DD",
                        external_id="c1",
                        contact_date=date(2018, 4, 1),
                        supervision_contact_id=101,
                        status=StateSupervisionContactStatus.COMPLETED,
                    )
                ],
            ),
            NormalizedStateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=2224,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            NormalizedStateStaff(
                state_code="US_DD",
                staff_id=1235,
                external_ids=[
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=2222,
                        state_code="US_DD",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=2223,
                        state_code="US_DD",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
                role_periods=[
                    NormalizedStateStaffRolePeriod(
                        staff_role_period_id=1111,
                        state_code="US_DD",
                        external_id="5000",
                        role_type=StateStaffRoleType.SUPERVISION_OFFICER,
                        start_date=date(year=2020, day=1, month=2),
                    ),
                    NormalizedStateStaffRolePeriod(
                        staff_role_period_id=1111,
                        state_code="US_DD",
                        external_id="4000",
                        role_type=StateStaffRoleType.SUPERVISION_OFFICER,
                        start_date=date(year=2018, day=1, month=2),
                    ),
                ],
            ),
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=1111,
                        state_code="US_DD",
                        external_id="4000",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    ),
                    NormalizedStatePersonExternalId(
                        person_external_id_id=1112,
                        state_code="US_DD",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    ),
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
                normalized_entities.NormalizedStateSupervisionContact,
                normalized_entities.NormalizedStateSupervisionPeriod,
                normalized_entities.NormalizedStateStaff,
                normalized_entities.NormalizedStateStaffExternalId,
                normalized_entities.NormalizedStateStaffRolePeriod,
            ],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )

        expected_error_message = (
            r"Found errors for root entity NormalizedStateStaff\(staff_id=1235, "
            r"external_ids=\[NormalizedStateStaffExternalId\(external_id='2000', "
            r"id_type='US_ZZ_TYPE', staff_external_id_id=2222\),"
            r"NormalizedStateStaffExternalId\(external_id='3000', id_type='MOD', staff_external_id_id=2223\)\]\):\n"
            r"  \* More than one NormalizedStateStaffRolePeriod entity found with "
            r"\(staff_role_period_id=1111\).*"
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_validate_duplicate_id_diff_child_entities(self) -> None:
        entities_without_backedges: list[Entity] = [
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=1237,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    )
                ],
                supervision_periods=[
                    NormalizedStateSupervisionPeriod(
                        state_code="US_DD",
                        supervision_period_id=2,
                        external_id="sp2",
                        start_date=date(2020, 1, 1),
                        termination_date=date(2020, 2, 1),
                    ),
                    NormalizedStateSupervisionPeriod(
                        state_code="US_DD",
                        supervision_period_id=200,
                        external_id="sp2",
                        start_date=date(2020, 2, 1),
                        termination_date=date(2020, 3, 1),
                    ),
                    NormalizedStateSupervisionPeriod(
                        state_code="US_DD",
                        supervision_period_id=300,
                        external_id="sp3",
                        start_date=date(2020, 3, 1),
                    ),
                ],
                supervision_contacts=[
                    NormalizedStateSupervisionContact(
                        state_code="US_DD",
                        external_id="c1",
                        contact_date=date(2018, 4, 1),
                        supervision_contact_id=101,
                        status=StateSupervisionContactStatus.COMPLETED,
                    )
                ],
            ),
            NormalizedStateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=11112,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            NormalizedStateStaff(
                state_code="US_DD",
                staff_id=1235,
                external_ids=[
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=2222,
                        state_code="US_DD",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=2223,
                        state_code="US_DD",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=111222,
                        state_code="US_DD",
                        external_id="4000",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    ),
                    NormalizedStatePersonExternalId(
                        person_external_id_id=111223,
                        state_code="US_DD",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    ),
                ],
                supervision_periods=[
                    NormalizedStateSupervisionPeriod(
                        state_code="US_DD",
                        supervision_period_id=300,
                        external_id="sp3",
                        start_date=date(2020, 4, 4),
                    ),
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
                normalized_entities.NormalizedStateSupervisionContact,
                normalized_entities.NormalizedStateSupervisionPeriod,
                normalized_entities.NormalizedStateStaff,
                normalized_entities.NormalizedStateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )

        expected_error_message = (
            r"Found errors for root entity NormalizedStatePerson\(person_id=1237, "
            r"external_ids=\[NormalizedStatePersonExternalId\(external_id='12345', "
            r"id_type='US_DD_TYPE', person_external_id_id=11111\)\]\):\n"
            r"  \* More than one NormalizedStateSupervisionPeriod entity found with "
            r"\(supervision_period_id=300\).*"
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_validate_duplicate_id_multiple_child_entities(self) -> None:
        entities_without_backedges: list[Entity] = [
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=1237,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11111,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    )
                ],
                supervision_periods=[
                    NormalizedStateSupervisionPeriod(
                        state_code="US_DD",
                        supervision_period_id=2,
                        external_id="sp2",
                        start_date=date(2020, 1, 1),
                        termination_date=date(2020, 2, 1),
                    ),
                    NormalizedStateSupervisionPeriod(
                        state_code="US_DD",
                        supervision_period_id=200,
                        external_id="sp2",
                        start_date=date(2020, 2, 1),
                        termination_date=date(2020, 3, 1),
                    ),
                    NormalizedStateSupervisionPeriod(
                        state_code="US_DD",
                        supervision_period_id=300,
                        external_id="sp3",
                        start_date=date(2020, 3, 1),
                    ),
                ],
                supervision_contacts=[
                    NormalizedStateSupervisionContact(
                        state_code="US_DD",
                        external_id="c1",
                        contact_date=date(2018, 4, 1),
                        supervision_contact_id=101,
                        status=StateSupervisionContactStatus.COMPLETED,
                    )
                ],
            ),
            NormalizedStateStaff(
                state_code="US_DD",
                staff_id=1234,
                external_ids=[
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=22224,
                        state_code="US_DD",
                        external_id="12345",
                        id_type="US_ZZ_TYPE",
                    )
                ],
            ),
            NormalizedStateStaff(
                state_code="US_DD",
                staff_id=1235,
                external_ids=[
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=22222,
                        state_code="US_DD",
                        external_id="2000",
                        id_type="US_ZZ_TYPE",
                    ),
                    NormalizedStateStaffExternalId(
                        staff_external_id_id=22223,
                        state_code="US_DD",
                        external_id="3000",
                        id_type="MOD",
                    ),
                ],
            ),
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=3000,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11112,
                        state_code="US_DD",
                        external_id="4000",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    ),
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11113,
                        state_code="US_DD",
                        external_id="5000",
                        id_type="US_YY_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    ),
                ],
                supervision_periods=[
                    NormalizedStateSupervisionPeriod(
                        state_code="US_DD",
                        supervision_period_id=311,
                        external_id="sp3",
                        start_date=date(2020, 4, 4),
                    ),
                ],
            ),
            NormalizedStatePerson(
                state_code="US_DD",
                person_id=3111,
                external_ids=[
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11114,
                        state_code="US_DD",
                        external_id="4001",
                        id_type="US_DD_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    ),
                    NormalizedStatePersonExternalId(
                        person_external_id_id=11115,
                        state_code="US_DD",
                        external_id="5001",
                        id_type="US_YY_TYPE",
                        is_current_display_id_for_type=True,
                        id_active_from_datetime=datetime(2020, 1, 1),
                        id_active_to_datetime=None,
                    ),
                ],
                supervision_periods=[
                    NormalizedStateSupervisionPeriod(
                        state_code="US_DD",
                        supervision_period_id=311,
                        external_id="sp5",
                        start_date=date(2020, 4, 4),
                    ),
                ],
            ),
        ]
        entities = [self._set_backedges(e) for e in entities_without_backedges]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
                normalized_entities.NormalizedStateSupervisionContact,
                normalized_entities.NormalizedStateSupervisionPeriod,
                normalized_entities.NormalizedStateStaff,
                normalized_entities.NormalizedStateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )

        expected_error_message = (
            r"Found errors for root entity NormalizedStatePerson\(person_id=3000, "
            r"external_ids=\[NormalizedStatePersonExternalId\(external_id='4000', "
            r"id_type='US_DD_TYPE', person_external_id_id=11112\),"
            r"NormalizedStatePersonExternalId\(external_id='5000', "
            r"id_type='US_YY_TYPE', person_external_id_id=11113\)\]\):\n"
            r"  \* More than one NormalizedStateSupervisionPeriod entity found with "
            r"\(supervision_period_id=311\).*"
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_unique_constraint_state_person_external_id_simple(self) -> None:
        person1 = NormalizedStatePerson(
            state_code="US_DD",
            person_id=1234,
        )
        person1.external_ids.append(
            NormalizedStatePersonExternalId(
                person_external_id_id=11111,
                state_code="US_DD",
                external_id="12345",
                id_type="US_DD_TYPE",
                is_current_display_id_for_type=True,
                id_active_from_datetime=datetime(2020, 1, 1),
                id_active_to_datetime=None,
                person=person1,
            ),
        )

        person2 = NormalizedStatePerson(
            state_code="US_DD",
            person_id=1235,
        )

        person2.external_ids.append(
            NormalizedStatePersonExternalId(
                person_external_id_id=11112,
                state_code="US_DD",
                external_id="12345",
                id_type="US_DD_TYPE",
                is_current_display_id_for_type=True,
                id_active_from_datetime=datetime(2020, 1, 1),
                id_active_to_datetime=None,
                person=person2,
            )
        )
        entities = [self._set_backedges(e) for e in [person1, person2]]

        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
            ],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )

        expected_error_message = (
            r"Found errors for root entity NormalizedStatePerson\(person_id=1234, "
            r"external_ids=\[NormalizedStatePersonExternalId\(external_id='12345', "
            r"id_type='US_DD_TYPE', person_external_id_id=11111\)\]\):\n"
            r"  \* More than one NormalizedStatePersonExternalId entity found with "
            r"\(state_code=US_DD, id_type=US_DD_TYPE, external_id=12345\).*"
        )
        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_unique_constraint_state_supervision_contact(self) -> None:
        person1 = NormalizedStatePerson(
            state_code="US_DD",
            person_id=1237,
        )
        person1.external_ids.append(
            NormalizedStatePersonExternalId(
                person_external_id_id=11111,
                state_code="US_DD",
                external_id="12345",
                id_type="US_DD_TYPE",
                is_current_display_id_for_type=True,
                id_active_from_datetime=datetime(2020, 1, 1),
                id_active_to_datetime=None,
                person=person1,
            )
        )
        person1.supervision_periods.append(
            NormalizedStateSupervisionPeriod(
                state_code="US_DD",
                supervision_period_id=2,
                external_id="sp2",
                start_date=date(2020, 1, 1),
                termination_date=date(2020, 2, 1),
                person=person1,
            )
        )
        person1.supervision_periods.append(
            NormalizedStateSupervisionPeriod(
                state_code="US_DD",
                supervision_period_id=200,
                external_id="sp2",
                start_date=date(2020, 2, 1),
                termination_date=date(2020, 3, 1),
                person=person1,
            ),
        )
        person1.supervision_periods.append(
            NormalizedStateSupervisionPeriod(
                state_code="US_DD",
                supervision_period_id=300,
                external_id="sp3",
                start_date=date(2020, 3, 1),
                person=person1,
            ),
        )
        person1.supervision_contacts.append(
            NormalizedStateSupervisionContact(
                state_code="US_DD",
                external_id="c1",
                contact_date=date(2018, 4, 1),
                supervision_contact_id=101,
                person=person1,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        )
        person1.supervision_contacts.append(
            NormalizedStateSupervisionContact(
                state_code="US_DD",
                external_id="c2",
                contact_date=date(2018, 4, 1),
                supervision_contact_id=102,
                person=person1,
                status=StateSupervisionContactStatus.COMPLETED,
            )
        )
        staff1 = NormalizedStateStaff(
            state_code="US_DD",
            staff_id=1234,
        )
        staff1.external_ids.append(
            NormalizedStateStaffExternalId(
                staff_external_id_id=22222,
                state_code="US_DD",
                external_id="12345",
                id_type="US_ZZ_TYPE",
                staff=staff1,
            )
        )

        person2 = NormalizedStatePerson(
            state_code="US_DD",
            person_id=3000,
        )

        person2.external_ids.append(
            NormalizedStatePersonExternalId(
                person_external_id_id=11112,
                state_code="US_DD",
                external_id="4000",
                id_type="US_DD_TYPE",
                is_current_display_id_for_type=True,
                id_active_from_datetime=datetime(2020, 1, 1),
                id_active_to_datetime=None,
                person=person2,
            )
        )

        person2.external_ids.append(
            NormalizedStatePersonExternalId(
                person_external_id_id=11113,
                state_code="US_DD",
                external_id="5000",
                id_type="US_YY_TYPE",
                is_current_display_id_for_type=True,
                id_active_from_datetime=datetime(2020, 1, 1),
                id_active_to_datetime=None,
                person=person2,
            )
        )

        person2.supervision_contacts.append(
            NormalizedStateSupervisionContact(
                state_code="US_YY",
                external_id="c2",
                contact_date=date(2020, 4, 1),
                supervision_contact_id=104,
                person=person2,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        )

        person2.supervision_contacts.append(
            NormalizedStateSupervisionContact(
                state_code="US_DD",
                external_id="c2",
                contact_date=date(2020, 4, 1),
                supervision_contact_id=105,
                person=person2,
                status=StateSupervisionContactStatus.COMPLETED,
            ),
        )
        entities_without_backedges: list[Entity] = [person1, staff1, person2]
        entities = [self._set_backedges(e) for e in entities_without_backedges]

        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
                normalized_entities.NormalizedStateSupervisionContact,
                normalized_entities.NormalizedStateSupervisionPeriod,
                normalized_entities.NormalizedStateStaff,
                normalized_entities.NormalizedStateStaffExternalId,
            ],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )

        expected_error_message = (
            r"Found errors for root entity NormalizedStatePerson\(person_id=1237, "
            r"external_ids=\[NormalizedStatePersonExternalId\(external_id='12345', "
            r"id_type='US_DD_TYPE', person_external_id_id=11111\)\]\):\n"
            r"  \* More than one NormalizedStateSupervisionContact entity found with "
            r"\(state_code=US_DD, external_id=c2\).*"
        )

        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_entity_tree_unique_constraints_simple_invalid(self) -> None:
        person = NormalizedStatePerson(
            state_code="US_DD",
            person_id=3111,
            external_ids=[
                NormalizedStatePersonExternalId(
                    person_external_id_id=11114,
                    state_code="US_DD",
                    external_id="4001",
                    id_type="US_DD_TYPE",
                    is_current_display_id_for_type=True,
                    id_active_from_datetime=datetime(2020, 1, 1),
                    id_active_to_datetime=None,
                ),
            ],
        )

        person.task_deadlines.append(
            NormalizedStateTaskDeadline(
                task_deadline_id=1,
                state_code="US_DD",
                task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )

        person.task_deadlines.append(
            NormalizedStateTaskDeadline(
                task_deadline_id=2,
                state_code="US_DD",
                task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )
        person.task_deadlines.append(
            NormalizedStateTaskDeadline(
                task_deadline_id=3,
                state_code="US_DD",
                task_type=StateTaskType.INTERNAL_UNKNOWN,
                eligible_date=date(2020, 9, 11),
                update_datetime=datetime(2023, 2, 1, 11, 19),
                task_metadata='{"external_id": "00000001-111123-371006", "sentence_type": "INCARCERATION"}',
                person=person,
            )
        )

        entities = [self._set_backedges(e) for e in [person]]

        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                normalized_entities.NormalizedStatePerson,
                normalized_entities.NormalizedStatePersonExternalId,
                normalized_entities.NormalizedStateTaskDeadline,
            ],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )
        expected_error_message = (
            r"Found errors for root entity NormalizedStatePerson\(person_id=3111, "
            r"external_ids=\[NormalizedStatePersonExternalId\(external_id='4001', "
            r"id_type='US_DD_TYPE', person_external_id_id=11114\)\]\):\n"
            r"  \* Found \[2\] NormalizedStateTaskDeadline entities with \(state_code=US_DD, "
            r"task_type=StateTaskType.DISCHARGE_FROM_INCARCERATION, task_subtype=None, "
            r"task_metadata=\{\"external_id\": \"00000001-111123-371006\", \"sentence_type\": \"INCARCERATION\"\}, "
            r"update_datetime=2023-02-01 11:19:00\).*"
        )

        with self.assertRaisesRegex(ValueError, expected_error_message):
            self.test_pipeline.run()

    def test_validate_non_zero_entities(self) -> None:
        entities = [
            self._set_backedges(
                NormalizedStateStaff(
                    state_code="US_DD",
                    staff_id=1234,
                    external_ids=[
                        NormalizedStateStaffExternalId(
                            staff_external_id_id=22222,
                            state_code="US_DD",
                            external_id="12345",
                            id_type="US_ZZ_TYPE",
                        )
                    ],
                )
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                normalized_entities.NormalizedStateStaff,
                normalized_entities.NormalizedStateStaffExternalId,
                normalized_entities.NormalizedStateStaffRolePeriod,
            ],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )
        with self.assertRaisesRegex(
            ValueError,
            r".*Expected non-zero NormalizedStateStaffRolePeriod entities to be "
            r"produced, but none were produced.",
        ):
            self.test_pipeline.run()

    def test_validate_only_expected_entities(self) -> None:
        entities = [
            self._set_backedges(
                NormalizedStateStaff(
                    state_code="US_DD",
                    staff_id=1234,
                    external_ids=[
                        NormalizedStateStaffExternalId(
                            staff_external_id_id=22222,
                            state_code="US_DD",
                            external_id="12345",
                            id_type="US_ZZ_TYPE",
                        )
                    ],
                )
            )
        ]
        input_entities = self.test_pipeline | "Create test input" >> beam.Create(
            entities
        )

        _ = input_entities | RunValidations(
            expected_output_entity_classes=[
                normalized_entities.NormalizedStateStaff,
                # Missing normalized_entities.NormalizedStateStaffExternalId
            ],
            state_code=self.state_code(),
            entities_module=normalized_entities,
        )
        with self.assertRaisesRegex(
            ValueError,
            r"Found output entities of type \[NormalizedStateStaffExternalId\] that "
            r"are not in the expected entities list.",
        ):
            self.test_pipeline.run()
