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
"""Tests the comprehensive normalization pipeline."""
import datetime
import unittest
from typing import Any, Dict, Iterable, List, Optional, Set, Type
from unittest.mock import patch

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.calculator.query.state.dataset_config import STATE_BASE_DATASET
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodAdmissionReason,
    StateIncarcerationPeriodReleaseReason,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.persistence.database.schema.state import schema
from recidiviz.persistence.database.schema_utils import (
    get_state_database_entity_with_name,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import StatePerson
from recidiviz.pipelines.normalization.comprehensive import entity_normalizer, pipeline
from recidiviz.pipelines.normalization.utils import entity_normalization_manager_utils
from recidiviz.pipelines.utils.execution_utils import RootEntityId
from recidiviz.tests.persistence.database import database_test_utils
from recidiviz.tests.pipelines.calculator_test_utils import (
    normalized_database_base_dict,
    normalized_database_base_dict_list,
)
from recidiviz.tests.pipelines.fake_bigquery import (
    FakeReadFromBigQueryFactory,
    FakeWriteNormalizedEntitiesToBigQuery,
    FakeWriteToBigQueryFactory,
)
from recidiviz.tests.pipelines.fake_state_calculation_config_manager import (
    start_pipeline_delegate_getter_patchers,
)
from recidiviz.tests.pipelines.utils.run_pipeline_test_utils import (
    DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX,
    default_data_dict_for_root_schema_classes,
    run_test_pipeline,
)

_STATE_CODE = "US_XX"


class TestComprehensiveNormalizationPipeline(unittest.TestCase):
    """Tests the comprehensive normalization pipeline."""

    def setUp(self) -> None:
        self.project_id = "test-project"
        self.project_id_patcher = patch("recidiviz.utils.metadata.project_id")
        self.project_id_patcher.start().return_value = self.project_id

        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()

        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(
            FakeWriteNormalizedEntitiesToBigQuery
        )

        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            entity_normalizer
        )
        self.pipeline_class = pipeline.ComprehensiveNormalizationPipeline

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

        self.project_id_patcher.stop()

    def run_test_pipeline(
        self,
        state_code: str,
        data_dict: Dict[str, Iterable[Dict]],
        root_entity_id_filter_set: Optional[Set[RootEntityId]] = None,
    ) -> None:
        """Runs a test version of the normalization pipeline."""
        read_from_bq_constructor = (
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                expected_entities_dataset=STATE_BASE_DATASET,
                data_dict=data_dict,
            )
        )
        write_to_bq_constructor = self.fake_bq_sink_factory.create_fake_bq_sink_constructor(
            expected_dataset=BigQueryAddressOverrides.format_sandbox_dataset(
                DEFAULT_TEST_PIPELINE_OUTPUT_SANDBOX_PREFIX,
                f"{state_code.lower()}_normalized_state",
            ),
            expected_output_tags=[
                entity.__name__
                for _, managers in self.pipeline_class.required_entity_normalization_managers().items()
                for manager in managers
                for entity in manager.normalized_entity_classes()
            ]
            + [
                f"{child_entity.__name__}_{parent_entity.__name__}"
                for _, managers in self.pipeline_class.required_entity_normalization_managers().items()
                for manager in managers
                for child_entity, parent_entity in manager.normalized_entity_associations()
            ],
        )

        run_test_pipeline(
            pipeline_cls=self.pipeline_class,
            state_code=state_code,
            project_id=self.project_id,
            read_from_bq_constructor=read_from_bq_constructor,
            write_to_bq_constructor=write_to_bq_constructor,
            root_entity_id_filter_set=root_entity_id_filter_set,
        )

    def build_comprehensive_normalization_pipeline_data_dict(
        self, fake_person_id: int, fake_staff_id: int, state_code: str = "US_XX"
    ) -> Dict[str, Iterable]:
        """Builds a data_dict for a basic run of the pipeline."""

        person = database_test_utils.generate_test_person(
            person_id=fake_person_id,
            state_code=state_code,
            incarceration_incidents=[],
            supervision_violations=[],
            supervision_contacts=[],
            incarceration_sentences=[],
            supervision_sentences=[],
            incarceration_periods=[],
            supervision_periods=[],
        )

        person_data = [normalized_database_base_dict(person)]

        incarceration_period = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
            external_id="ip1",
            incarceration_type=StateIncarcerationType.STATE_PRISON,
            state_code=state_code,
            county_code="124",
            facility="San Quentin",
            admission_reason=StateIncarcerationPeriodAdmissionReason.NEW_ADMISSION,
            admission_date=datetime.date(2008, 11, 20),
            release_date=datetime.date(2010, 12, 4),
            release_reason=StateIncarcerationPeriodReleaseReason.SENTENCE_SERVED,
            person_id=fake_person_id,
        )

        supervision_period = schema.StateSupervisionPeriod(
            supervision_period_id=1111,
            external_id="sp1",
            state_code=state_code,
            county_code="124",
            admission_reason=StateSupervisionPeriodAdmissionReason.RELEASE_FROM_INCARCERATION,
            start_date=datetime.date(2010, 12, 4),
            termination_reason=StateSupervisionPeriodTerminationReason.REVOCATION,
            termination_date=datetime.date(2011, 4, 5),
            supervision_type=StateSupervisionPeriodSupervisionType.PROBATION,
            supervision_level=StateSupervisionLevel.MINIMUM,
            person_id=fake_person_id,
            case_type_entries=[
                schema.StateSupervisionCaseTypeEntry(
                    supervision_case_type_entry_id=888,
                    state_code=state_code,
                    person_id=fake_person_id,
                    case_type=StateSupervisionCaseType.DRUG_COURT,
                    supervision_period_id=1111,
                )
            ],
        )

        charge = database_test_utils.generate_test_charge(fake_person_id, 1)
        early_discharge = database_test_utils.generate_test_early_discharge(
            fake_person_id
        )

        incarceration_sentence = (
            database_test_utils.generate_test_incarceration_sentence(
                fake_person_id, [charge], []
            )
        )

        supervision_sentence = database_test_utils.generate_test_supervision_sentence(
            fake_person_id, [charge], [early_discharge]
        )
        early_discharge.supervision_sentence_id = (
            supervision_sentence.supervision_sentence_id
        )

        charge_to_incarceration_sentence_association = [
            {
                "charge_id": charge.charge_id,
                "incarceration_sentence_id": incarceration_sentence.incarceration_sentence_id,
                "state_code": state_code,
            }
        ]
        charge_to_supervision_sentence_association = [
            {
                "charge_id": charge.charge_id,
                "supervision_sentence_id": supervision_sentence.supervision_sentence_id,
                "state_code": state_code,
            }
        ]

        incarceration_sentence_data = [
            normalized_database_base_dict(incarceration_sentence)
        ]

        supervision_sentence_data = [
            normalized_database_base_dict(supervision_sentence)
        ]

        charge_data = normalized_database_base_dict_list(
            incarceration_sentence.charges
        ) + normalized_database_base_dict_list(supervision_sentence.charges)

        early_discharge_data = normalized_database_base_dict_list(
            supervision_sentence.early_discharges
        )

        incarceration_periods_data = [
            normalized_database_base_dict(incarceration_period),
        ]

        supervision_periods_data = [normalized_database_base_dict(supervision_period)]

        supervision_period_case_type_data = normalized_database_base_dict_list(
            supervision_period.case_type_entries
        )

        supervision_violation_response = (
            database_test_utils.generate_test_supervision_violation_response(
                fake_person_id
            )
        )

        supervision_violation = database_test_utils.generate_test_supervision_violation(
            fake_person_id, [supervision_violation_response]
        )

        supervision_violation_data = [
            normalized_database_base_dict(supervision_violation)
        ]

        supervision_violation_type_data = normalized_database_base_dict_list(
            supervision_violation.supervision_violation_types
        )

        supervision_violated_condition_data = normalized_database_base_dict_list(
            supervision_violation.supervision_violated_conditions
        )

        supervision_violation_response.supervision_violation_id = (
            supervision_violation.supervision_violation_id
        )

        supervision_violation_response_data = [
            normalized_database_base_dict(supervision_violation_response)
        ]

        supervision_violation_response_decision_data = (
            normalized_database_base_dict_list(
                supervision_violation_response.supervision_violation_response_decisions
            )
        )

        program_assignment = database_test_utils.generate_test_program_assignment(
            fake_person_id
        )

        program_assignment_data = [normalized_database_base_dict(program_assignment)]

        supervision_contact = database_test_utils.generate_test_supervision_contact(
            fake_person_id
        )

        supervision_contact_data = [normalized_database_base_dict(supervision_contact)]

        assessment = database_test_utils.generate_test_assessment(fake_person_id)

        assessment_data = [normalized_database_base_dict(assessment)]

        staff_external_id = database_test_utils.generate_test_staff_external_id(
            fake_staff_id
        )
        staff_external_id_data = [normalized_database_base_dict(staff_external_id)]
        staff_caseload_period = (
            database_test_utils.generate_test_staff_caseload_type_period(fake_staff_id)
        )
        staff_caseload_data = [normalized_database_base_dict(staff_caseload_period)]
        staff_location_period = database_test_utils.generate_test_staff_location_period(
            fake_staff_id
        )
        staff_location_data = [normalized_database_base_dict(staff_location_period)]
        staff_role_period = database_test_utils.generate_test_staff_role_period(
            fake_staff_id
        )
        staff_role_data = [normalized_database_base_dict(staff_role_period)]
        staff_supervisor_period = (
            database_test_utils.generate_test_staff_supervisor_period(fake_staff_id)
        )
        staff_supervisor_data = [normalized_database_base_dict(staff_supervisor_period)]

        staff = database_test_utils.generate_test_staff(
            fake_staff_id,
            external_ids=[staff_external_id],
            location_periods=[staff_location_period],
            role_periods=[staff_role_period],
            supervisor_periods=[staff_supervisor_period],
            caseload_type_periods=[staff_caseload_period],
        )

        staff_data = [normalized_database_base_dict(staff)]

        us_mo_sentence_status_data: List[Dict[str, Any]] = (
            [
                {
                    "state_code": state_code,
                    "person_id": fake_person_id,
                    "sentence_external_id": "XXX",
                    "sentence_status_external_id": "YYY",
                    "status_code": "ZZZ",
                    "status_date": "not_a_date",
                    "status_description": "XYZ",
                }
            ]
            if state_code == "US_MO"
            else []
        )

        offense_description_labels_data: List[Dict[str, Any]] = []

        state_person_to_staff_data: List[Dict[str, Any]] = []

        data_dict = default_data_dict_for_root_schema_classes(
            [
                get_state_database_entity_with_name(entity_class.__name__)
                for entity_class in self.pipeline_class.required_entities().get(
                    StatePerson, []
                )
            ]
        )
        data_dict_overrides = {
            schema.StatePerson.__tablename__: person_data,
            schema.StateIncarcerationSentence.__tablename__: incarceration_sentence_data,
            schema.StateSupervisionSentence.__tablename__: supervision_sentence_data,
            schema.StateIncarcerationPeriod.__tablename__: incarceration_periods_data,
            schema.StateSupervisionPeriod.__tablename__: supervision_periods_data,
            schema.StateSupervisionCaseTypeEntry.__tablename__: supervision_period_case_type_data,
            schema.StateSupervisionViolationResponse.__tablename__: supervision_violation_response_data,
            schema.StateSupervisionViolationResponseDecisionEntry.__tablename__: supervision_violation_response_decision_data,
            schema.StateSupervisionViolation.__tablename__: supervision_violation_data,
            schema.StateSupervisionViolationTypeEntry.__tablename__: supervision_violation_type_data,
            schema.StateSupervisionViolatedConditionEntry.__tablename__: supervision_violated_condition_data,
            schema.StateProgramAssignment.__tablename__: program_assignment_data,
            schema.StateSupervisionContact.__tablename__: supervision_contact_data,
            schema.StateAssessment.__tablename__: assessment_data,
            schema.StateCharge.__tablename__: charge_data,
            schema.StateEarlyDischarge.__tablename__: early_discharge_data,
            schema.StateStaff.__tablename__: staff_data,
            schema.StateStaffCaseloadTypePeriod.__tablename__: staff_caseload_data,
            schema.StateStaffExternalId.__tablename__: staff_external_id_data,
            schema.StateStaffLocationPeriod.__tablename__: staff_location_data,
            schema.StateStaffRolePeriod.__tablename__: staff_role_data,
            schema.StateStaffSupervisorPeriod.__tablename__: staff_supervisor_data,
            "state_charge_incarceration_sentence_association": charge_to_incarceration_sentence_association,
            "state_charge_supervision_sentence_association": charge_to_supervision_sentence_association,
            "us_mo_sentence_statuses": us_mo_sentence_status_data,
            "state_charge_offense_description_to_labels": offense_description_labels_data,
            "state_person_to_state_staff": state_person_to_staff_data,
        }
        data_dict.update(data_dict_overrides)
        return data_dict

    def test_comprehensive_normalization_pipeline(self) -> None:
        fake_person_id = 12345
        fake_staff_id = 2345
        data_dict = self.build_comprehensive_normalization_pipeline_data_dict(
            fake_person_id=fake_person_id, fake_staff_id=fake_staff_id
        )

        self.run_test_pipeline(
            state_code=_STATE_CODE,
            data_dict=data_dict,
        )

    def test_required_entities_completeness(self) -> None:
        """Tests that there are no entities in the normalized_entity_classes list of a
        normalization manager that aren't also listed as required by the pipeline."""
        all_normalized_entities: Set[Type[Entity]] = set()

        for manager in entity_normalization_manager_utils.NORMALIZATION_MANAGERS:
            normalized_entities = set(manager.normalized_entity_classes())
            all_normalized_entities.update(normalized_entities)

        missing_entities = all_normalized_entities.difference(
            {
                item
                for _, items in self.pipeline_class.required_entities().items()
                for item in items
            }
        )

        self.assertEqual(set(), missing_entities)
