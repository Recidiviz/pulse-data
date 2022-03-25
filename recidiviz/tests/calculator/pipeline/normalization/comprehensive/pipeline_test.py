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
from typing import Any, Dict, List, Optional, Set, Type

import mock

from recidiviz.calculator.pipeline.normalization.comprehensive import pipeline
from recidiviz.calculator.pipeline.normalization.utils import (
    entity_normalization_manager_utils,
)
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
from recidiviz.tests.calculator.calculator_test_utils import (
    normalized_database_base_dict,
    normalized_database_base_dict_list,
)
from recidiviz.tests.calculator.pipeline.fake_bigquery import (
    FakeReadFromBigQueryFactory,
    FakeWriteNormalizedEntitiesToBigQuery,
    FakeWriteToBigQueryFactory,
)
from recidiviz.tests.calculator.pipeline.utils.run_pipeline_test_utils import (
    default_data_dict_for_root_schema_classes,
    run_test_pipeline,
)
from recidiviz.tests.calculator.pipeline.utils.state_utils.state_calculation_config_manager_test import (
    STATE_DELEGATES_FOR_TESTS,
)
from recidiviz.tests.persistence.database import database_test_utils

_STATE_CODE = "US_XX"


class TestComprehensiveNormalizationPipeline(unittest.TestCase):
    """Tests the comprehensive normalization pipeline."""

    def setUp(self) -> None:
        self.fake_bq_source_factory = FakeReadFromBigQueryFactory()

        self.fake_bq_sink_factory = FakeWriteToBigQueryFactory(
            FakeWriteNormalizedEntitiesToBigQuery
        )

        self.state_specific_delegate_patcher = mock.patch(
            "recidiviz.calculator.pipeline.utils.state_utils"
            ".state_calculation_config_manager.get_all_state_specific_delegates"
        )
        self.mock_get_state_delegate_container = (
            self.state_specific_delegate_patcher.start()
        )
        self.mock_get_state_delegate_container.return_value = STATE_DELEGATES_FOR_TESTS
        self.run_delegate_class = pipeline.ComprehensiveNormalizationPipelineRunDelegate

    def tearDown(self) -> None:
        self._stop_state_specific_delegate_patchers()

    def _stop_state_specific_delegate_patchers(self) -> None:
        self.state_specific_delegate_patcher.stop()

    def run_test_pipeline(
        self,
        state_code: str,
        data_dict: Dict[str, List[Dict]],
        unifying_id_field_filter_set: Optional[Set[int]] = None,
    ) -> None:
        """Runs a test version of the supervision pipeline."""
        project = "project"
        dataset = "dataset"

        read_from_bq_constructor = (
            self.fake_bq_source_factory.create_fake_bq_source_constructor(
                dataset, data_dict
            )
        )
        write_to_bq_constructor = self.fake_bq_sink_factory.create_fake_bq_sink_constructor(
            dataset,
            expected_output_tags=[
                entity.__name__
                for manager in pipeline.ComprehensiveNormalizationPipelineRunDelegate.required_entity_normalization_managers()
                for entity in manager.normalized_entity_classes()
            ],
        )

        run_test_pipeline(
            run_delegate=pipeline.ComprehensiveNormalizationPipelineRunDelegate,
            state_code=state_code,
            project_id=project,
            dataset_id=dataset,
            read_from_bq_constructor=read_from_bq_constructor,
            write_to_bq_constructor=write_to_bq_constructor,
            unifying_id_field_filter_set=unifying_id_field_filter_set,
        )

    def build_comprehensive_normalization_pipeline_data_dict(
        self, fake_person_id: int, state_code: str = "US_XX"
    ) -> Dict[str, List]:
        """Builds a data_dict for a basic run of the pipeline."""

        incarceration_period = schema.StateIncarcerationPeriod(
            incarceration_period_id=1111,
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
            state_code=state_code,
            county_code="124",
            admission_reason=StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
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

        incarceration_sentence = (
            database_test_utils.generate_test_incarceration_sentence(
                fake_person_id, [], []
            )
        )

        supervision_sentence = database_test_utils.generate_test_supervision_sentence(
            fake_person_id, [], []
        )

        incarceration_sentence_data = [
            normalized_database_base_dict(incarceration_sentence)
        ]

        supervision_sentence_data = [
            normalized_database_base_dict(supervision_sentence)
        ]

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

        data_dict = default_data_dict_for_root_schema_classes(
            [
                get_state_database_entity_with_name(entity_class.__name__)
                for entity_class in self.run_delegate_class.pipeline_config().required_entities
            ]
        )
        data_dict_overrides = {
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
            "us_mo_sentence_statuses": us_mo_sentence_status_data,
        }
        data_dict.update(data_dict_overrides)
        return data_dict

    def testComprehensiveNormalizationPipeline(self) -> None:
        fake_person_id = 12345
        data_dict = self.build_comprehensive_normalization_pipeline_data_dict(
            fake_person_id=fake_person_id
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
            set(
                pipeline.ComprehensiveNormalizationPipelineRunDelegate.pipeline_config().required_entities
            )
        )

        self.assertEqual(set(), missing_entities)
