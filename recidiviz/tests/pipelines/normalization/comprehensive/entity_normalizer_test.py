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
"""Tests the entity_normalizer."""
import unittest
from copy import deepcopy
from typing import Any, Dict, List, Optional, Sequence, Union

import attr

from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
    normalized_entity_class_with_base_class_name,
)
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateCharge,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StateProgramAssignment,
    StateSentence,
    StateSentenceStatusSnapshot,
    StateStaff,
    StateStaffRolePeriod,
    StateStaffSupervisorPeriod,
    StateSupervisionContact,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateIncarcerationPeriod,
    NormalizedStateProgramAssignment,
    NormalizedStateStaffRolePeriod,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionContact,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.pipelines.normalization.comprehensive import entity_normalizer, pipeline
from recidiviz.pipelines.normalization.comprehensive.entity_normalizer import (
    EntityNormalizerResult,
)
from recidiviz.pipelines.normalization.comprehensive.state_person_to_state_staff_query_provider import (
    STATE_PERSON_TO_STATE_STAFF_QUERY_NAME,
)
from recidiviz.pipelines.normalization.utils.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)
from recidiviz.pipelines.utils.execution_utils import TableRow
from recidiviz.tests.persistence.entity.normalized_entities_utils_test import (
    get_normalized_violation_tree,
    get_violation_tree,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    generate_full_graph_state_person,
    generate_full_graph_state_staff,
)
from recidiviz.tests.pipelines.fake_state_calculation_config_manager import (
    start_pipeline_delegate_getter_patchers,
)
from recidiviz.tests.pipelines.normalization.utils.entity_normalization_manager_utils_test import (
    STATE_PERSON_TO_STATE_STAFF_LIST,
)

_STATE_CODE = "US_XX"

# TODO(#21376) Properly refactor once strategy for separate normalization is defined.
class TestNormalizeEntities(unittest.TestCase):
    """Tests the normalize_entities function on the ComprehensiveEntityNormalizer."""

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            entity_normalizer
        )

        self.pipeline = pipeline.ComprehensiveNormalizationPipeline
        self.entity_normalizer = entity_normalizer.ComprehensiveEntityNormalizer(
            state_code=StateCode.US_XX
        )

        self.full_graph_person = generate_full_graph_state_person(
            set_back_edges=True, include_person_back_edges=False, set_ids=True
        )
        self.full_graph_staff = generate_full_graph_state_staff(
            set_back_edges=True, set_ids=True
        )

        self.violation_responses: List[StateSupervisionViolationResponse] = []
        for v in self.full_graph_person.supervision_violations:
            self.violation_responses.extend(v.supervision_violation_responses)

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def _run_normalize_person_entities(
        self,
        incarceration_periods: Optional[List[StateIncarcerationPeriod]],
        supervision_periods: Optional[List[StateSupervisionPeriod]],
        violation_responses: Optional[List[StateSupervisionViolationResponse]],
        program_assignments: Optional[List[StateProgramAssignment]],
        incarceration_sentences: Optional[List[StateIncarcerationSentence]],
        supervision_sentences: Optional[List[StateSupervisionSentence]],
        sentences: Optional[List[StateSentence]],
        sentence_status_snapshots: Optional[List[StateSentenceStatusSnapshot]],
        assessments: Optional[List[StateAssessment]],
        persons: Optional[List[StatePerson]],
        supervision_contacts: Optional[List[StateSupervisionContact]],
        state_person_to_state_staff: Optional[List[Dict[str, Any]]],
    ) -> EntityNormalizerResult:
        """Helper for testing the normalize_entities function on the
        ComprehensiveEntityNormalizer for StatePerson."""
        all_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            StateIncarcerationPeriod.__name__: incarceration_periods or [],
            StateSupervisionPeriod.__name__: supervision_periods or [],
            StateSupervisionViolationResponse.__name__: violation_responses or [],
            StateProgramAssignment.__name__: program_assignments or [],
            StateIncarcerationSentence.__name__: incarceration_sentences or [],
            StateSupervisionSentence.__name__: supervision_sentences or [],
            StateSentence.__name__: sentences or [],
            StateSentenceStatusSnapshot.__name__: sentence_status_snapshots or [],
            StateAssessment.__name__: assessments or [],
            StatePerson.__name__: persons or [],
            StateSupervisionContact.__name__: supervision_contacts or [],
            STATE_PERSON_TO_STATE_STAFF_QUERY_NAME: state_person_to_state_staff or [],
        }

        assert self.full_graph_person.person_id is not None

        return self.entity_normalizer.normalize_entities(
            self.full_graph_person.person_id, StatePerson, all_kwargs
        )

    def _run_normalize_staff_entities(
        self,
        staff_role_periods: Optional[List[StateStaffRolePeriod]],
        staff_supervisor_periods: Optional[List[StateStaffSupervisorPeriod]],
    ) -> EntityNormalizerResult:
        """Helper for testing the normalize_entities function on the ComprehensiveEntityNormalizer
        for StateStaff."""
        all_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            StateStaffRolePeriod.__name__: staff_role_periods or [],
            StateStaffSupervisorPeriod.__name__: staff_supervisor_periods or [],
        }

        assert self.full_graph_staff.staff_id is not None

        return self.entity_normalizer.normalize_entities(
            self.full_graph_staff.staff_id, StateStaff, all_kwargs
        )

    def test_normalize_person_entities(self) -> None:
        expected_additional_attributes_map: AdditionalAttributesMap = {
            StateIncarcerationPeriod.__name__: {
                ip.incarceration_period_id: {
                    "sequence_num": index,
                    "purpose_for_incarceration_subtype": None,
                    "incarceration_admission_violation_type": None,
                }
                for index, ip in enumerate(self.full_graph_person.incarceration_periods)
                if ip.incarceration_period_id
            }
        }

        expected_additional_attributes_map.update(
            {
                StateSupervisionPeriod.__name__: {
                    sp.supervision_period_id: {
                        "sequence_num": index,
                        "supervising_officer_staff_id": 10000,
                    }
                    for index, sp in enumerate(
                        self.full_graph_person.supervision_periods
                    )
                    if sp.supervision_period_id
                }
            }
        )

        expected_additional_attributes_map.update(
            {
                StateProgramAssignment.__name__: {
                    pa.program_assignment_id: {
                        "referring_staff_id": 10000,
                        "sequence_num": index,
                    }
                    for index, pa in enumerate(
                        self.full_graph_person.program_assignments
                    )
                    if pa.program_assignment_id
                }
            }
        )

        violation = get_violation_tree()
        violation_responses = violation.supervision_violation_responses

        expected_additional_attributes_map.update(
            {
                StateSupervisionViolationResponse.__name__: {
                    vr.supervision_violation_response_id: {
                        "sequence_num": index,
                        "deciding_staff_id": None,
                    }
                    for index, vr in enumerate(violation_responses)
                    if vr.supervision_violation_response_id
                }
            }
        )

        expected_additional_attributes_map.update(
            {
                StateAssessment.__name__: {
                    assessment.assessment_id: {
                        "assessment_score_bucket": "39+"
                        if assessment.assessment_score == 55
                        else "0-23",
                        "conducting_staff_id": 30000,
                        "sequence_num": index,
                    }
                    for index, assessment in enumerate(
                        self.full_graph_person.assessments
                    )
                    if assessment.assessment_id
                }
            }
        )

        expected_additional_attributes_map.update(
            {
                StateSupervisionContact.__name__: {
                    supervision_contact.supervision_contact_id: {
                        "contacting_staff_id": 20000,
                    }
                    for index, supervision_contact in enumerate(
                        self.full_graph_person.supervision_contacts
                    )
                    if supervision_contact.supervision_contact_id
                }
            }
        )

        expected_additional_attributes_map.update(
            {
                StateIncarcerationSentence.__name__: {},
                StateSupervisionSentence.__name__: {},
            }
        )

        expected_additional_attributes_map.update(
            {
                StateCharge.__name__: {
                    charge.charge_id: {
                        "ncic_code_external": charge.ncic_code,
                        "ncic_category_external": None,
                        "description_external": charge.description,
                        "is_violent_external": charge.is_violent,
                        "is_drug_external": charge.is_drug,
                        "is_sex_offense_external": charge.is_sex_offense,
                    }
                    for supervision_sentence in self.full_graph_person.supervision_sentences
                    for charge in supervision_sentence.charges
                    if charge.charge_id
                }
            }
        )

        expected_additional_attributes_map.update(
            {
                StateCharge.__name__: {
                    charge.charge_id: {
                        "ncic_code_external": charge.ncic_code,
                        "ncic_category_external": None,
                        "description_external": charge.description,
                        "is_violent_external": charge.is_violent,
                        "is_drug_external": charge.is_drug,
                        "is_sex_offense_external": charge.is_sex_offense,
                    }
                    for incarceration_sentence in self.full_graph_person.incarceration_sentences
                    for charge in incarceration_sentence.charges
                    if charge.charge_id
                }
            }
        )

        expected_output_entities: Dict[str, Sequence[Entity]] = {
            StateIncarcerationPeriod.__name__: [
                attr.evolve(
                    ip,
                    specialized_purpose_for_incarceration=StateSpecializedPurposeForIncarceration.GENERAL,
                )
                for ip in self.full_graph_person.incarceration_periods
            ],
            StateSupervisionPeriod.__name__: [
                attr.evolve(sp) for sp in self.full_graph_person.supervision_periods
            ],
            StateSupervisionViolation.__name__: [attr.evolve(violation)],
            StateProgramAssignment.__name__: [
                attr.evolve(pa) for pa in self.full_graph_person.program_assignments
            ],
            StateAssessment.__name__: [
                attr.evolve(assessment)
                for assessment in self.full_graph_person.assessments
            ],
            StateIncarcerationSentence.__name__: [
                attr.evolve(incarceration_sentence)
                for incarceration_sentence in self.full_graph_person.incarceration_sentences
            ],
            StateSupervisionSentence.__name__: [
                attr.evolve(supervision_sentence)
                for supervision_sentence in self.full_graph_person.supervision_sentences
            ],
            StateSupervisionContact.__name__: [
                attr.evolve(sc) for sc in self.full_graph_person.supervision_contacts
            ],
        }

        (
            normalized_entities,
            additional_attributes_map,
        ) = self._run_normalize_person_entities(
            persons=[self.full_graph_person],
            incarceration_periods=self.full_graph_person.incarceration_periods,
            supervision_periods=self.full_graph_person.supervision_periods,
            violation_responses=violation_responses,
            program_assignments=self.full_graph_person.program_assignments,
            assessments=self.full_graph_person.assessments,
            supervision_contacts=self.full_graph_person.supervision_contacts,
            incarceration_sentences=self.full_graph_person.incarceration_sentences,
            supervision_sentences=self.full_graph_person.supervision_sentences,
            sentences=self.full_graph_person.sentences,
            sentence_status_snapshots=[
                snapshot
                for sentence in self.full_graph_person.sentences
                for snapshot in sentence.sentence_status_snapshots
            ],
            state_person_to_state_staff=[
                {
                    "person_id": self.full_graph_person.person_id,
                    "staff_id": 10000,
                    "staff_external_id": "EMP1",
                    "staff_external_id_type": "US_XX_STAFF_ID",
                },
                {
                    "person_id": self.full_graph_person.person_id,
                    "staff_id": 20000,
                    "staff_external_id": "EMP2",
                    "staff_external_id_type": "US_XX_STAFF_ID",
                },
                {
                    "person_id": self.full_graph_person.person_id,
                    "staff_id": 30000,
                    "staff_external_id": "EMP3",
                    "staff_external_id_type": "US_XX_STAFF_ID",
                },
            ],
        )
        self.assertDictEqual(
            expected_additional_attributes_map, additional_attributes_map
        )

        for entity_name, items in normalized_entities.items():
            self.assertEqual(expected_output_entities[entity_name], items)

    def test_normalize_person_entities_missing_entities(self) -> None:
        """Tests that calling normalize_entities will still work even when a person
        doesn't have instances of all the entities that get normalized."""

        _ = self._run_normalize_person_entities(
            persons=[self.full_graph_person],
            incarceration_periods=self.full_graph_person.incarceration_periods,
            supervision_periods=[],
            violation_responses=get_violation_tree().supervision_violation_responses,
            program_assignments=[],
            incarceration_sentences=[],
            supervision_sentences=[],
            sentences=[],
            sentence_status_snapshots=[],
            assessments=[],
            supervision_contacts=[],
            state_person_to_state_staff=[],
        )

    def test_normalize_staff_entities(self) -> None:
        expected_output_entities = {
            StateStaffRolePeriod.__name__: self.full_graph_staff.role_periods,
        }
        (
            normalized_entities,
            additional_attributes_map,
        ) = self._run_normalize_staff_entities(
            self.full_graph_staff.role_periods, self.full_graph_staff.supervisor_periods
        )

        self.assertDictEqual(
            additional_attributes_map,
            {
                StateStaffRolePeriod.__name__: {
                    role_period.staff_role_period_id: {}
                    for role_period in self.full_graph_staff.role_periods
                }
            },
        )
        for entity_name, items in normalized_entities.items():
            self.assertEqual(expected_output_entities[entity_name], items)


# TODO(#21376) Properly refactor once strategy for separate normalization is defined.
class TestNormalizeEntitiesConvertedToNormalized(unittest.TestCase):
    """Tests that the output of the normalize_entities function on the
    ComprehensiveEntityNormalizer can be converted into fully hydrated trees of
    Normalized entities.

    This isn't a process that is ever done in the pipelines, but ensures that the
    values in the AdditionalAttributesMap in the output contain all required values
    necessary to load the Normalized version of the entities in the downstream metric
    pipelines.
    """

    def setUp(self) -> None:
        self.delegate_patchers = start_pipeline_delegate_getter_patchers(
            entity_normalizer
        )

        self.pipeline = pipeline.ComprehensiveNormalizationPipeline
        self.entity_normalizer = entity_normalizer.ComprehensiveEntityNormalizer(
            state_code=StateCode.US_XX
        )

        self.full_graph_person = generate_full_graph_state_person(
            set_back_edges=True, include_person_back_edges=False, set_ids=True
        )

        self.full_graph_staff = generate_full_graph_state_staff(
            set_back_edges=True, set_ids=True
        )

        self.violation_responses: List[StateSupervisionViolationResponse] = []
        for v in self.full_graph_person.supervision_violations:
            self.violation_responses.extend(v.supervision_violation_responses)

    def tearDown(self) -> None:
        for patcher in self.delegate_patchers:
            patcher.stop()

    def _normalize_person_entities_and_convert(
        self,
        incarceration_periods: Optional[List[StateIncarcerationPeriod]],
        supervision_periods: Optional[List[StateSupervisionPeriod]],
        violation_responses: Optional[List[StateSupervisionViolationResponse]],
        program_assignments: Optional[List[StateProgramAssignment]],
        supervision_contacts: Optional[List[StateSupervisionContact]],
        incarceration_sentences: Optional[List[StateIncarcerationSentence]],
        supervision_sentences: Optional[List[StateSupervisionSentence]],
        sentences: Optional[List[StateSentence]],
        sentence_status_snapshots: Optional[List[StateSentenceStatusSnapshot]],
        assessments: Optional[List[StateAssessment]],
        persons: Optional[List[StatePerson]],
        state_person_to_state_staff: Optional[List[Dict[str, Any]]],
    ) -> Dict[str, Sequence[NormalizedStateEntity]]:
        """Helper for testing the find_events function on the identifier."""
        all_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            StateIncarcerationPeriod.__name__: incarceration_periods or [],
            StateSupervisionPeriod.__name__: supervision_periods or [],
            StateSupervisionViolationResponse.__name__: violation_responses or [],
            StateProgramAssignment.__name__: program_assignments or [],
            StateIncarcerationSentence.__name__: incarceration_sentences or [],
            StateSupervisionSentence.__name__: supervision_sentences or [],
            StateSentence.__name__: sentences or [],
            StateSentenceStatusSnapshot.__name__: sentence_status_snapshots or [],
            StateAssessment.__name__: assessments or [],
            StatePerson.__name__: persons or [],
            StateSupervisionContact.__name__: supervision_contacts or [],
            STATE_PERSON_TO_STATE_STAFF_QUERY_NAME: state_person_to_state_staff or [],
        }

        assert self.full_graph_person.person_id is not None

        (
            all_processed_entities,
            additional_attributes_map,
        ) = self.entity_normalizer.normalize_entities(
            self.full_graph_person.person_id, StatePerson, all_kwargs
        )

        normalized_entities: Dict[str, Sequence[NormalizedStateEntity]] = {}

        for entity_name, processed_entities in all_processed_entities.items():
            normalized_entities[
                entity_name
            ] = convert_entity_trees_to_normalized_versions(
                root_entities=processed_entities,
                normalized_entity_class=normalized_entity_class_with_base_class_name(
                    entity_name
                ),
                additional_attributes_map=additional_attributes_map,
            )

        return normalized_entities

    def _normalize_staff_entities_and_convert(
        self,
        staff_role_periods: Optional[List[StateStaffRolePeriod]],
        staff_supervisor_periods: Optional[List[StateStaffSupervisorPeriod]],
    ) -> Dict[str, Sequence[NormalizedStateEntity]]:
        """Helper for testing the find_events function on the identifier."""
        all_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            StateStaffRolePeriod.__name__: staff_role_periods or [],
            StateStaffSupervisorPeriod.__name__: staff_supervisor_periods or [],
        }

        assert self.full_graph_staff.staff_id is not None

        (
            all_processed_entities,
            additional_attributes_map,
        ) = self.entity_normalizer.normalize_entities(
            self.full_graph_staff.staff_id, StateStaff, all_kwargs
        )

        normalized_entities: Dict[str, Sequence[NormalizedStateEntity]] = {}

        for entity_name, processed_entities in all_processed_entities.items():
            normalized_entities[
                entity_name
            ] = convert_entity_trees_to_normalized_versions(
                root_entities=processed_entities,
                normalized_entity_class=normalized_entity_class_with_base_class_name(
                    entity_name
                ),
                additional_attributes_map=additional_attributes_map,
            )

        return normalized_entities

    def test_normalize_person_entities(self) -> None:
        normalized_ips: List[NormalizedStateIncarcerationPeriod] = []

        for index, ip in enumerate(self.full_graph_person.incarceration_periods):
            normalized_ip = NormalizedStateIncarcerationPeriod(
                **{
                    **ip.__dict__,
                    **{
                        "specialized_purpose_for_incarceration": StateSpecializedPurposeForIncarceration.GENERAL,
                        "sequence_num": index,
                    },
                }
            )

            normalized_ips.append(normalized_ip)

        normalized_sps: List[NormalizedStateSupervisionPeriod] = []

        for index, sp in enumerate(self.full_graph_person.supervision_periods):
            normalized_case_type_entries: List[
                NormalizedStateSupervisionCaseTypeEntry
            ] = [
                NormalizedStateSupervisionCaseTypeEntry(
                    **{
                        field: value
                        for field, value in cte.__dict__.items()
                        if field != "supervision_period"
                    }
                )
                for cte in sp.case_type_entries
            ]

            sp_copy = deepcopy(sp)
            sp_copy.case_type_entries = []
            normalized_sp = NormalizedStateSupervisionPeriod(
                **{
                    **sp_copy.__dict__,
                    "sequence_num": index,
                    "case_type_entries": normalized_case_type_entries,
                    "supervising_officer_staff_id": 10000,
                },
            )

            normalized_sps.append(normalized_sp)

            for cte in normalized_case_type_entries:
                cte.supervision_period = normalized_sp

        normalized_pas: List[NormalizedStateProgramAssignment] = []

        for index, pa in enumerate(self.full_graph_person.program_assignments):
            normalized_pa = NormalizedStateProgramAssignment(
                **{
                    **pa.__dict__,
                    "sequence_num": index,
                    "referring_staff_id": 10000,
                },
            )

            normalized_pas.append(normalized_pa)

        normalized_scs: List[NormalizedStateSupervisionContact] = []

        for index, sc in enumerate(self.full_graph_person.supervision_contacts):
            normalized_sc = NormalizedStateSupervisionContact(
                **{**sc.__dict__, "contacting_staff_id": 20000}
            )

            normalized_scs.append(normalized_sc)

        normalized_entities = self._normalize_person_entities_and_convert(
            persons=[self.full_graph_person],
            incarceration_periods=self.full_graph_person.incarceration_periods,
            supervision_periods=self.full_graph_person.supervision_periods,
            violation_responses=get_violation_tree().supervision_violation_responses,
            program_assignments=self.full_graph_person.program_assignments,
            supervision_contacts=self.full_graph_person.supervision_contacts,
            incarceration_sentences=[],
            supervision_sentences=[],
            sentences=[],
            sentence_status_snapshots=[],
            assessments=[],
            state_person_to_state_staff=STATE_PERSON_TO_STATE_STAFF_LIST,
        )

        expected_normalized_entities: Dict[str, Sequence[NormalizedStateEntity]] = {
            StateIncarcerationPeriod.__name__: normalized_ips,
            StateSupervisionPeriod.__name__: normalized_sps,
            StateSupervisionViolation.__name__: [get_normalized_violation_tree()],
            StateProgramAssignment.__name__: normalized_pas,
            StateAssessment.__name__: [],
            StateIncarcerationSentence.__name__: [],
            StateSupervisionSentence.__name__: [],
            StateSupervisionContact.__name__: normalized_scs,
        }

        for entity_name, items in normalized_entities.items():
            self.assertEqual(expected_normalized_entities[entity_name], items)

    def test_normalize_person_entities_missing_entities(self) -> None:
        """Tests that calling normalize_entities will still work even when a person
        doesn't have instances of all of the entities that get normalized."""
        normalized_ips: List[NormalizedStateIncarcerationPeriod] = []

        for index, ip in enumerate(self.full_graph_person.incarceration_periods):
            normalized_ip = NormalizedStateIncarcerationPeriod(
                **{
                    **ip.__dict__,
                    **{
                        "specialized_purpose_for_incarceration": StateSpecializedPurposeForIncarceration.GENERAL,
                        "sequence_num": index,
                    },
                }
            )

            normalized_ips.append(normalized_ip)

        normalized_entities = self._normalize_person_entities_and_convert(
            persons=[self.full_graph_person],
            incarceration_periods=self.full_graph_person.incarceration_periods,
            supervision_periods=[],
            violation_responses=get_violation_tree().supervision_violation_responses,
            program_assignments=[],
            supervision_contacts=[],
            incarceration_sentences=[],
            supervision_sentences=[],
            sentences=[],
            sentence_status_snapshots=[],
            assessments=[],
            state_person_to_state_staff=[],
        )

        expected_normalized_entities: Dict[str, Sequence[NormalizedStateEntity]] = {
            StateIncarcerationPeriod.__name__: normalized_ips,
            StateSupervisionPeriod.__name__: [],
            StateSupervisionViolation.__name__: [get_normalized_violation_tree()],
            StateProgramAssignment.__name__: [],
            StateAssessment.__name__: [],
            StateIncarcerationSentence.__name__: [],
            StateSupervisionSentence.__name__: [],
            StateSupervisionContact.__name__: [],
        }

        for entity_name, entity_list in normalized_entities.items():
            self.assertEqual(expected_normalized_entities[entity_name], entity_list)

    def test_normalize_staff_entities(self) -> None:
        normalized_entities = self._normalize_staff_entities_and_convert(
            staff_role_periods=self.full_graph_staff.role_periods,
            staff_supervisor_periods=self.full_graph_staff.supervisor_periods,
        )

        normalized_role_periods: Sequence[NormalizedStateStaffRolePeriod] = [
            NormalizedStateStaffRolePeriod(
                **{
                    field: value
                    for field, value in role_period.__dict__.items()
                    if field != "staff"
                }
            )
            for role_period in self.full_graph_staff.role_periods
        ]

        expected_normalized_entities: Dict[str, Sequence[NormalizedStateEntity]] = {
            StateStaffRolePeriod.__name__: normalized_role_periods,
        }
        for entity_name, entity_list in normalized_entities.items():
            self.assertEqual(expected_normalized_entities[entity_name], entity_list)
