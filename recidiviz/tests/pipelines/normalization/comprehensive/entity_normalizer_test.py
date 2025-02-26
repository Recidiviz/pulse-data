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

from recidiviz.calculator.query.state.views.reference.state_charge_offense_description_to_labels import (
    STATE_CHARGE_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.state_person_to_state_staff import (
    STATE_PERSON_TO_STATE_STAFF_VIEW_NAME,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
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
    StateStaff,
    StateStaffRolePeriod,
    StateSupervisionContact,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolation,
    StateSupervisionViolationResponse,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStateEntity,
    NormalizedStateIncarcerationPeriod,
    NormalizedStateProgramAssignment,
    NormalizedStateStaffRolePeriod,
    NormalizedStateSupervisionCaseTypeEntry,
    NormalizedStateSupervisionContact,
    NormalizedStateSupervisionPeriod,
)
from recidiviz.pipelines.normalization.comprehensive import entity_normalizer, pipeline
from recidiviz.pipelines.normalization.comprehensive.entity_normalizer import (
    EntityNormalizerResult,
)
from recidiviz.pipelines.normalization.utils.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)
from recidiviz.pipelines.utils.execution_utils import TableRow
from recidiviz.pipelines.utils.state_utils.state_calculation_config_manager import (
    get_required_state_specific_delegates,
)
from recidiviz.pipelines.utils.state_utils.state_specific_delegate import (
    StateSpecificDelegate,
)
from recidiviz.tests.persistence.entity.normalized_entities_utils_test import (
    get_normalized_violation_tree,
    get_violation_tree,
)
from recidiviz.tests.persistence.entity.state.entities_test_utils import (
    generate_full_graph_state_person,
    generate_full_graph_state_staff,
)
from recidiviz.tests.pipelines.normalization.utils.entity_normalization_manager_utils_test import (
    STATE_PERSON_TO_STATE_STAFF_LIST,
)
from recidiviz.tests.pipelines.utils.state_utils.state_calculation_config_manager_test import (
    STATE_DELEGATES_FOR_TESTS,
)

_STATE_CODE = "US_XX"


# TODO(#21376) Properly refactor once strategy for separate normalization is defined.
class TestNormalizeEntities(unittest.TestCase):
    """Tests the normalize_entities function on the ComprehensiveEntityNormalizer."""

    def setUp(self) -> None:
        self.pipeline = pipeline.ComprehensiveNormalizationPipeline
        self.entity_normalizer = entity_normalizer.ComprehensiveEntityNormalizer()

        self.full_graph_person = generate_full_graph_state_person(
            set_back_edges=True, include_person_back_edges=False, set_ids=True
        )
        self.full_graph_staff = generate_full_graph_state_staff(
            set_back_edges=True, set_ids=True
        )

        self.violation_responses: List[StateSupervisionViolationResponse] = []
        for v in self.full_graph_person.supervision_violations:
            self.violation_responses.extend(v.supervision_violation_responses)

    def _run_normalize_person_entities(
        self,
        incarceration_periods: Optional[List[StateIncarcerationPeriod]] = None,
        supervision_periods: Optional[List[StateSupervisionPeriod]] = None,
        violation_responses: Optional[List[StateSupervisionViolationResponse]] = None,
        program_assignments: Optional[List[StateProgramAssignment]] = None,
        incarceration_sentences: Optional[List[StateIncarcerationSentence]] = None,
        supervision_sentences: Optional[List[StateSupervisionSentence]] = None,
        assessments: Optional[List[StateAssessment]] = None,
        persons: Optional[List[StatePerson]] = None,
        supervision_contacts: Optional[List[StateSupervisionContact]] = None,
        charge_offense_descriptions_to_labels: Optional[List[Dict[str, Any]]] = None,
        state_code_override: Optional[str] = None,
        state_person_to_state_staff: Optional[List[Dict[str, Any]]] = None,
    ) -> EntityNormalizerResult:
        """Helper for testing the normalize_entities function on the
        ComprehensiveEntityNormalizer for StatePerson."""
        entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            StateIncarcerationPeriod.__name__: incarceration_periods or [],
            StateSupervisionPeriod.__name__: supervision_periods or [],
            StateSupervisionViolationResponse.__name__: violation_responses or [],
            StateProgramAssignment.__name__: program_assignments or [],
            StateIncarcerationSentence.__name__: incarceration_sentences or [],
            StateSupervisionSentence.__name__: supervision_sentences or [],
            StateAssessment.__name__: assessments or [],
            StatePerson.__name__: persons or [],
            StateSupervisionContact.__name__: supervision_contacts or [],
            STATE_CHARGE_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_NAME: charge_offense_descriptions_to_labels
            or [],
            STATE_PERSON_TO_STATE_STAFF_VIEW_NAME: state_person_to_state_staff or [],
        }
        if not state_code_override:
            required_delegates = STATE_DELEGATES_FOR_TESTS
        else:
            required_delegates = get_required_state_specific_delegates(
                state_code=(state_code_override or _STATE_CODE),
                required_delegates=self.pipeline.state_specific_required_delegates().get(
                    StatePerson, []
                ),
                entity_kwargs=entity_kwargs,
            )

        all_kwargs: Dict[
            str, Union[Sequence[Entity], List[TableRow], StateSpecificDelegate]
        ] = {**required_delegates, **entity_kwargs}

        assert self.full_graph_person.person_id is not None

        return self.entity_normalizer.normalize_entities(
            self.full_graph_person.person_id, StatePerson, all_kwargs
        )

    def _run_normalize_staff_entities(
        self,
        staff_role_periods: Optional[List[StateStaffRolePeriod]],
        state_code_override: Optional[str] = None,
    ) -> EntityNormalizerResult:
        """Helper for testing the normalize_entities function on the ComprehensiveEntityNormalizer
        for StateStaff."""
        entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            StateStaffRolePeriod.__name__: staff_role_periods or []
        }
        if not state_code_override:
            required_delegates = STATE_DELEGATES_FOR_TESTS
        else:
            required_delegates = get_required_state_specific_delegates(
                state_code=(state_code_override or _STATE_CODE),
                required_delegates=self.pipeline.state_specific_required_delegates().get(
                    StateStaff, []
                ),
                entity_kwargs=entity_kwargs,
            )

        all_kwargs: Dict[
            str, Union[Sequence[Entity], List[TableRow], StateSpecificDelegate]
        ] = {**required_delegates, **entity_kwargs}

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
                        "uccs_code_uniform": 3160,
                        "uccs_description_uniform": "Possession/Use of Unspecified Drug",
                        "uccs_category_uniform": "Possession/Use of Unspecified Drug",
                        "ncic_code_uniform": "3599",
                        "ncic_description_uniform": "Dangerous Drugs (describe offense)",
                        "ncic_category_uniform": "Dangerous Drugs",
                        "nibrs_code_uniform": "35A",
                        "nibrs_description_uniform": "Drug/Narcotic",
                        "nibrs_category_uniform": "Drug/Narcotic",
                        "crime_against_uniform": "Society",
                        "is_drug_uniform": True,
                        "is_violent_uniform": False,
                        "is_sex_offense_uniform": None,
                        "offense_completed_uniform": True,
                        "offense_attempted_uniform": False,
                        "offense_conspired_uniform": False,
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
                        "uccs_code_uniform": 3160,
                        "uccs_description_uniform": "Possession/Use of Unspecified Drug",
                        "uccs_category_uniform": "Possession/Use of Unspecified Drug",
                        "ncic_code_uniform": "3599",
                        "ncic_description_uniform": "Dangerous Drugs (describe offense)",
                        "ncic_category_uniform": "Dangerous Drugs",
                        "nibrs_code_uniform": "35A",
                        "nibrs_description_uniform": "Drug/Narcotic",
                        "nibrs_category_uniform": "Drug/Narcotic",
                        "crime_against_uniform": "Society",
                        "is_drug_uniform": True,
                        "is_violent_uniform": False,
                        "is_sex_offense_uniform": None,
                        "offense_completed_uniform": True,
                        "offense_attempted_uniform": False,
                        "offense_conspired_uniform": False,
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
            incarceration_periods=self.full_graph_person.incarceration_periods,
            supervision_periods=self.full_graph_person.supervision_periods,
            violation_responses=violation_responses,
            program_assignments=self.full_graph_person.program_assignments,
            assessments=self.full_graph_person.assessments,
            supervision_contacts=self.full_graph_person.supervision_contacts,
            incarceration_sentences=self.full_graph_person.incarceration_sentences,
            supervision_sentences=self.full_graph_person.supervision_sentences,
            charge_offense_descriptions_to_labels=[
                {
                    "person_id": self.full_graph_person.person_id,
                    "charge_id": charge.charge_id,
                    "state_code": "US_XX",
                    "offense_description": "DRUG POSSESSION",
                    "probability": 0.993368719,
                    "uccs_code": 3160,
                    "uccs_description": "Possession/Use of Unspecified Drug",
                    "uccs_category": "Possession/Use of Unspecified Drug",
                    "ncic_code": "3599",
                    "ncic_description": "Dangerous Drugs (describe offense)",
                    "ncic_category": "Dangerous Drugs",
                    "nibrs_code": "35A",
                    "nibrs_description": "Drug/Narcotic",
                    "nibrs_category": "Drug/Narcotic",
                    "crime_against": "Society",
                    "is_drug": True,
                    "is_violent": False,
                    "offense_completed": True,
                    "offense_attempted": False,
                    "offense_conspired": False,
                }
                for incarceration_sentence in self.full_graph_person.incarceration_sentences
                for charge in incarceration_sentence.charges
                if charge.charge_id
            ]
            + [
                {
                    "person_id": self.full_graph_person.person_id,
                    "charge_id": charge.charge_id,
                    "state_code": "US_XX",
                    "offense_description": "DRUG POSSESSION",
                    "probability": 0.993368719,
                    "uccs_code": 3160,
                    "uccs_description": "Possession/Use of Unspecified Drug",
                    "uccs_category": "Possession/Use of Unspecified Drug",
                    "ncic_code": "3599",
                    "ncic_description": "Dangerous Drugs (describe offense)",
                    "ncic_category": "Dangerous Drugs",
                    "nibrs_code": "35A",
                    "nibrs_description": "Drug/Narcotic",
                    "nibrs_category": "Drug/Narcotic",
                    "crime_against": "Society",
                    "is_drug": True,
                    "is_violent": False,
                    "offense_completed": True,
                    "offense_attempted": False,
                    "offense_conspired": False,
                }
                for supervision_sentence in self.full_graph_person.supervision_sentences
                for charge in supervision_sentence.charges
                if charge.charge_id
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
            incarceration_periods=self.full_graph_person.incarceration_periods,
            supervision_periods=None,
            violation_responses=get_violation_tree().supervision_violation_responses,
            program_assignments=None,
        )

    def test_normalize_staff_entities(self) -> None:
        expected_output_entities = {
            StateStaffRolePeriod.__name__: self.full_graph_staff.role_periods,
        }
        (
            normalized_entities,
            additional_attributes_map,
        ) = self._run_normalize_staff_entities(self.full_graph_staff.role_periods)

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
        self.pipeline = pipeline.ComprehensiveNormalizationPipeline
        self.entity_normalizer = entity_normalizer.ComprehensiveEntityNormalizer()

        self.full_graph_person = generate_full_graph_state_person(
            set_back_edges=True, include_person_back_edges=False, set_ids=True
        )

        self.full_graph_staff = generate_full_graph_state_staff(
            set_back_edges=True, set_ids=True
        )

        self.violation_responses: List[StateSupervisionViolationResponse] = []
        for v in self.full_graph_person.supervision_violations:
            self.violation_responses.extend(v.supervision_violation_responses)

        self.field_index = CoreEntityFieldIndex()

    def _normalize_person_entities_and_convert(
        self,
        incarceration_periods: Optional[List[StateIncarcerationPeriod]] = None,
        supervision_periods: Optional[List[StateSupervisionPeriod]] = None,
        violation_responses: Optional[List[StateSupervisionViolationResponse]] = None,
        program_assignments: Optional[List[StateProgramAssignment]] = None,
        supervision_contacts: Optional[List[StateSupervisionContact]] = None,
        incarceration_sentences: Optional[List[StateIncarcerationSentence]] = None,
        supervision_sentences: Optional[List[StateSupervisionSentence]] = None,
        assessments: Optional[List[StateAssessment]] = None,
        persons: Optional[List[StatePerson]] = None,
        state_code_override: Optional[str] = None,
        charge_offense_descriptions_to_labels: Optional[List[Dict[str, Any]]] = None,
        state_person_to_state_staff: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Sequence[NormalizedStateEntity]]:
        """Helper for testing the find_events function on the identifier."""
        entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            StateIncarcerationPeriod.__name__: incarceration_periods or [],
            StateSupervisionPeriod.__name__: supervision_periods or [],
            StateSupervisionViolationResponse.__name__: violation_responses or [],
            StateProgramAssignment.__name__: program_assignments or [],
            StateIncarcerationSentence.__name__: incarceration_sentences or [],
            StateSupervisionSentence.__name__: supervision_sentences or [],
            StateAssessment.__name__: assessments or [],
            StatePerson.__name__: persons or [],
            StateSupervisionContact.__name__: supervision_contacts or [],
            STATE_CHARGE_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_NAME: charge_offense_descriptions_to_labels
            or [],
            STATE_PERSON_TO_STATE_STAFF_VIEW_NAME: state_person_to_state_staff or [],
        }

        if not state_code_override:
            required_delegates = STATE_DELEGATES_FOR_TESTS
        else:
            required_delegates = get_required_state_specific_delegates(
                state_code=(state_code_override or _STATE_CODE),
                required_delegates=self.pipeline.state_specific_required_delegates().get(
                    StatePerson, []
                ),
                entity_kwargs=entity_kwargs,
            )

        all_kwargs: Dict[
            str, Union[Sequence[Entity], List[TableRow], StateSpecificDelegate]
        ] = {**required_delegates, **entity_kwargs}

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
                field_index=self.field_index,
            )

        return normalized_entities

    def _normalize_staff_entities_and_convert(
        self,
        staff_role_periods: Optional[List[StateStaffRolePeriod]] = None,
        state_code_override: Optional[str] = None,
    ) -> Dict[str, Sequence[NormalizedStateEntity]]:
        """Helper for testing the find_events function on the identifier."""
        entity_kwargs: Dict[str, Union[Sequence[Entity], List[TableRow]]] = {
            StateStaffRolePeriod.__name__: staff_role_periods or []
        }

        if not state_code_override:
            required_delegates = STATE_DELEGATES_FOR_TESTS
        else:
            required_delegates = get_required_state_specific_delegates(
                state_code=(state_code_override or _STATE_CODE),
                required_delegates=self.pipeline.state_specific_required_delegates().get(
                    StateStaff, []
                ),
                entity_kwargs=entity_kwargs,
            )

        all_kwargs: Dict[
            str, Union[Sequence[Entity], List[TableRow], StateSpecificDelegate]
        ] = {**required_delegates, **entity_kwargs}

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
                field_index=self.field_index,
            )

        return normalized_entities

    def test_normalize_person_entities(self) -> None:
        normalized_ips: List[NormalizedStateIncarcerationPeriod] = []

        for index, ip in enumerate(self.full_graph_person.incarceration_periods):
            normalized_ip = NormalizedStateIncarcerationPeriod.new_with_defaults(
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
            normalized_case_type_entries: Sequence[
                NormalizedStateSupervisionCaseTypeEntry
            ] = [
                NormalizedStateSupervisionCaseTypeEntry.new_with_defaults(
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
            normalized_sp = NormalizedStateSupervisionPeriod.new_with_defaults(
                **{
                    field: value
                    for field, value in sp_copy.__dict__.items()
                    if field not in ("case_type_entries")
                },
                **{
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
            normalized_pa = NormalizedStateProgramAssignment.new_with_defaults(
                **pa.__dict__,
                **{
                    "sequence_num": index,
                    "referring_staff_id": 10000,
                },
            )

            normalized_pas.append(normalized_pa)

        normalized_scs: List[NormalizedStateSupervisionContact] = []

        for index, sc in enumerate(self.full_graph_person.supervision_contacts):
            normalized_sc = NormalizedStateSupervisionContact.new_with_defaults(
                **{**sc.__dict__, "contacting_staff_id": 20000}
            )

            normalized_scs.append(normalized_sc)

        normalized_entities = self._normalize_person_entities_and_convert(
            incarceration_periods=self.full_graph_person.incarceration_periods,
            supervision_periods=self.full_graph_person.supervision_periods,
            violation_responses=get_violation_tree().supervision_violation_responses,
            program_assignments=self.full_graph_person.program_assignments,
            supervision_contacts=self.full_graph_person.supervision_contacts,
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
            normalized_ip = NormalizedStateIncarcerationPeriod.new_with_defaults(
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
            incarceration_periods=self.full_graph_person.incarceration_periods,
            supervision_periods=None,
            violation_responses=get_violation_tree().supervision_violation_responses,
            program_assignments=None,
            supervision_contacts=None,
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
        )

        normalized_role_periods: Sequence[NormalizedStateStaffRolePeriod] = [
            NormalizedStateStaffRolePeriod.new_with_defaults(
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
