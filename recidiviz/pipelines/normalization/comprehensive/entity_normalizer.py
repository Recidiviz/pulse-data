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
"""Entity normalizer for normalizing all entities with configured normalization
processes."""
from typing import Any, Dict, List, Optional, Sequence, Tuple, Type

from more_itertools import one

from recidiviz.calculator.query.state.views.reference.state_person_to_state_staff import (
    STATE_PERSON_TO_STATE_STAFF_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.us_mo_sentence_statuses import (
    US_MO_SENTENCE_STATUSES_VIEW_NAME,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.normalized_entities_utils import (
    AdditionalAttributesMap,
    merge_additional_attributes_maps,
)
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StateProgramAssignment,
    StateSentence,
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
    NormalizedStateIncarcerationSentence,
    NormalizedStateSupervisionSentence,
)
from recidiviz.pipelines.normalization.utils.entity_normalization_manager_utils import (
    normalized_periods_for_calculations,
    normalized_violation_responses_from_processed_versions,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.assessment_normalization_manager import (
    AssessmentNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.program_assignment_normalization_manager import (
    ProgramAssignmentNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.sentence_normalization_manager import (
    SentenceNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.staff_role_period_normalization_manager import (
    StaffRolePeriodNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_contact_normalization_manager import (
    SupervisionContactNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_violation_responses_normalization_manager import (
    ViolationResponseNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)
from recidiviz.pipelines.utils.execution_utils import (
    build_staff_external_id_to_staff_id_map,
)
from recidiviz.pipelines.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_assessment_normalization_delegate,
    get_state_specific_incarceration_period_normalization_delegate,
    get_state_specific_sentence_normalization_delegate,
    get_state_specific_staff_role_period_normalization_delegate,
    get_state_specific_supervision_period_normalization_delegate,
    get_state_specific_violation_response_normalization_delegate,
)
from recidiviz.utils.types import assert_type

EntityNormalizerContext = Dict[str, Any]

EntityNormalizerResult = Tuple[Dict[str, Sequence[Entity]], AdditionalAttributesMap]


class ComprehensiveEntityNormalizer:
    """Entity normalizer class for the normalization pipeline that normalizes all
    entities with configured normalization processes."""

    def __init__(self, state_code: StateCode) -> None:
        # Store as a string to avoid issues with pickling
        self.state_code_str = state_code.value
        self.field_index = CoreEntityFieldIndex()

    def normalize_entities(
        self,
        root_entity_id: int,
        root_entity_type: Type[Entity],
        normalizer_args: EntityNormalizerContext,
    ) -> EntityNormalizerResult:
        """Normalizes all entities with corresponding normalization managers.

        Note: does not normalize all of the entities that are required by
        normalization.

        Returns a dictionary mapping the entity class name to the list of normalized
        entities, as well as a map of additional attributes that should be persisted
        to the normalized entity tables.
        """
        # TODO(#21376) Properly refactor once strategy for separate normalization is defined.
        if root_entity_type == StateStaff:
            return self._normalize_staff_entities(
                staff_role_periods=normalizer_args[StateStaffRolePeriod.__name__],
                staff_supervisor_periods=normalizer_args[
                    StateStaffSupervisorPeriod.__name__
                ],
            )

        if US_MO_SENTENCE_STATUSES_VIEW_NAME in normalizer_args:
            us_mo_sentence_statuses_list = [
                a
                for a in normalizer_args[US_MO_SENTENCE_STATUSES_VIEW_NAME]
                if isinstance(a, dict)
            ]
        else:
            us_mo_sentence_statuses_list = None

        person = one(normalizer_args[StatePerson.__name__])
        if person.person_id != root_entity_id:
            raise ValueError(
                f"Found root_entity_id [{root_entity_id}] which does not match the "
                f"person_id [{person.person_id}]"
            )

        return self._normalize_person_entities(
            person=person,
            incarceration_periods=normalizer_args[StateIncarcerationPeriod.__name__],
            incarceration_sentences=normalizer_args[
                StateIncarcerationSentence.__name__
            ],
            supervision_sentences=normalizer_args[StateSupervisionSentence.__name__],
            sentences=normalizer_args[StateSentence.__name__],
            supervision_periods=normalizer_args[StateSupervisionPeriod.__name__],
            violation_responses=normalizer_args[
                StateSupervisionViolationResponse.__name__
            ],
            program_assignments=normalizer_args[StateProgramAssignment.__name__],
            assessments=normalizer_args[StateAssessment.__name__],
            supervision_contacts=normalizer_args[StateSupervisionContact.__name__],
            state_person_to_state_staff=normalizer_args[
                STATE_PERSON_TO_STATE_STAFF_VIEW_NAME
            ],
            us_mo_sentence_statuses_list=us_mo_sentence_statuses_list,
        )

    def _normalize_person_entities(
        self,
        person: StatePerson,
        incarceration_periods: List[StateIncarcerationPeriod],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        # TODO(#30199): Actually use these sentences instead of
        #  us_mo_sentence_statuses_list and remove the pylint exemption.
        sentences: List[StateSentence],  # pylint: disable=unused-argument
        supervision_periods: List[StateSupervisionPeriod],
        violation_responses: List[StateSupervisionViolationResponse],
        program_assignments: List[StateProgramAssignment],
        assessments: List[StateAssessment],
        supervision_contacts: List[StateSupervisionContact],
        state_person_to_state_staff: List[Dict[str, Any]],
        # TODO(#30199): Remove MO sentence statuses table dependency in favor of
        #  state_sentence_status_snapshot data
        us_mo_sentence_statuses_list: Optional[List[Dict[str, Any]]],
    ) -> EntityNormalizerResult:
        """Normalizes all entities rooted with StatePerson with corresponding normalization managers."""

        staff_external_id_to_staff_id: Dict[
            Tuple[str, str], int
        ] = build_staff_external_id_to_staff_id_map(state_person_to_state_staff)
        processed_entities = all_normalized_person_entities(
            state_code=StateCode(self.state_code_str),
            person=person,
            incarceration_periods=incarceration_periods,
            supervision_periods=supervision_periods,
            violation_responses=violation_responses,
            program_assignments=program_assignments,
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences,
            assessments=assessments,
            supervision_contacts=supervision_contacts,
            staff_external_id_to_staff_id=staff_external_id_to_staff_id,
            us_mo_sentence_statuses_list=us_mo_sentence_statuses_list,
            field_index=self.field_index,
        )

        return processed_entities

    def _normalize_staff_entities(
        self,
        staff_role_periods: List[StateStaffRolePeriod],
        staff_supervisor_periods: List[StateStaffSupervisorPeriod],
    ) -> EntityNormalizerResult:
        """Normalizes all entities rooted with StateStaff with corresponding normalization managers."""
        staff_role_period_normalization_manager = StaffRolePeriodNormalizationManager(
            staff_role_periods,
            get_state_specific_staff_role_period_normalization_delegate(
                self.state_code_str, staff_supervisor_periods
            ),
        )
        (
            processed_staff_role_periods,
            additional_staff_role_period_attributes,
        ) = (
            staff_role_period_normalization_manager.normalized_staff_role_periods_and_additional_attributes()
        )

        return (
            {StateStaffRolePeriod.__name__: processed_staff_role_periods},
            additional_staff_role_period_attributes,
        )


def all_normalized_person_entities(
    state_code: StateCode,
    person: StatePerson,
    incarceration_periods: List[StateIncarcerationPeriod],
    supervision_periods: List[StateSupervisionPeriod],
    violation_responses: List[StateSupervisionViolationResponse],
    program_assignments: List[StateProgramAssignment],
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    assessments: List[StateAssessment],
    supervision_contacts: List[StateSupervisionContact],
    staff_external_id_to_staff_id: Dict[Tuple[str, str], int],
    # TODO(#30199): Remove MO sentence statuses table dependency in favor of
    #  state_sentence_status_snapshot data
    us_mo_sentence_statuses_list: Optional[List[Dict[str, Any]]],
    field_index: CoreEntityFieldIndex,
) -> EntityNormalizerResult:
    """Normalizes all entities that have corresponding comprehensive managers.

    Returns a dictionary mapping the entity class name to the list of normalized
    entities.
    """
    violation_response_manager = ViolationResponseNormalizationManager(
        person_id=assert_type(person.person_id, int),
        delegate=get_state_specific_violation_response_normalization_delegate(
            state_code.value, incarceration_periods
        ),
        violation_responses=violation_responses,
        staff_external_id_to_staff_id=staff_external_id_to_staff_id,
    )

    (
        processed_violation_responses,
        additional_vr_attributes,
    ) = violation_response_manager.normalized_violation_responses_for_calculations()

    normalized_violation_responses = (
        normalized_violation_responses_from_processed_versions(
            processed_violation_responses=processed_violation_responses,
            additional_vr_attributes=additional_vr_attributes,
            field_index=field_index,
        )
    )

    sentence_normalization_manager = SentenceNormalizationManager(
        incarceration_sentences,
        supervision_sentences,
        get_state_specific_sentence_normalization_delegate(state_code.value),
    )
    (
        processed_incarceration_sentences,
        additional_incarceration_sentence_attributes,
    ) = (
        sentence_normalization_manager.normalized_incarceration_sentences_and_additional_attributes()
    )

    (
        processed_supervision_sentences,
        additional_supervision_sentence_attributes,
    ) = (
        sentence_normalization_manager.normalized_supervision_sentences_and_additional_attributes()
    )

    normalized_incarceration_sentences = convert_entity_trees_to_normalized_versions(
        processed_incarceration_sentences,
        NormalizedStateIncarcerationSentence,
        additional_incarceration_sentence_attributes,
        field_index,
    )

    normalized_supervision_sentences = convert_entity_trees_to_normalized_versions(
        processed_supervision_sentences,
        NormalizedStateSupervisionSentence,
        additional_supervision_sentence_attributes,
        field_index,
    )

    program_assignment_manager = ProgramAssignmentNormalizationManager(
        program_assignments,
        staff_external_id_to_staff_id,
    )

    (
        processed_program_assignments,
        additional_pa_attributes,
    ) = (
        program_assignment_manager.normalized_program_assignments_and_additional_attributes()
    )

    supervision_contact_manager = SupervisionContactNormalizationManager(
        supervision_contacts,
        staff_external_id_to_staff_id,
    )

    (
        processed_supervision_contacts,
        additional_sc_attributes,
    ) = (
        supervision_contact_manager.normalized_supervision_contacts_and_additional_attributes()
    )

    (
        (processed_incarceration_periods, additional_ip_attributes),
        (processed_supervision_periods, additional_sp_attributes),
    ) = normalized_periods_for_calculations(
        person_id=assert_type(person.person_id, int),
        ip_normalization_delegate=get_state_specific_incarceration_period_normalization_delegate(
            state_code.value
        ),
        sp_normalization_delegate=get_state_specific_supervision_period_normalization_delegate(
            state_code.value,
            assessments,
            incarceration_periods,
            us_mo_sentence_statuses_list,
        ),
        incarceration_periods=incarceration_periods,
        supervision_periods=supervision_periods,
        normalized_violation_responses=normalized_violation_responses,
        field_index=field_index,
        incarceration_sentences=normalized_incarceration_sentences,
        supervision_sentences=normalized_supervision_sentences,
        staff_external_id_to_staff_id=staff_external_id_to_staff_id,
    )

    assessment_normalization_manager = AssessmentNormalizationManager(
        assessments,
        get_state_specific_assessment_normalization_delegate(state_code.value, person),
        staff_external_id_to_staff_id,
    )

    (
        processed_assessments,
        additional_assessments_attributes,
    ) = (
        assessment_normalization_manager.normalized_assessments_and_additional_attributes()
    )

    additional_attributes_map = merge_additional_attributes_maps(
        # We don't expect any overlapping entity types in this list, but we just merge
        # so that we can return one unified map.
        additional_attributes_maps=[
            additional_ip_attributes,
            additional_sp_attributes,
            additional_pa_attributes,
            additional_vr_attributes,
            additional_assessments_attributes,
            additional_sc_attributes,
            additional_incarceration_sentence_attributes,
            additional_supervision_sentence_attributes,
        ]
    )

    distinct_processed_violations: List[StateSupervisionViolation] = []

    # We return the StateSupervisionViolation entities as the top-level entity being
    # normalized, since they are the root of the entity tree that contains all
    # violation information. This allows us to not have to de-duplicate
    # NormalizedStateSupervisionViolation entities, which would happen if we sent in
    # the StateSupervisionViolationResponse entities as the top-level entity,
    # since multiple responses can hang off of the same StateSupervisionViolation.
    for response in processed_violation_responses:
        if (
            response.supervision_violation
            and response.supervision_violation not in distinct_processed_violations
        ):
            distinct_processed_violations.append(response.supervision_violation)

    return (
        {
            StateIncarcerationPeriod.__name__: processed_incarceration_periods,
            StateSupervisionPeriod.__name__: processed_supervision_periods,
            StateSupervisionViolation.__name__: distinct_processed_violations,
            StateProgramAssignment.__name__: processed_program_assignments,
            StateAssessment.__name__: processed_assessments,
            StateSupervisionContact.__name__: processed_supervision_contacts,
            StateIncarcerationSentence.__name__: processed_incarceration_sentences,
            StateSupervisionSentence.__name__: processed_supervision_sentences,
        },
        additional_attributes_map,
    )
