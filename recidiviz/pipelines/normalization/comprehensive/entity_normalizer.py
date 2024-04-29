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
from typing import Any, Dict, List, Sequence, Tuple, Type

from recidiviz.calculator.query.state.views.reference.state_charge_offense_description_to_labels import (
    STATE_CHARGE_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_NAME,
)
from recidiviz.calculator.query.state.views.reference.state_person_to_state_staff import (
    STATE_PERSON_TO_STATE_STAFF_VIEW_NAME,
)
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
    NormalizedStateIncarcerationSentence,
    NormalizedStateSupervisionSentence,
)
from recidiviz.pipelines.normalization.utils.entity_normalization_manager_utils import (
    normalized_periods_for_calculations,
    normalized_violation_responses_from_processed_versions,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.assessment_normalization_manager import (
    AssessmentNormalizationManager,
    StateSpecificAssessmentNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.incarceration_period_normalization_manager import (
    StateSpecificIncarcerationNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.program_assignment_normalization_manager import (
    ProgramAssignmentNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.sentence_normalization_manager import (
    SentenceNormalizationManager,
    StateSpecificSentenceNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.staff_role_period_normalization_manager import (
    StaffRolePeriodNormalizationManager,
    StateSpecificStaffRolePeriodNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_contact_normalization_manager import (
    SupervisionContactNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_period_normalization_manager import (
    StateSpecificSupervisionNormalizationDelegate,
)
from recidiviz.pipelines.normalization.utils.normalization_managers.supervision_violation_responses_normalization_manager import (
    StateSpecificViolationResponseNormalizationDelegate,
    ViolationResponseNormalizationManager,
)
from recidiviz.pipelines.normalization.utils.normalized_entity_conversion_utils import (
    convert_entity_trees_to_normalized_versions,
)
from recidiviz.pipelines.utils.execution_utils import (
    build_staff_external_id_to_staff_id_map,
)

EntityNormalizerContext = Dict[str, Any]

EntityNormalizerResult = Tuple[Dict[str, Sequence[Entity]], AdditionalAttributesMap]


class ComprehensiveEntityNormalizer:
    """Entity normalizer class for the normalization pipeline that normalizes all
    entities with configured normalization processes."""

    def __init__(self) -> None:
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
                staff_id=root_entity_id,
                staff_role_periods=normalizer_args[StateStaffRolePeriod.__name__],
                staff_role_period_normalization_delegate=normalizer_args[
                    StateSpecificStaffRolePeriodNormalizationDelegate.__name__
                ],
            )
        return self._normalize_person_entities(
            person_id=root_entity_id,
            ip_normalization_delegate=normalizer_args[
                StateSpecificIncarcerationNormalizationDelegate.__name__
            ],
            sp_normalization_delegate=normalizer_args[
                StateSpecificSupervisionNormalizationDelegate.__name__
            ],
            violation_response_normalization_delegate=normalizer_args[
                StateSpecificViolationResponseNormalizationDelegate.__name__
            ],
            assessment_normalization_delegate=normalizer_args[
                StateSpecificAssessmentNormalizationDelegate.__name__
            ],
            sentence_normalization_delegate=normalizer_args[
                StateSpecificSentenceNormalizationDelegate.__name__
            ],
            incarceration_periods=normalizer_args[StateIncarcerationPeriod.__name__],
            incarceration_sentences=normalizer_args[
                StateIncarcerationSentence.__name__
            ],
            supervision_sentences=normalizer_args[StateSupervisionSentence.__name__],
            supervision_periods=normalizer_args[StateSupervisionPeriod.__name__],
            violation_responses=normalizer_args[
                StateSupervisionViolationResponse.__name__
            ],
            program_assignments=normalizer_args[StateProgramAssignment.__name__],
            assessments=normalizer_args[StateAssessment.__name__],
            supervision_contacts=normalizer_args[StateSupervisionContact.__name__],
            charge_offense_descriptions_to_labels=normalizer_args[
                STATE_CHARGE_OFFENSE_DESCRIPTION_TO_LABELS_VIEW_NAME
            ],
            state_person_to_state_staff=normalizer_args[
                STATE_PERSON_TO_STATE_STAFF_VIEW_NAME
            ],
        )

    def _normalize_person_entities(
        self,
        person_id: int,
        ip_normalization_delegate: StateSpecificIncarcerationNormalizationDelegate,
        sp_normalization_delegate: StateSpecificSupervisionNormalizationDelegate,
        violation_response_normalization_delegate: StateSpecificViolationResponseNormalizationDelegate,
        assessment_normalization_delegate: StateSpecificAssessmentNormalizationDelegate,
        sentence_normalization_delegate: StateSpecificSentenceNormalizationDelegate,
        incarceration_periods: List[StateIncarcerationPeriod],
        incarceration_sentences: List[StateIncarcerationSentence],
        supervision_sentences: List[StateSupervisionSentence],
        supervision_periods: List[StateSupervisionPeriod],
        violation_responses: List[StateSupervisionViolationResponse],
        program_assignments: List[StateProgramAssignment],
        assessments: List[StateAssessment],
        supervision_contacts: List[StateSupervisionContact],
        charge_offense_descriptions_to_labels: List[Dict[str, Any]],
        state_person_to_state_staff: List[Dict[str, Any]],
    ) -> EntityNormalizerResult:
        """Normalizes all entities rooted with StatePerson with corresponding normalization managers."""

        staff_external_id_to_staff_id: Dict[
            Tuple[str, str], int
        ] = build_staff_external_id_to_staff_id_map(state_person_to_state_staff)
        processed_entities = all_normalized_person_entities(
            person_id=person_id,
            ip_normalization_delegate=ip_normalization_delegate,
            sp_normalization_delegate=sp_normalization_delegate,
            violation_response_normalization_delegate=violation_response_normalization_delegate,
            assessment_normalization_delegate=assessment_normalization_delegate,
            sentence_normalization_delegate=sentence_normalization_delegate,
            incarceration_periods=incarceration_periods,
            supervision_periods=supervision_periods,
            violation_responses=violation_responses,
            program_assignments=program_assignments,
            incarceration_sentences=incarceration_sentences,
            supervision_sentences=supervision_sentences,
            assessments=assessments,
            supervision_contacts=supervision_contacts,
            charge_offense_descriptions_to_labels=charge_offense_descriptions_to_labels,
            staff_external_id_to_staff_id=staff_external_id_to_staff_id,
            field_index=self.field_index,
        )

        return processed_entities

    # pylint: disable=unused-argument
    def _normalize_staff_entities(
        self,
        staff_id: int,
        staff_role_periods: List[StateStaffRolePeriod],
        staff_role_period_normalization_delegate: StateSpecificStaffRolePeriodNormalizationDelegate,
    ) -> EntityNormalizerResult:
        """Normalizes all entities rooted with StateStaff with corresponding normalization managers."""
        staff_role_period_normalization_manager = StaffRolePeriodNormalizationManager(
            staff_role_periods, staff_role_period_normalization_delegate
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
    person_id: int,
    ip_normalization_delegate: StateSpecificIncarcerationNormalizationDelegate,
    sp_normalization_delegate: StateSpecificSupervisionNormalizationDelegate,
    violation_response_normalization_delegate: StateSpecificViolationResponseNormalizationDelegate,
    assessment_normalization_delegate: StateSpecificAssessmentNormalizationDelegate,
    sentence_normalization_delegate: StateSpecificSentenceNormalizationDelegate,
    incarceration_periods: List[StateIncarcerationPeriod],
    supervision_periods: List[StateSupervisionPeriod],
    violation_responses: List[StateSupervisionViolationResponse],
    program_assignments: List[StateProgramAssignment],
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    assessments: List[StateAssessment],
    supervision_contacts: List[StateSupervisionContact],
    charge_offense_descriptions_to_labels: List[Dict[str, Any]],
    staff_external_id_to_staff_id: Dict[Tuple[str, str], int],
    field_index: CoreEntityFieldIndex,
) -> EntityNormalizerResult:
    """Normalizes all entities that have corresponding comprehensive managers.

    Returns a dictionary mapping the entity class name to the list of normalized
    entities.
    """
    violation_response_manager = ViolationResponseNormalizationManager(
        person_id=person_id,
        delegate=violation_response_normalization_delegate,
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
        charge_offense_descriptions_to_labels,
        sentence_normalization_delegate,
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
        person_id=person_id,
        ip_normalization_delegate=ip_normalization_delegate,
        sp_normalization_delegate=sp_normalization_delegate,
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
        assessment_normalization_delegate,
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
