# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Functionality for normalizing the given StatePerson root entity into a
NormalizedStatePerson.
"""
import datetime
from typing import Mapping, Sequence

from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.base_entity import Entity
from recidiviz.persistence.entity.state.entities import (
    StatePerson,
    StateSupervisionViolationResponse,
)
from recidiviz.persistence.entity.state.normalized_entities import (
    NormalizedStatePerson,
    NormalizedStatePersonStaffRelationshipPeriod,
    NormalizedStateSupervisionViolationResponse,
)
from recidiviz.persistence.entity.state.normalized_state_entity import (
    NormalizedStateEntity,
)
from recidiviz.persistence.entity.state.violation_utils import (
    collect_violation_responses,
)
from recidiviz.pipelines.ingest.state.create_root_entity_id_to_staff_id_mapping import (
    StaffExternalIdToIdMap,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.assessment_normalization_manager import (
    AssessmentNormalizationManager,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.incarceration_period_normalization_manager import (
    IncarcerationPeriodNormalizationManager,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.program_assignment_normalization_manager import (
    ProgramAssignmentNormalizationManager,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.sentence_normalization_manager import (
    SentenceNormalizationManager,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.supervision_contact_normalization_manager import (
    SupervisionContactNormalizationManager,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.supervision_period_normalization_manager import (
    SupervisionPeriodNormalizationManager,
)
from recidiviz.pipelines.ingest.state.normalization.normalization_managers.supervision_violation_responses_normalization_manager import (
    ViolationResponseNormalizationManager,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_person_external_ids import (
    get_normalized_person_external_ids,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_person_staff_relationship_periods import (
    get_normalized_person_staff_relationship_periods,
)
from recidiviz.pipelines.ingest.state.normalization.normalize_root_entity_helpers import (
    build_normalized_root_entity,
)
from recidiviz.pipelines.ingest.state.normalization.sentencing.normalize_all_sentencing_entities import (
    get_normalized_sentencing_entities,
)
from recidiviz.pipelines.utils.entity_normalization.normalized_supervision_period_index import (
    NormalizedSupervisionPeriodIndex,
)
from recidiviz.pipelines.utils.period_utils import (
    find_earliest_date_of_period_ending_in_death,
)
from recidiviz.pipelines.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_assessment_normalization_delegate,
    get_state_specific_incarceration_period_normalization_delegate,
    get_state_specific_normalization_delegate,
    get_state_specific_sentence_normalization_delegate,
    get_state_specific_supervision_period_normalization_delegate,
    get_state_specific_violation_response_normalization_delegate,
)
from recidiviz.utils.types import assert_type


def build_normalized_state_person(
    person: StatePerson,
    staff_external_id_to_staff_id: StaffExternalIdToIdMap,
    expected_output_entities: set[type[Entity]],
) -> NormalizedStatePerson:
    """Normalizes the given StatePerson root entity into a NormalizedStatePerson."""
    person_id = assert_type(person.person_id, int)
    state_code = StateCode(person.state_code)

    normalized_person_external_ids = get_normalized_person_external_ids(
        state_code=state_code,
        person_id=person_id,
        external_ids=person.external_ids,
        delegate=get_state_specific_normalization_delegate(state_code.value),
    )

    normalized_violations = ViolationResponseNormalizationManager(
        person_id=person_id,
        delegate=get_state_specific_violation_response_normalization_delegate(
            state_code.value, person.incarceration_periods
        ),
        violation_responses=collect_violation_responses(
            person.supervision_violations, StateSupervisionViolationResponse
        ),
        staff_external_id_to_staff_id=staff_external_id_to_staff_id,
    ).get_normalized_violations()

    sentencing_delegate = get_state_specific_sentence_normalization_delegate(
        state_code.value
    )
    (
        normalized_incarceration_sentences,
        normalized_supervision_sentences,
    ) = SentenceNormalizationManager(
        person.incarceration_sentences,
        person.supervision_sentences,
        delegate=sentencing_delegate,
    ).get_normalized_sentences()

    normalized_program_assignments = ProgramAssignmentNormalizationManager(
        person.program_assignments,
        staff_external_id_to_staff_id,
    ).get_normalized_program_assignments()

    normalized_supervision_contacts = SupervisionContactNormalizationManager(
        person.supervision_contacts,
        staff_external_id_to_staff_id,
    ).get_normalized_supervision_contacts()

    # The normalization functions need to know if this person has any periods that
    # ended because of death to handle any open periods or periods that extend past
    # their death date accordingly.
    earliest_death_date: datetime.date | None = (
        find_earliest_date_of_period_ending_in_death(
            periods=person.supervision_periods + person.incarceration_periods
        )
    )

    normalized_supervision_periods = SupervisionPeriodNormalizationManager(
        person_id=person_id,
        supervision_periods=person.supervision_periods,
        incarceration_periods=person.incarceration_periods,
        delegate=get_state_specific_supervision_period_normalization_delegate(
            state_code=state_code.value,
            assessments=person.assessments,
            supervision_sentences=normalized_supervision_sentences,
            incarceration_periods=person.incarceration_periods,
            sentences=person.sentences,
        ),
        earliest_death_date=earliest_death_date,
        staff_external_id_to_staff_id=staff_external_id_to_staff_id,
    ).get_normalized_supervision_periods()

    (
        normalized_sentences,
        normalized_sentence_groups,
        normalized_sentence_inferred_groups,
        normalized_sentence_imposed_groups,
    ) = get_normalized_sentencing_entities(
        state_code=state_code,
        person=person,
        delegate=sentencing_delegate,
        expected_output_entities=expected_output_entities,
    )

    normalized_incarceration_periods = IncarcerationPeriodNormalizationManager(
        person_id=person_id,
        incarceration_periods=person.incarceration_periods,
        normalization_delegate=get_state_specific_incarceration_period_normalization_delegate(
            state_code.value, incarceration_sentences=normalized_incarceration_sentences
        ),
        normalized_supervision_period_index=NormalizedSupervisionPeriodIndex(
            sorted_supervision_periods=normalized_supervision_periods
        ),
        normalized_violation_responses=collect_violation_responses(
            normalized_violations, NormalizedStateSupervisionViolationResponse
        ),
        earliest_death_date=earliest_death_date,
    ).get_normalized_incarceration_periods()

    normalized_assessments = AssessmentNormalizationManager(
        person.assessments,
        staff_external_id_to_staff_id=staff_external_id_to_staff_id,
        delegate=get_state_specific_assessment_normalization_delegate(
            state_code.value, person
        ),
    ).get_normalized_assessments()

    normalized_staff_relationship_periods: list[
        NormalizedStatePersonStaffRelationshipPeriod
    ] = get_normalized_person_staff_relationship_periods(
        person_id=person_id,
        person_staff_relationship_periods=person.staff_relationship_periods,
        staff_external_id_to_staff_id=staff_external_id_to_staff_id,
    )

    person_kwargs: Mapping[str, Sequence[NormalizedStateEntity]] = {
        "assessments": normalized_assessments,
        "external_ids": normalized_person_external_ids,
        "incarceration_periods": normalized_incarceration_periods,
        "incarceration_sentences": normalized_incarceration_sentences,
        "program_assignments": normalized_program_assignments,
        "sentences": normalized_sentences,
        "sentence_groups": normalized_sentence_groups,
        "sentence_inferred_groups": normalized_sentence_inferred_groups,
        "sentence_imposed_groups": normalized_sentence_imposed_groups,
        "supervision_contacts": normalized_supervision_contacts,
        "supervision_periods": normalized_supervision_periods,
        "supervision_sentences": normalized_supervision_sentences,
        "supervision_violations": normalized_violations,
        "staff_relationship_periods": normalized_staff_relationship_periods,
    }
    return assert_type(
        build_normalized_root_entity(
            pre_normalization_root_entity=person,
            normalized_root_entity_cls=NormalizedStatePerson,
            root_entity_subtree_kwargs=person_kwargs,
        ),
        NormalizedStatePerson,
    )
