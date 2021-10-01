# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Manages state-specific methodology decisions made throughout the calculation pipelines."""
import logging

# TODO(#2995): Make a state config file for every state and every one of these state-specific calculation methodologies
from datetime import date
from typing import List, Optional

from recidiviz.calculator.pipeline.utils.incarceration_period_pre_processing_manager import (
    StateSpecificIncarcerationPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_commitment_from_supervision_delegate import (
    StateSpecificCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_incarceration_delegate import (
    StateSpecificIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_supervision_delegate import (
    StateSpecificSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.state_specific_violations_delegate import (
    StateSpecificViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_commitment_from_supervision_delegate import (
    UsIdCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_incarceration_delegate import (
    UsIdIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_incarceration_period_pre_processing_delegate import (
    UsIdIncarcerationPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_compliance import (
    UsIdSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_delegate import (
    UsIdSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_pre_processing_delegate import (
    UsIdSupervisionPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_supervision_utils import (
    us_id_get_post_incarceration_supervision_type,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_violation_response_preprocessing_delegate import (
    UsIdViolationResponsePreprocessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_id.us_id_violations_delegate import (
    UsIdViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_commitment_from_supervision_delegate import (
    UsMoCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_incarceration_delegate import (
    UsMoIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_incarceration_period_pre_processing_delegate import (
    UsMoIncarcerationPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_delegate import (
    UsMoSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_period_pre_processing_delegate import (
    UsMoSupervisionPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_supervision_utils import (
    us_mo_get_month_supervision_type,
    us_mo_get_most_recent_supervision_type_before_upper_bound_day,
    us_mo_get_post_incarceration_supervision_type,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violation_response_preprocessing_delegate import (
    UsMoViolationResponsePreprocessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_mo.us_mo_violations_delegate import (
    UsMoViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_commitment_from_supervision_delegate import (
    UsNdCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_incarceration_delegate import (
    UsNdIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_incarceration_period_pre_processing_delegate import (
    UsNdIncarcerationPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_compliance import (
    UsNdSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_delegate import (
    UsNdSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_pre_processing_delegate import (
    UsNdSupervisionPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_supervision_utils import (
    us_nd_get_post_incarceration_supervision_type,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_violation_response_preprocessing_delegate import (
    UsNdViolationResponsePreprocessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_nd.us_nd_violations_delegate import (
    UsNdViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_commitment_from_supervision_delegate import (
    UsPaCommitmentFromSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_incarceration_delegate import (
    UsPaIncarcerationDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_incarceration_period_pre_processing_delegate import (
    UsPaIncarcerationPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_compliance import (
    UsPaSupervisionCaseCompliance,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_delegate import (
    UsPaSupervisionDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_supervision_period_pre_processing_delegate import (
    UsPaSupervisionPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_violation_response_preprocessing_delegate import (
    UsPaViolationResponsePreprocessingDelegate,
)
from recidiviz.calculator.pipeline.utils.state_utils.us_pa.us_pa_violations_delegate import (
    UsPaViolationDelegate,
)
from recidiviz.calculator.pipeline.utils.supervision_case_compliance_manager import (
    StateSupervisionCaseComplianceManager,
)
from recidiviz.calculator.pipeline.utils.supervision_period_pre_processing_manager import (
    StateSpecificSupervisionPreProcessingDelegate,
)
from recidiviz.calculator.pipeline.utils.supervision_type_identification import (
    get_month_supervision_type_default,
)
from recidiviz.calculator.pipeline.utils.supervision_violation_responses_pre_processing_manager import (
    StateSpecificViolationResponsePreProcessingDelegate,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.entity_utils import CoreEntityFieldIndex
from recidiviz.persistence.entity.state.entities import (
    StateAssessment,
    StateIncarcerationPeriod,
    StateIncarcerationSentence,
    StatePerson,
    StateSupervisionContact,
    StateSupervisionPeriod,
    StateSupervisionSentence,
    StateSupervisionViolationResponse,
)


def get_month_supervision_type(
    any_date_in_month: date,
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_period: StateSupervisionPeriod,
    field_index: CoreEntityFieldIndex,
) -> StateSupervisionPeriodSupervisionType:
    """Supervision type can change over time even if the period does not change. This function calculates the
    supervision type that a given supervision period represents during the month that |any_date_in_month| falls in. The
    objects / info we use to determine supervision type may be state-specific.

    Args:
    any_date_in_month: (date) Any day in the month to consider
    supervision_period: (StateSupervisionPeriod) The supervision period we want to associate a supervision type with
    supervision_sentences: (List[StateSupervisionSentence]) All supervision sentences for a given person.
    """

    if supervision_period.state_code.upper() == "US_MO":
        return us_mo_get_month_supervision_type(
            any_date_in_month,
            supervision_sentences,
            incarceration_sentences,
            supervision_period,
        )

    if supervision_period.state_code.upper() in ("US_ID", "US_PA"):
        return (
            supervision_period.supervision_type
            if supervision_period.supervision_type
            else StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
        )

    return get_month_supervision_type_default(
        any_date_in_month,
        supervision_sentences,
        incarceration_sentences,
        supervision_period,
        field_index,
    )


def get_post_incarceration_supervision_type(
    incarceration_sentences: List[StateIncarcerationSentence],
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_period: StateIncarcerationPeriod,
) -> Optional[StateSupervisionPeriodSupervisionType]:
    """If the person was released from incarceration onto some form of supervision, returns the type of supervision
    they were released to. This function must be implemented for each state for which we need this output. There is not
    a default way to determine the supervision type someone is released onto.

    Args:
        incarceration_sentences: (List[StateIncarcerationSentence]) All IncarcerationSentences associated with this
            person.
        supervision_sentences: (List[StateSupervisionSentence]) All SupervisionSentences associated with this person.
        incarceration_period: (StateIncarcerationPeriod) The incarceration period the person was released from.
    """
    state_code = incarceration_period.state_code

    if state_code.upper() == "US_ID":
        return us_id_get_post_incarceration_supervision_type(
            incarceration_sentences, supervision_sentences, incarceration_period
        )
    if state_code.upper() == "US_MO":
        return us_mo_get_post_incarceration_supervision_type(
            incarceration_sentences, supervision_sentences, incarceration_period
        )
    if state_code.upper() == "US_ND":
        return us_nd_get_post_incarceration_supervision_type(incarceration_period)

    logging.warning(
        "get_post_incarceration_supervision_type not implemented for state: %s",
        state_code,
    )
    return None


def terminating_supervision_period_supervision_type(
    supervision_period: StateSupervisionPeriod,
    supervision_sentences: List[StateSupervisionSentence],
    incarceration_sentences: List[StateIncarcerationSentence],
    field_index: CoreEntityFieldIndex,
) -> StateSupervisionPeriodSupervisionType:
    """Calculates the supervision type that should be associated with a terminated supervision period. In some cases,
    the supervision period will be terminated long after the person has been incarcerated (e.g. in the case of a board
    hold, someone might remain assigned to a PO until their parole is revoked), so we do a lookback to see the most
    recent supervision period supervision type we can associate with this termination.
    """

    if not supervision_period.termination_date:
        raise ValueError(
            f"Expected a terminated supervision period for period "
            f"[{supervision_period.supervision_period_id}]"
        )

    if supervision_period.state_code.upper() == "US_MO":
        supervision_type = (
            us_mo_get_most_recent_supervision_type_before_upper_bound_day(
                upper_bound_exclusive_date=supervision_period.termination_date,
                lower_bound_inclusive_date=supervision_period.start_date,
                incarceration_sentences=incarceration_sentences,
                supervision_sentences=supervision_sentences,
            )
        )

        return (
            supervision_type
            if supervision_type
            else StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
        )

    if supervision_period.state_code.upper() in ("US_ID", "US_PA"):
        return (
            supervision_period.supervision_type
            if supervision_period.supervision_type
            else StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN
        )

    return get_month_supervision_type_default(
        supervision_period.termination_date,
        supervision_sentences,
        incarceration_sentences,
        supervision_period,
        field_index=field_index,
    )


def get_state_specific_case_compliance_manager(
    person: StatePerson,
    supervision_period: StateSupervisionPeriod,
    case_type: StateSupervisionCaseType,
    start_of_supervision: date,
    assessments: List[StateAssessment],
    supervision_contacts: List[StateSupervisionContact],
    violation_responses: List[StateSupervisionViolationResponse],
) -> Optional[StateSupervisionCaseComplianceManager]:
    """Returns a state-specific SupervisionCaseComplianceManager object, containing information about whether the
    given supervision case is in compliance with state-specific standards. If the state of the
    supervision_period does not have state-specific compliance calculations, returns None."""
    state_code = supervision_period.state_code.upper()
    if state_code == StateCode.US_ID.value:
        return UsIdSupervisionCaseCompliance(
            person,
            supervision_period,
            case_type,
            start_of_supervision,
            assessments,
            supervision_contacts,
            violation_responses,
        )
    if state_code == StateCode.US_ND.value:
        return UsNdSupervisionCaseCompliance(
            person,
            supervision_period,
            case_type,
            start_of_supervision,
            assessments,
            supervision_contacts,
            violation_responses,
        )
    if state_code == StateCode.US_PA.value:
        return UsPaSupervisionCaseCompliance(
            person,
            supervision_period,
            case_type,
            start_of_supervision,
            assessments,
            supervision_contacts,
            violation_responses,
        )

    return None


def get_state_specific_incarceration_period_pre_processing_delegate(
    state_code: str,
) -> StateSpecificIncarcerationPreProcessingDelegate:
    """Returns the type of IncarcerationPreProcessingDelegate that should be used for
    pre-processing StateIncarcerationPeriod entities from a given |state_code|."""
    if state_code == StateCode.US_ID.value:
        return UsIdIncarcerationPreProcessingDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoIncarcerationPreProcessingDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdIncarcerationPreProcessingDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaIncarcerationPreProcessingDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def get_state_specific_supervision_period_pre_processing_delegate(
    state_code: str,
) -> StateSpecificSupervisionPreProcessingDelegate:
    """Returns the type of SupervisionPreProcessingDelegate that should be used for
    pre-processing StateSupervisionPeriod entities from a given |state_code|."""
    if state_code == StateCode.US_ID.value:
        return UsIdSupervisionPreProcessingDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoSupervisionPreProcessingDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdSupervisionPreProcessingDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaSupervisionPreProcessingDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def get_state_specific_commitment_from_supervision_delegate(
    state_code: str,
) -> StateSpecificCommitmentFromSupervisionDelegate:
    """Returns the type of StateSpecificCommitmentFromSupervisionDelegate that should be used for
    commitment from supervision admission calculations in a given |state_code|."""
    if state_code == StateCode.US_ID.value:
        return UsIdCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdCommitmentFromSupervisionDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaCommitmentFromSupervisionDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def get_state_specific_violation_delegate(
    state_code: str,
) -> StateSpecificViolationDelegate:
    """Returns the type of StateSpecificViolationDelegate that should be used for
    violation calculations in a given |state_code|."""
    if state_code == StateCode.US_ID.value:
        return UsIdViolationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoViolationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdViolationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaViolationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def get_state_specific_violation_response_preprocessing_delegate(
    state_code: str,
) -> StateSpecificViolationResponsePreProcessingDelegate:
    """Returns the type of StateSpecificViolationResponsePreProcessingDelegate that should be used for
    violation calculations in a given |state_code|."""
    if state_code == StateCode.US_ID.value:
        return UsIdViolationResponsePreprocessingDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoViolationResponsePreprocessingDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdViolationResponsePreprocessingDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaViolationResponsePreprocessingDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def get_state_specific_incarceration_delegate(
    state_code: str,
) -> StateSpecificIncarcerationDelegate:
    """Returns the type of StateSpecificIncarcerationDelegate that should be used for
    incarceration calculations in a given |state_code|."""
    if state_code == StateCode.US_ID.value:
        return UsIdIncarcerationDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoIncarcerationDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdIncarcerationDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaIncarcerationDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")


def get_state_specific_supervision_delegate(
    state_code: str,
) -> StateSpecificSupervisionDelegate:
    """Returns the type of StateSpecificSupervisionDelegate that should be used for
    supervision calculations in a given |state_code|."""
    if state_code == StateCode.US_ID.value:
        return UsIdSupervisionDelegate()
    if state_code == StateCode.US_MO.value:
        return UsMoSupervisionDelegate()
    if state_code == StateCode.US_ND.value:
        return UsNdSupervisionDelegate()
    if state_code == StateCode.US_PA.value:
        return UsPaSupervisionDelegate()

    raise ValueError(f"Unexpected state code [{state_code}]")
