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
# TODO(#12127): Delete this file once all enums are documented
"""Temporary config file used by enum doc generator while enum documentation is
in-progress."""
from typing import List, Type

from recidiviz.common.constants.entity_enum import EntityEnum
from recidiviz.common.constants.state.state_assessment import (
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_charge import (
    StateChargeClassificationType,
    StateChargeStatus,
)
from recidiviz.common.constants.state.state_court_case import (
    StateCourtCaseStatus,
    StateCourtType,
)
from recidiviz.common.constants.state.state_early_discharge import (
    StateEarlyDischargeDecision,
    StateEarlyDischargeDecisionStatus,
)
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
    StateIncarcerationIncidentType,
)
from recidiviz.common.constants.state.state_person import (
    StateEthnicity,
    StateGender,
    StateRace,
    StateResidencyStatus,
)
from recidiviz.common.constants.state.state_person_alias import StatePersonAliasType
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentDischargeReason,
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_contact import (
    StateSupervisionContactLocation,
    StateSupervisionContactMethod,
    StateSupervisionContactReason,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
)
from recidiviz.common.constants.state.state_supervision_sentence import (
    StateSupervisionSentenceSupervisionType,
)

ENUMS_WITH_INCOMPLETE_DOCS: List[Type[EntityEnum]] = [
    StateAssessmentClass,
    StateAssessmentLevel,
    StateAssessmentType,
    StateChargeClassificationType,
    StateChargeStatus,
    StateCourtCaseStatus,
    StateCourtType,
    StateCustodialAuthority,
    StateEarlyDischargeDecision,
    StateEarlyDischargeDecisionStatus,
    StateEthnicity,
    StateGender,
    StateIncarcerationIncidentOutcomeType,
    StateIncarcerationIncidentType,
    StateIncarcerationType,
    StatePersonAliasType,
    StateProgramAssignmentDischargeReason,
    StateProgramAssignmentParticipationStatus,
    StateRace,
    StateResidencyStatus,
    StateSentenceStatus,
    StateSupervisionCaseType,
    StateSupervisionContactLocation,
    StateSupervisionContactMethod,
    StateSupervisionContactReason,
    StateSupervisionContactStatus,
    StateSupervisionContactType,
    StateSupervisionLevel,
    StateSupervisionPeriodAdmissionReason,
    StateSupervisionPeriodSupervisionType,
    StateSupervisionPeriodTerminationReason,
    StateSupervisionSentenceSupervisionType,
]
