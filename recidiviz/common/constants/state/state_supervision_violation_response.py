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

"""Constants related to a StateSupervisionViolationResponse."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


@unique
class StateSupervisionViolationResponseType(StateEntityEnum):
    CITATION = state_enum_strings.state_supervision_violation_response_type_citation
    VIOLATION_REPORT = (
        state_enum_strings.state_supervision_violation_response_type_violation_report
    )
    PERMANENT_DECISION = (
        state_enum_strings.state_supervision_violation_response_type_permanent_decision
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The response type of an actor responding to a person violating a "
            "condition of their supervision."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_VIOLATION_RESPONSE_TYPE_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_VIOLATION_RESPONSE_TYPE_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateSupervisionViolationResponseType.CITATION: "A form completed by a "
    "supervision officer documenting an instance of a person violating a condition "
    "of their supervision. Typically used in cases where the violating behavior is not "
    "severe enough to warrant filing a violation report, per state policy.",
    StateSupervisionViolationResponseType.PERMANENT_DECISION: "Describes an "
    "authoritative body making a final decision about the consequences of the person "
    "violating a condition of their supervision. Used to model the record of the "
    "decision that is made. Should always be used in conjunction with "
    "`StateSupervisionViolationResponseDecision` entries to describe the contents of "
    "the decision.",
    StateSupervisionViolationResponseType.VIOLATION_REPORT: "A report submitted by a "
    "supervision officer documenting an instance of a person violating a condition of "
    "their supervision. Violation reports are typically sent to the judge who "
    "sentenced the case (for probation cases), and may result in a revocation hearing "
    "by the court or parole board. May include recommendations from the officer on "
    "appropriate consequences for the violating behavior.",
}


@unique
class StateSupervisionViolationResponseDecision(StateEntityEnum):
    """Possible types of supervision violation responses."""

    COMMUNITY_SERVICE = (
        state_enum_strings.state_supervision_violation_response_decision_community_service
    )
    CONTINUANCE = (
        state_enum_strings.state_supervision_violation_response_decision_continuance
    )
    DELAYED_ACTION = (
        state_enum_strings.state_supervision_violation_response_decision_delayed_action
    )
    EXTENSION = (
        state_enum_strings.state_supervision_violation_response_decision_extension
    )
    NEW_CONDITIONS = (
        state_enum_strings.state_supervision_violation_response_decision_new_conditions
    )
    OTHER = state_enum_strings.state_supervision_violation_response_decision_other
    REVOCATION = (
        state_enum_strings.state_supervision_violation_response_decision_revocation
    )
    PRIVILEGES_REVOKED = (
        state_enum_strings.state_supervision_violation_response_decision_privileges_revoked
    )
    SERVICE_TERMINATION = (
        state_enum_strings.state_supervision_violation_response_decision_service_termination
    )
    SPECIALIZED_COURT = (
        state_enum_strings.state_supervision_violation_response_decision_specialized_court
    )
    SHOCK_INCARCERATION = (
        state_enum_strings.state_supervision_violation_response_decision_shock_incarceration
    )
    SUSPENSION = (
        state_enum_strings.state_supervision_violation_response_decision_suspension
    )
    TREATMENT_IN_PRISON = (
        state_enum_strings.state_supervision_violation_response_decision_treatment_in_prison
    )
    TREATMENT_IN_FIELD = (
        state_enum_strings.state_supervision_violation_response_decision_treatment_in_field
    )
    # Ultimately, the original violation was not found/formalized, e.g. because it was withdrawn by the officer
    VIOLATION_UNFOUNDED = (
        state_enum_strings.state_supervision_violation_response_decision_violation_unfounded
    )
    WARNING = state_enum_strings.state_supervision_violation_response_decision_warning
    WARRANT_ISSUED = (
        state_enum_strings.state_supervision_violation_response_decision_warrant_issued
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The decision that is made by an actor following a person violating a "
            "condition of their supervision. For `StateSupervisionViolationResponses` "
            "with a `response_type` of "
            "`StateSupervisionViolationResponseType.PERMANENT_DECISION`, this "
            "describes the final decision that was made (e.g. the parole board "
            "deciding to revoke someone’s parole). For other types of responses "
            "(e.g. `StateSupervisionViolationResponseType.CITATION` and "
            "`StateSupervisionViolationResponseType.VIOLATION_REPORT`), this describes "
            "the recommendation of the supervising officer to the authoritative body "
            "that will be making the final decision (e.g. an officer recommending to "
            "the court that a person is mandated to complete treatment in the field "
            "in response to a substance use violation)."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_VIOLATION_RESPONSE_DECISION_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_VIOLATION_RESPONSE_DECISION_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateSupervisionViolationResponseDecision.COMMUNITY_SERVICE: "Mandating the "
    "completion of some form of community service.",
    StateSupervisionViolationResponseDecision.CONTINUANCE: "Keeping the person on "
    "supervision without a sanction or change to their supervision conditions.",
    StateSupervisionViolationResponseDecision.DELAYED_ACTION: "Delaying any further "
    "responses to the violation until further notice (e.g. waiting until more "
    "information is found after an investigation is completed before issuing a "
    "warrant).",
    StateSupervisionViolationResponseDecision.EXTENSION: "Extending the person’s "
    "supervision past their current discharge date.",
    StateSupervisionViolationResponseDecision.NEW_CONDITIONS: "Adding new conditions "
    "to the person’s supervision.",
    StateSupervisionViolationResponseDecision.OTHER: "Describes a decision that is "
    "explicitly labeled as `Other` by the state.",
    StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED: "Revoking "
    "privileges previously granted to the person on supervision.",
    StateSupervisionViolationResponseDecision.REVOCATION: "Revoking the person’s "
    "supervision.",
    StateSupervisionViolationResponseDecision.SERVICE_TERMINATION: "Terminating the "
    "person’s supervision.",
    StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION: "Mandating that the "
    "person spend a distinct amount of time (e.g. 9 months) in incarceration.",
    StateSupervisionViolationResponseDecision.SPECIALIZED_COURT: "Diverting the "
    "person’s case to a specialized court (e.g. Drug Court or Domestic Violence Court).",
    StateSupervisionViolationResponseDecision.SUSPENSION: "Suspending the person’s "
    "supervision.",
    StateSupervisionViolationResponseDecision.TREATMENT_IN_FIELD: "Mandating that the "
    "person complete some form of treatment program in the community.",
    StateSupervisionViolationResponseDecision.TREATMENT_IN_PRISON: "Mandating that "
    "the person complete some form of treatment program in incarceration.",
    StateSupervisionViolationResponseDecision.VIOLATION_UNFOUNDED: "Withdrawing the "
    "report of the violation entirely.",
    StateSupervisionViolationResponseDecision.WARNING: "Issuing a warning to the "
    "person without other sanctions or changes to their supervision.",
    StateSupervisionViolationResponseDecision.WARRANT_ISSUED: "Issuing a warrant for "
    "the person’s arrest.",
}


@unique
class StateSupervisionViolationResponseDecidingBodyType(StateEntityEnum):
    COURT = (
        state_enum_strings.state_supervision_violation_response_deciding_body_type_court
    )
    PAROLE_BOARD = (
        state_enum_strings.state_supervision_violation_response_deciding_body_parole_board
    )
    # A parole/probation officer (PO)
    SUPERVISION_OFFICER = (
        state_enum_strings.state_supervision_violation_response_deciding_body_type_supervision_officer
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The type of actor that is making a decision in response to a person "
            "violating a condition of their supervision."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return (
            _STATE_SUPERVISION_VIOLATION_RESPONSE_DECIDING_BODY_TYPE_VALUE_DESCRIPTIONS
        )


_STATE_SUPERVISION_VIOLATION_RESPONSE_DECIDING_BODY_TYPE_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateSupervisionViolationResponseDecidingBodyType.COURT: "The court, typically the "
    "judge who initially sentenced the person. This is used on "
    "`StateSupervisionViolationResponses` that represent the court "
    "making a decision about the consequences of the person violating a condition "
    "of their supervision. Should be used in conjunction with a "
    "`StateSupervisionViolationResponseType.PERMANENT_DECISION`.",
    StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD: "The parole board "
    "of the state. This is used on `StateSupervisionViolationResponses` that represent "
    "the parole board making a decision about the consequences of the person violating "
    "a condition of their supervision. Should be used in conjunction with a "
    "`StateSupervisionViolationResponseType.PERMANENT_DECISION`.",
    StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER: "A "
    "supervision officer (e.g. a probation or parole officer), typically the officer "
    "assigned to supervise the person on supervision. Typically used in conjunction "
    "with `StateSupervisionViolationResponseType.CITATION` or "
    "`StateSupervisionViolationResponseType.VIOLATION_REPORT`.",
}


@unique
class StateSupervisionViolationResponseSeverity(StateEntityEnum):
    """Possible State Supervision Violation Severity Levels"""

    HIGHEST = state_enum_strings.state_supervision_violation_response_severity_highest
    SECOND_HIGHEST = (
        state_enum_strings.state_supervision_violation_response_severity_second_highest
    )
    THIRD_HIGHEST = (
        state_enum_strings.state_supervision_violation_response_severity_third_highest
    )
    FOURTH_HIGHEST = (
        state_enum_strings.state_supervision_violation_response_severity_fourth_highest
    )
    FIFTH_HIGHEST = (
        state_enum_strings.state_supervision_violation_response_severity_fifth_highest
    )
    SIXTH_HIGHEST = (
        state_enum_strings.state_supervision_violation_response_severity_sixth_highest
    )
    SEVENTH_HIGHEST = (
        state_enum_strings.state_supervision_violation_response_severity_seventh_highest
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "An enum indicating the severity of an incident."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_VIOLATION_RESPONSE_SEVERITY_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_VIOLATION_RESPONSE_SEVERITY_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateSupervisionViolationResponseSeverity.HIGHEST: "Highest response severity level."
    "Responses with the highest severity level should always be mapped to this value,"
    "regardless of the state's naming / numbering scheme for response severity.",
    StateSupervisionViolationResponseSeverity.SECOND_HIGHEST: "Second highest response severity level."
    "Responses with the second highest severity level should always be mapped to this value,"
    "regardless of the state's naming / numbering scheme for response severity.",
    StateSupervisionViolationResponseSeverity.THIRD_HIGHEST: "Third highest response severity level."
    "Responses with the third highest severity level should always be mapped to this value,"
    "regardless of the state's naming / numbering scheme for response severity.",
    StateSupervisionViolationResponseSeverity.FOURTH_HIGHEST: "Fourth highest response severity level."
    "Responses with the fourth highest severity level should always be mapped to this value,"
    "regardless of the state's naming / numbering scheme for response severity.",
    StateSupervisionViolationResponseSeverity.FIFTH_HIGHEST: "Fifth highest response severity level."
    "Responses with the fifth highest severity level should always be mapped to this value,"
    "regardless of the state's naming / numbering scheme for response severity.",
    StateSupervisionViolationResponseSeverity.SIXTH_HIGHEST: "Sixth highest response severity level."
    "Responses with the sixth highest severity level should always be mapped to this value,"
    "regardless of the state's naming / numbering scheme for response severity.",
    StateSupervisionViolationResponseSeverity.SEVENTH_HIGHEST: "Seventh highest response severity level."
    "Responses with the seventh highest severity level should always be mapped to this value,"
    "regardless of the state's naming / numbering scheme for response severity.",
}
