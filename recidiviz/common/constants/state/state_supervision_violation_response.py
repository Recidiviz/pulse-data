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

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


@unique
class StateSupervisionViolationResponseType(EntityEnum, metaclass=EntityEnumMeta):
    CITATION = state_enum_strings.state_supervision_violation_response_type_citation
    VIOLATION_REPORT = (
        state_enum_strings.state_supervision_violation_response_type_violation_report
    )
    PERMANENT_DECISION = (
        state_enum_strings.state_supervision_violation_response_type_permanent_decision
    )

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_VIOLATION_RESPONSE_TYPE_MAP


@unique
class StateSupervisionViolationResponseDecision(EntityEnum, metaclass=EntityEnumMeta):
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
    INTERNAL_UNKNOWN = enum_strings.internal_unknown
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
    WARNING = state_enum_strings.state_supervision_violation_response_decision_warning
    WARRANT_ISSUED = (
        state_enum_strings.state_supervision_violation_response_decision_warrant_issued
    )

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_VIOLATION_RESPONSE_DECISION_MAP


@unique
class StateSupervisionViolationResponseRevocationType(
    EntityEnum, metaclass=EntityEnumMeta
):
    REINCARCERATION = (
        state_enum_strings.state_supervision_violation_response_revocation_type_reincarceration
    )
    RETURN_TO_SUPERVISION = (
        state_enum_strings.state_supervision_violation_response_revocation_type_return_to_supervision
    )
    SHOCK_INCARCERATION = (
        state_enum_strings.state_supervision_violation_response_revocation_type_shock_incarceration
    )
    TREATMENT_IN_PRISON = (
        state_enum_strings.state_supervision_violation_response_revocation_type_treatment_in_prison
    )

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_VIOLATION_RESPONSE_REVOCATION_TYPE_MAP


# TODO(#3108): Transition this enum to use StateActingBodyType
@unique
class StateSupervisionViolationResponseDecidingBodyType(
    EntityEnum, metaclass=EntityEnumMeta
):
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

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_VIOLATION_RESPONSE_DECIDING_BODY_TYPE_MAP


_STATE_SUPERVISION_VIOLATION_RESPONSE_TYPE_MAP = {
    "CITATION": StateSupervisionViolationResponseType.CITATION,
    "VIOLATION REPORT": StateSupervisionViolationResponseType.VIOLATION_REPORT,
    "PERMANENT DECISION": StateSupervisionViolationResponseType.PERMANENT_DECISION,
}

_STATE_SUPERVISION_VIOLATION_RESPONSE_DECISION_MAP = {
    "COMMUNITY SERVICE": StateSupervisionViolationResponseDecision.COMMUNITY_SERVICE,
    "CONTINUANCE": StateSupervisionViolationResponseDecision.CONTINUANCE,
    "DELAYED ACTION": StateSupervisionViolationResponseDecision.DELAYED_ACTION,
    "EXTENSION": StateSupervisionViolationResponseDecision.EXTENSION,
    "INTERNAL UNKNOWN": StateSupervisionViolationResponseDecision.INTERNAL_UNKNOWN,
    "NEW CONDITIONS": StateSupervisionViolationResponseDecision.NEW_CONDITIONS,
    "OTHER": StateSupervisionViolationResponseDecision.OTHER,
    "PRIVILEGES REVOKED": StateSupervisionViolationResponseDecision.PRIVILEGES_REVOKED,
    "REVOCATION": StateSupervisionViolationResponseDecision.REVOCATION,
    "SERVICE TERMINATION": StateSupervisionViolationResponseDecision.SERVICE_TERMINATION,
    "SPECIALIZED COURT": StateSupervisionViolationResponseDecision.SPECIALIZED_COURT,
    "SHOCK INCARCERATION": StateSupervisionViolationResponseDecision.SHOCK_INCARCERATION,
    "SUSPENSION": StateSupervisionViolationResponseDecision.SUSPENSION,
    "TREATMENT IN PRISON": StateSupervisionViolationResponseDecision.TREATMENT_IN_PRISON,
    "TREATMENT IN FIELD": StateSupervisionViolationResponseDecision.TREATMENT_IN_FIELD,
    "WARNING": StateSupervisionViolationResponseDecision.WARNING,
    "WARRANT ISSUED": StateSupervisionViolationResponseDecision.WARRANT_ISSUED,
}

_STATE_SUPERVISION_VIOLATION_RESPONSE_REVOCATION_TYPE_MAP = {
    "REINCARCERATION": StateSupervisionViolationResponseRevocationType.REINCARCERATION,
    "SUPERVISION TERMINATED": StateSupervisionViolationResponseRevocationType.REINCARCERATION,
    "PLACED BACK ON PROBATION PAROLE": StateSupervisionViolationResponseRevocationType.RETURN_TO_SUPERVISION,
    "SUPERVISION": StateSupervisionViolationResponseRevocationType.RETURN_TO_SUPERVISION,
    "RETURN TO SUPERVISION": StateSupervisionViolationResponseRevocationType.RETURN_TO_SUPERVISION,
    "SHOCK INCARCERATION": StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
    "TREATMENT IN PRISON": StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON,
}

_STATE_SUPERVISION_VIOLATION_RESPONSE_DECIDING_BODY_TYPE_MAP = {
    "COURT": StateSupervisionViolationResponseDecidingBodyType.COURT,
    "PAROLE BOARD": StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
    "SUPERVISION OFFICER": StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
}
