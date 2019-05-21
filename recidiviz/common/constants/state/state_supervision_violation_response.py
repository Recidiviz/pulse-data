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

import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class StateSupervisionViolationResponseType(EntityEnum,
                                            metaclass=EntityEnumMeta):
    CITATION = \
        state_enum_strings.state_supervision_violation_response_type_citation
    VIOLATION_REPORT = \
        state_enum_strings.\
        state_supervision_violation_response_type_violation_report
    PERMANENT_DECISION = \
        state_enum_strings. \
        state_supervision_violation_response_type_permanent_decision

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_VIOLATION_RESPONSE_TYPE_MAP


class StateSupervisionViolationResponseDecision(EntityEnum,
                                                metaclass=EntityEnumMeta):
    CONTINUANCE = \
        state_enum_strings.\
        state_supervision_violation_response_decision_continuance
    EXTENSION = \
        state_enum_strings.\
        state_supervision_violation_response_decision_extension
    REVOCATION = \
        state_enum_strings.\
        state_supervision_violation_response_decision_revocation
    SUSPENSION = \
        state_enum_strings.\
        state_supervision_violation_response_decision_suspension

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_VIOLATION_RESPONSE_DECISION_MAP


class StateSupervisionViolationResponseRevocationType(EntityEnum,
                                                      metaclass=EntityEnumMeta):
    SHOCK_INCARCERATION = \
        state_enum_strings. \
        state_supervision_violation_response_revocation_type_shock_incarceration
    STANDARD = state_enum_strings. \
        state_supervision_violation_response_revocation_type_standard
    TREATMENT_IN_PRISON = \
        state_enum_strings. \
        state_supervision_violation_response_revocation_type_treatment_in_prison

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_VIOLATION_RESPONSE_REVOCATION_TYPE_MAP


class StateSupervisionViolationResponseDecidingBodyType(
        EntityEnum, metaclass=EntityEnumMeta):
    COURT = \
        state_enum_strings. \
        state_supervision_violation_response_deciding_body_type_court
    PAROLE_BOARD = \
        state_enum_strings.\
        state_supervision_violation_response_deciding_body_parole_board
    # A parole/probation officer (PO)
    # pylint: disable=line-too-long
    SUPERVISION_OFFICER = \
        state_enum_strings. \
        state_supervision_violation_response_deciding_body_type_supervision_officer

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_VIOLATION_RESPONSE_DECIDING_BODY_TYPE_MAP


_STATE_SUPERVISION_VIOLATION_RESPONSE_TYPE_MAP = {
    'CITATION': StateSupervisionViolationResponseType.CITATION,
    'VIOLATION REPORT': StateSupervisionViolationResponseType.VIOLATION_REPORT,
    'PERMANENT DECISION':
        StateSupervisionViolationResponseType.PERMANENT_DECISION,
}

_STATE_SUPERVISION_VIOLATION_RESPONSE_DECISION_MAP = {
    'CONTINUANCE': StateSupervisionViolationResponseDecision.CONTINUANCE,
    'EXTENSION': StateSupervisionViolationResponseDecision.EXTENSION,
    'REVOCATION': StateSupervisionViolationResponseDecision.REVOCATION,
    'SUSPENSION': StateSupervisionViolationResponseDecision.SUSPENSION,
}

_STATE_SUPERVISION_VIOLATION_RESPONSE_REVOCATION_TYPE_MAP = {
    'SHOCK INCARCERATION':
        StateSupervisionViolationResponseRevocationType.SHOCK_INCARCERATION,
    'STANDARD':
        StateSupervisionViolationResponseRevocationType.STANDARD,
    'TREATMENT IN PRISON':
        StateSupervisionViolationResponseRevocationType.TREATMENT_IN_PRISON,

}

_STATE_SUPERVISION_VIOLATION_RESPONSE_DECIDING_BODY_TYPE_MAP = {
    'COURT': StateSupervisionViolationResponseDecidingBodyType.COURT,
    'PAROLE BOARD':
        StateSupervisionViolationResponseDecidingBodyType.PAROLE_BOARD,
    'SUPERVISION OFFICER':
        StateSupervisionViolationResponseDecidingBodyType.SUPERVISION_OFFICER,
}
