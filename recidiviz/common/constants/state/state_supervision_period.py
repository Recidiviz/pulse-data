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

"""Constants related to a StateSupervisionPeriod."""
from typing import Dict

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


# TODO(2891): Update supervision period objects in schema to use this type
class StateSupervisionPeriodSupervisionType(EntityEnum, metaclass=EntityEnumMeta):
    """Enum that denotes what type of supervision someone is serving at a moment in time."""
    # If the person is serving both probation and parole at the same time, this may be modeled with just one supervision
    # period if the PO is the same. In this case, the supervision period supervision type is DUAL.
    DUAL = state_enum_strings.state_supervision_period_supervision_type_dual
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    # A type of supervision where the person is not formally supervised and does not have to regularly report to a PO.
    # The person does have certain conditions associated with their supervision, that when violated can lead to
    # revocations. Might also be called "Court Probation".
    INFORMAL_PROBATION = state_enum_strings.state_supervision_period_supervision_type_informal_probation
    INTERNAL_UNKNOWN = enum_strings.internal_unknown
    INVESTIGATION = state_enum_strings.state_supervision_period_supervision_type_investigation
    PAROLE = state_enum_strings.state_supervision_period_supervision_type_parole
    PROBATION = state_enum_strings.state_supervision_period_supervision_type_probation

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_PERIOD_SUPERVISION_TYPE_MAP


class StateSupervisionPeriodAdmissionReason(EntityEnum, metaclass=EntityEnumMeta):
    """Admission reasons for StateSupervisionPeriod"""
    ABSCONSION = state_enum_strings.state_supervision_period_admission_reason_absconsion
    # This reason indicates the person has been released from an incarceration period into a supervision period
    # (i.e. parole).
    CONDITIONAL_RELEASE = state_enum_strings.state_supervision_period_admission_reason_conditional_release
    COURT_SENTENCE = state_enum_strings.state_supervision_period_admission_reason_court_sentence
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    INTERNAL_UNKNOWN = enum_strings.internal_unknown
    # TODO(3276): Remove this enum once we've completely transitioned to using
    #  StateSupervisionPeriod.supervision_period_supervision_type for Investigation
    INVESTIGATION = state_enum_strings.state_supervision_period_admission_reason_investigation
    TRANSFER_OUT_OF_STATE = state_enum_strings.state_supervision_period_admission_reason_transfer_out_of_state
    TRANSFER_WITHIN_STATE = state_enum_strings.state_supervision_period_admission_reason_transfer_within_state
    RETURN_FROM_ABSCONSION = state_enum_strings.state_supervision_period_admission_reason_return_from_absconsion
    RETURN_FROM_SUSPENSION = state_enum_strings.state_supervision_period_admission_reason_return_from_suspension

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_ADMISSION_TYPE_MAP


class StateSupervisionPeriodStatus(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    PRESENT_WITHOUT_INFO = enum_strings.present_without_info
    TERMINATED = state_enum_strings.state_supervision_period_status_terminated
    UNDER_SUPERVISION = state_enum_strings.state_supervision_period_status_under_supervision

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_STATUS_MAP


class StateSupervisionLevel(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    INTERNAL_UNKNOWN = enum_strings.internal_unknown
    PRESENT_WITHOUT_INFO = enum_strings.present_without_info
    DIVERSION = state_enum_strings.state_supervision_period_supervision_level_diversion
    INCARCERATED = state_enum_strings.state_supervision_period_supervision_level_incarcerated
    IN_CUSTODY = state_enum_strings.state_supervision_period_supervision_level_in_custody
    INTERSTATE_COMPACT = state_enum_strings.state_supervision_period_supervision_level_interstate_compact
    LIMITED = state_enum_strings.state_supervision_period_supervision_level_limited
    MINIMUM = state_enum_strings.state_supervision_period_supervision_level_minimum
    MEDIUM = state_enum_strings.state_supervision_period_supervision_level_medium
    HIGH = state_enum_strings.state_supervision_period_supervision_level_high
    MAXIMUM = state_enum_strings.state_supervision_period_supervision_level_maximum
    UNSUPERVISED = state_enum_strings.state_supervision_period_supervision_level_unsupervised

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_LEVEL_MAP


class StateSupervisionPeriodTerminationReason(EntityEnum, metaclass=EntityEnumMeta):
    """Termination reasons for StateSupervisionPeriod"""
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    ABSCONSION = state_enum_strings.state_supervision_period_termination_reason_absconsion
    DEATH = state_enum_strings.state_supervision_period_termination_reason_death
    DISCHARGE = state_enum_strings.state_supervision_period_termination_reason_discharge
    EXPIRATION = state_enum_strings.state_supervision_period_termination_reason_expiration
    INTERNAL_UNKNOWN = enum_strings.internal_unknown
    # TODO(3276): Remove this enum once we've completely transitioned to using
    #  StateSupervisionPeriod.supervision_period_supervision_type for Investigation
    INVESTIGATION = state_enum_strings.state_supervision_period_termination_reason_investigation
    TRANSFER_OUT_OF_STATE = state_enum_strings.state_supervision_period_termination_reason_transfer_out_of_state
    TRANSFER_WITHIN_STATE = state_enum_strings.state_supervision_period_termination_reason_transfer_within_state
    RETURN_FROM_ABSCONSION = state_enum_strings.state_supervision_period_termination_reason_return_from_absconsion
    RETURN_TO_INCARCERATION = state_enum_strings.state_supervision_period_termination_reason_return_to_incarceration
    REVOCATION = state_enum_strings.state_supervision_period_termination_reason_revocation
    SUSPENSION = state_enum_strings.state_supervision_period_termination_reason_suspension

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_PERIOD_TERMINATION_REASON_MAP


_STATE_SUPERVISION_PERIOD_SUPERVISION_TYPE_MAP = {
    'DUAL': StateSupervisionPeriodSupervisionType.DUAL,
    'EXTERNAL UNKNOWN': StateSupervisionPeriodSupervisionType.EXTERNAL_UNKNOWN,
    'INFORMAL PROBATION': StateSupervisionPeriodSupervisionType.INFORMAL_PROBATION,
    'INTERNAL UNKNOWN': StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN,
    'INVESTIGATION': StateSupervisionPeriodSupervisionType.INVESTIGATION,
    'PAROLE': StateSupervisionPeriodSupervisionType.PAROLE,
    'PROBATION': StateSupervisionPeriodSupervisionType.PROBATION
}


_STATE_SUPERVISION_ADMISSION_TYPE_MAP = {
    'ABSCONDED': StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
    'ABSCONSION': StateSupervisionPeriodAdmissionReason.ABSCONSION,
    'CONDITIONAL RELEASE': StateSupervisionPeriodAdmissionReason.CONDITIONAL_RELEASE,
    'COURT SENTENCE': StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
    'SENTENCE': StateSupervisionPeriodAdmissionReason.COURT_SENTENCE,
    'EXTERNAL UNKNOWN': StateSupervisionPeriodAdmissionReason.EXTERNAL_UNKNOWN,
    'INTERNAL UNKNOWN': StateSupervisionPeriodAdmissionReason.INTERNAL_UNKNOWN,
    'INVESTIGATION': StateSupervisionPeriodAdmissionReason.INVESTIGATION,
    'TRANSFER WITHIN STATE': StateSupervisionPeriodAdmissionReason.TRANSFER_WITHIN_STATE,
    'TRANSFER OUT OF STATE': StateSupervisionPeriodAdmissionReason.TRANSFER_OUT_OF_STATE,
    'RETURN FROM ABSCOND': StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
    'RETURN FROM ABSCONSION': StateSupervisionPeriodAdmissionReason.RETURN_FROM_ABSCONSION,
    'RETURN FROM SUSPENSION': StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION,
    'SUSPENDED': StateSupervisionPeriodAdmissionReason.RETURN_FROM_SUSPENSION,
}

_STATE_SUPERVISION_STATUS_MAP = {
    'EXTERNAL UNKNOWN': StateSupervisionPeriodStatus.EXTERNAL_UNKNOWN,
    'PRESENT WITHOUT INFO': StateSupervisionPeriodStatus.PRESENT_WITHOUT_INFO,
    'TERMINATED': StateSupervisionPeriodStatus.TERMINATED,
    'RELEASED': StateSupervisionPeriodStatus.TERMINATED,
    'UNDER SUPERVISION': StateSupervisionPeriodStatus.UNDER_SUPERVISION,
    'SUPERVISED': StateSupervisionPeriodStatus.UNDER_SUPERVISION
}

_STATE_SUPERVISION_LEVEL_MAP: Dict[str, StateSupervisionLevel] = {
    'EXTERNAL UNKNOWN': StateSupervisionLevel.EXTERNAL_UNKNOWN,
    'INTERNAL UNKNOWN': StateSupervisionLevel.INTERNAL_UNKNOWN,
    'PRESENT WITHOUT INFO': StateSupervisionLevel.PRESENT_WITHOUT_INFO,
    'INCARCERATED': StateSupervisionLevel.INCARCERATED,
    'IN CUSTODY': StateSupervisionLevel.IN_CUSTODY,
    'MINIMUM': StateSupervisionLevel.MINIMUM,
    'MIN': StateSupervisionLevel.MINIMUM,
    'MEDIUM': StateSupervisionLevel.MEDIUM,
    'MED': StateSupervisionLevel.MEDIUM,
    'HIGH': StateSupervisionLevel.HIGH,
    'MAXIMUM': StateSupervisionLevel.MAXIMUM,
    'MAX': StateSupervisionLevel.MAXIMUM,
    'DIVERSION': StateSupervisionLevel.DIVERSION,
    'INTERSTATE COMPACT': StateSupervisionLevel.INTERSTATE_COMPACT,
    'INTERSTATE': StateSupervisionLevel.INTERSTATE_COMPACT,
    'UNSUPERVISED': StateSupervisionLevel.UNSUPERVISED,
    'LIMITED': StateSupervisionLevel.LIMITED,
}

_STATE_SUPERVISION_PERIOD_TERMINATION_REASON_MAP = {
    'ABSCOND': StateSupervisionPeriodTerminationReason.ABSCONSION,
    'ABSCONDED': StateSupervisionPeriodTerminationReason.ABSCONSION,
    'ABSCONSION': StateSupervisionPeriodTerminationReason.ABSCONSION,
    'DEATH': StateSupervisionPeriodTerminationReason.DEATH,
    'DECEASED': StateSupervisionPeriodTerminationReason.DEATH,
    'DISCHARGE': StateSupervisionPeriodTerminationReason.DISCHARGE,
    'DISCHARGED': StateSupervisionPeriodTerminationReason.DISCHARGE,
    'EXPIRATION': StateSupervisionPeriodTerminationReason.EXPIRATION,
    'EXTERNAL UNKNOWN': StateSupervisionPeriodTerminationReason.EXTERNAL_UNKNOWN,
    'EXPIRED': StateSupervisionPeriodTerminationReason.EXPIRATION,
    'INTERNAL UNKNOWN': StateSupervisionPeriodTerminationReason.INTERNAL_UNKNOWN,
    'INVESTIGATION': StateSupervisionPeriodTerminationReason.INVESTIGATION,
    'TRANSFER WITHIN STATE': StateSupervisionPeriodTerminationReason.TRANSFER_WITHIN_STATE,
    'TRANSFER OUT OF STATE': StateSupervisionPeriodTerminationReason.TRANSFER_OUT_OF_STATE,
    'RETURN FROM ABSCONSION': StateSupervisionPeriodTerminationReason.RETURN_FROM_ABSCONSION,
    'RETURN TO INCARCERATION': StateSupervisionPeriodTerminationReason.RETURN_TO_INCARCERATION,
    'REVOCATION': StateSupervisionPeriodTerminationReason.REVOCATION,
    'REVOKED': StateSupervisionPeriodTerminationReason.REVOCATION,
    'SUSPENDED': StateSupervisionPeriodTerminationReason.SUSPENSION,
    'SUSPENSION': StateSupervisionPeriodTerminationReason.SUSPENSION,
}
