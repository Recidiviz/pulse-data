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

"""Constants related to a StateSupervisionContact."""
from enum import unique

import recidiviz.common.constants.enum_canonical_strings as enum_strings
import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


@unique
class StateSupervisionContactType(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    INTERNAL_UNKNOWN = enum_strings.internal_unknown

    FACE_TO_FACE = state_enum_strings.state_supervision_contact_type_face_to_face
    TELEPHONE = state_enum_strings.state_supervision_contact_type_telephone
    WRITTEN_MESSAGE = state_enum_strings.state_supervision_contact_type_written_message
    VIRTUAL = state_enum_strings.state_supervision_contact_type_virtual

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_CONTACT_TYPE_MAP


@unique
class StateSupervisionContactReason(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    INTERNAL_UNKNOWN = enum_strings.internal_unknown

    EMERGENCY_CONTACT = (
        state_enum_strings.state_supervision_contact_reason_emergency_contact
    )
    GENERAL_CONTACT = (
        state_enum_strings.state_supervision_contact_reason_general_contact
    )
    INITIAL_CONTACT = (
        state_enum_strings.state_supervision_contact_reason_initial_contact
    )

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_CONTACT_REASON_MAP


@unique
class StateSupervisionContactStatus(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    INTERNAL_UNKNOWN = enum_strings.internal_unknown

    ATTEMPTED = state_enum_strings.state_supervision_contact_status_attempted
    COMPLETED = state_enum_strings.state_supervision_contact_status_completed

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_CONTACT_STATUS_MAP


@unique
class StateSupervisionContactLocation(EntityEnum, metaclass=EntityEnumMeta):
    EXTERNAL_UNKNOWN = enum_strings.external_unknown
    INTERNAL_UNKNOWN = enum_strings.internal_unknown

    COURT = state_enum_strings.state_supervision_contact_location_court
    FIELD = state_enum_strings.state_supervision_contact_location_field
    JAIL = state_enum_strings.state_supervision_contact_location_jail
    PLACE_OF_EMPLOYMENT = (
        state_enum_strings.state_supervision_contact_location_place_of_employment
    )
    RESIDENCE = state_enum_strings.state_supervision_contact_location_residence
    SUPERVISION_OFFICE = (
        state_enum_strings.state_supervision_contact_location_supervision_office
    )
    TREATMENT_PROVIDER = (
        state_enum_strings.state_supervision_contact_location_treatment_provider
    )

    @staticmethod
    def _get_default_map():
        return _STATE_SUPERVISION_CONTACT_LOCATION_MAP


_STATE_SUPERVISION_CONTACT_LOCATION_MAP = {
    "EXTERNAL UNKNOWN": StateSupervisionContactLocation.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateSupervisionContactLocation.INTERNAL_UNKNOWN,
    "COURT": StateSupervisionContactLocation.COURT,
    "FIELD": StateSupervisionContactLocation.FIELD,
    "JAIL": StateSupervisionContactLocation.JAIL,
    "PLACE OF EMPLOYMENT": StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT,
    "RESIDENCE": StateSupervisionContactLocation.RESIDENCE,
    "SUPERVISION OFFICE": StateSupervisionContactLocation.SUPERVISION_OFFICE,
    "TREATMENT PROVIDER": StateSupervisionContactLocation.TREATMENT_PROVIDER,
}

_STATE_SUPERVISION_CONTACT_STATUS_MAP = {
    "EXTERNAL UNKNOWN": StateSupervisionContactStatus.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateSupervisionContactStatus.INTERNAL_UNKNOWN,
    "ATTEMPTED": StateSupervisionContactStatus.ATTEMPTED,
    "COMPLETED": StateSupervisionContactStatus.COMPLETED,
}

_STATE_SUPERVISION_CONTACT_REASON_MAP = {
    "EXTERNAL UNKNOWN": StateSupervisionContactReason.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateSupervisionContactReason.INTERNAL_UNKNOWN,
    "EMERGENCY CONTACT": StateSupervisionContactReason.EMERGENCY_CONTACT,
    "GENERAL CONTACT": StateSupervisionContactReason.GENERAL_CONTACT,
    "INITIAL CONTACT": StateSupervisionContactReason.INITIAL_CONTACT,
}

_STATE_SUPERVISION_CONTACT_TYPE_MAP = {
    "EXTERNAL UNKNOWN": StateSupervisionContactType.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateSupervisionContactType.INTERNAL_UNKNOWN,
    "FACE TO FACE": StateSupervisionContactType.FACE_TO_FACE,
    "TELEPHONE": StateSupervisionContactType.TELEPHONE,
    "WRITTEN MESSAGE": StateSupervisionContactType.WRITTEN_MESSAGE,
    "VIRTUAL": StateSupervisionContactType.VIRTUAL,
}
