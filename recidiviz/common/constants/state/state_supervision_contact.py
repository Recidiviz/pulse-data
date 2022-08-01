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
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateSupervisionContactType(StateEntityEnum):
    COLLATERAL = state_enum_strings.state_supervision_contact_type_collateral
    DIRECT = state_enum_strings.state_supervision_contact_type_direct
    BOTH_COLLATERAL_AND_DIRECT = (
        state_enum_strings.state_supervision_contact_type_both_collateral_and_direct
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSupervisionContactType"]:
        return _STATE_SUPERVISION_CONTACT_TYPE_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "The category of the contact."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_CONTACT_TYPE_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_CONTACT_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateSupervisionContactType.BOTH_COLLATERAL_AND_DIRECT: "Used when the supervising "
    "officer is contacting both the person on supervision and someone else (e.g. the "
    "person’s spouse, the person’s employer, etc.).",
    StateSupervisionContactType.COLLATERAL: "Used when the supervising officer is "
    "contacting someone that is not the person on supervision (e.g. the person’s "
    "spouse, the person’s employer, etc.).",
    StateSupervisionContactType.DIRECT: "Used when the supervising officer is "
    "contacting the person on supervision.",
}


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateSupervisionContactMethod(StateEntityEnum):
    TELEPHONE = state_enum_strings.state_supervision_contact_method_telephone
    WRITTEN_MESSAGE = (
        state_enum_strings.state_supervision_contact_method_written_message
    )
    VIRTUAL = state_enum_strings.state_supervision_contact_method_virtual
    IN_PERSON = state_enum_strings.state_supervision_contact_method_in_person
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSupervisionContactMethod"]:
        return _STATE_SUPERVISION_CONTACT_METHOD_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The way in which the supervising officer interacted with the person "
            "on supervision."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_CONTACT_METHOD_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_CONTACT_METHOD_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateSupervisionContactMethod.IN_PERSON: "Used when the supervising officer "
    "saw the person on supervision in person.",
    StateSupervisionContactMethod.TELEPHONE: "Used when the supervising officer "
    "communicated with the person on supervision over the telephone.",
    StateSupervisionContactMethod.VIRTUAL: "Used when the supervising officer "
    "communicated with the person on supervision virtually (e.g. a video call).",
    StateSupervisionContactMethod.WRITTEN_MESSAGE: "Used when the supervising officer "
    "communicated with the person on supervision through some form of written "
    "communication (e.g. a letter).",
}


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateSupervisionContactReason(StateEntityEnum):
    EMERGENCY_CONTACT = (
        state_enum_strings.state_supervision_contact_reason_emergency_contact
    )
    GENERAL_CONTACT = (
        state_enum_strings.state_supervision_contact_reason_general_contact
    )
    INITIAL_CONTACT = (
        state_enum_strings.state_supervision_contact_reason_initial_contact
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSupervisionContactReason"]:
        return _STATE_SUPERVISION_CONTACT_REASON_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return "The reason the supervising officer contacted the person on supervision."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_CONTACT_REASON_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_CONTACT_REASON_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateSupervisionContactReason.EMERGENCY_CONTACT: "Used when the supervising "
    "officer contacted the person on supervision because of some kind of emergency.",
    StateSupervisionContactReason.GENERAL_CONTACT: "Used when the supervising officer "
    "contacted the person on supervision because of the general contact requirements "
    "of the person’s supervision conditions.",
    StateSupervisionContactReason.INITIAL_CONTACT: "Used when the supervising officer "
    "contacted the person on supervision because the person had just started on "
    "supervision.",
}


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateSupervisionContactStatus(StateEntityEnum):
    ATTEMPTED = state_enum_strings.state_supervision_contact_status_attempted
    COMPLETED = state_enum_strings.state_supervision_contact_status_completed
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSupervisionContactStatus"]:
        return _STATE_SUPERVISION_CONTACT_STATUS_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The status of the contact between the supervising officer and the "
            "person on supervision."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_CONTACT_STATUS_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_CONTACT_STATUS_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateSupervisionContactStatus.ATTEMPTED: "Used when the contact was attempted but "
    "not successfully completed (e.g. the supervising officer called the person on "
    "their phone, but the person did not answer).",
    StateSupervisionContactStatus.COMPLETED: "Used when the contact was successfully "
    "completed.",
}


@unique
class StateSupervisionContactLocation(StateEntityEnum):
    """The location at which the supervising officer interacted with the
    person on supervision."""

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
    LAW_ENFORCEMENT_AGENCY = (
        state_enum_strings.state_supervision_contact_location_law_enforcement_agency
    )
    PAROLE_COMMISSION = (
        state_enum_strings.state_supervision_contact_location_parole_commission
    )
    ALTERNATIVE_PLACE_OF_EMPLOYMENT = (
        state_enum_strings.state_supervision_contact_location_alternative_place_of_employment
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateSupervisionContactLocation"]:
        return _STATE_SUPERVISION_CONTACT_LOCATION_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The location at which the supervising officer interacted with the "
            "person on supervision."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SUPERVISION_CONTACT_LOCATION_VALUE_DESCRIPTIONS


_STATE_SUPERVISION_CONTACT_LOCATION_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateSupervisionContactLocation.ALTERNATIVE_PLACE_OF_EMPLOYMENT: "A location of employment "
    "for the person on supervision that is not the person’s primary employment.",
    StateSupervisionContactLocation.COURT: "A courtroom.",
    StateSupervisionContactLocation.FIELD: "Used when a supervising officer interacts "
    "with a person on supervision somewhere outside of an official office that does "
    "not fall into any of the other `StateSupervisionContactLocation` categories.",
    StateSupervisionContactLocation.JAIL: "A jail.",
    StateSupervisionContactLocation.LAW_ENFORCEMENT_AGENCY: "A law enforcement agency "
    "location (e.g. a police station).",
    StateSupervisionContactLocation.PAROLE_COMMISSION: "Used when a supervising "
    "officer made contact with an individual on supervision during a parole board "
    "hearing.",
    StateSupervisionContactLocation.PLACE_OF_EMPLOYMENT: "The location of employment "
    "for the person on supervision.",
    StateSupervisionContactLocation.RESIDENCE: "The residence of the person on "
    "supervision.",
    StateSupervisionContactLocation.SUPERVISION_OFFICE: "The office where the "
    "supervising officer works.",
    StateSupervisionContactLocation.TREATMENT_PROVIDER: "A location at which the "
    "person on supervision is receiving treatment.",
}

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
    "LAW ENFORCEMENT AGENCY": StateSupervisionContactLocation.LAW_ENFORCEMENT_AGENCY,
    "PAROLE COMMISSION": StateSupervisionContactLocation.PAROLE_COMMISSION,
    "ALTERNATIVE WORK SITE": StateSupervisionContactLocation.ALTERNATIVE_PLACE_OF_EMPLOYMENT,
    "ALTERNATIVE PLACE OF EMPLOYMENT": StateSupervisionContactLocation.ALTERNATIVE_PLACE_OF_EMPLOYMENT,
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
    "COLLATERAL": StateSupervisionContactType.COLLATERAL,
    "DIRECT": StateSupervisionContactType.DIRECT,
    "BOTH COLLATERAL AND DIRECT": StateSupervisionContactType.BOTH_COLLATERAL_AND_DIRECT,
}

_STATE_SUPERVISION_CONTACT_METHOD_MAP = {
    "EXTERNAL UNKNOWN": StateSupervisionContactMethod.EXTERNAL_UNKNOWN,
    "INTERNAL UNKNOWN": StateSupervisionContactMethod.INTERNAL_UNKNOWN,
    "TELEPHONE": StateSupervisionContactMethod.TELEPHONE,
    "WRITTEN MESSAGE": StateSupervisionContactMethod.WRITTEN_MESSAGE,
    "VIRTUAL": StateSupervisionContactMethod.VIRTUAL,
    "IN PERSON": StateSupervisionContactMethod.IN_PERSON,
}
