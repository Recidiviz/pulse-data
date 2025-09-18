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

"""Constants related to a StateScheduledSupervisionContact."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


@unique
class StateScheduledSupervisionContactType(StateEntityEnum):
    COLLATERAL = state_enum_strings.state_scheduled_supervision_contact_type_collateral
    DIRECT = state_enum_strings.state_scheduled_supervision_contact_type_direct
    BOTH_COLLATERAL_AND_DIRECT = (
        state_enum_strings.state_scheduled_supervision_contact_type_both_collateral_and_direct
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The category of the scheduled contact."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SCHEDULED_SUPERVISION_CONTACT_TYPE_VALUE_DESCRIPTIONS


_STATE_SCHEDULED_SUPERVISION_CONTACT_TYPE_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateScheduledSupervisionContactType.BOTH_COLLATERAL_AND_DIRECT: "Used when the supervising "
    "officer will be contacting both the person on supervision and someone else (e.g. the "
    "person’s spouse, the person’s employer, etc.).",
    StateScheduledSupervisionContactType.COLLATERAL: "Used when the supervising officer will be "
    "contacting someone that is not the person on supervision (e.g. the person’s "
    "spouse, the person’s employer, etc.).",
    StateScheduledSupervisionContactType.DIRECT: "Used when the supervising officer will be "
    "contacting the person on supervision.",
}


@unique
class StateScheduledSupervisionContactMethod(StateEntityEnum):
    TELEPHONE = state_enum_strings.state_scheduled_supervision_contact_method_telephone
    WRITTEN_MESSAGE = (
        state_enum_strings.state_scheduled_supervision_contact_method_written_message
    )
    VIRTUAL = state_enum_strings.state_scheduled_supervision_contact_method_virtual
    IN_PERSON = state_enum_strings.state_scheduled_supervision_contact_method_in_person
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The way in which the supervising officer will interact with the person "
            "on supervision."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SCHEDULED_SUPERVISION_CONTACT_METHOD_VALUE_DESCRIPTIONS


_STATE_SCHEDULED_SUPERVISION_CONTACT_METHOD_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateScheduledSupervisionContactMethod.IN_PERSON: "Used when the supervising officer "
    "will see the person on supervision in person.",
    StateScheduledSupervisionContactMethod.TELEPHONE: "Used when the supervising officer "
    "will communicate with the person on supervision over the telephone.",
    StateScheduledSupervisionContactMethod.VIRTUAL: "Used when the supervising officer "
    "will communicat with the person on supervision virtually (e.g. a video call).",
    StateScheduledSupervisionContactMethod.WRITTEN_MESSAGE: "Used when the supervising officer "
    "will communicate with the person on supervision through some form of written "
    "communication (e.g. a letter).",
}


@unique
class StateScheduledSupervisionContactReason(StateEntityEnum):
    EMERGENCY_CONTACT = (
        state_enum_strings.state_scheduled_supervision_contact_reason_emergency_contact
    )
    GENERAL_CONTACT = (
        state_enum_strings.state_scheduled_supervision_contact_reason_general_contact
    )
    INITIAL_CONTACT = (
        state_enum_strings.state_scheduled_supervision_contact_reason_initial_contact
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The reason the supervising officer scheduled the contact with the person on supervision."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SCHEDULED_SUPERVISION_CONTACT_REASON_VALUE_DESCRIPTIONS


_STATE_SCHEDULED_SUPERVISION_CONTACT_REASON_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateScheduledSupervisionContactReason.EMERGENCY_CONTACT: "Used when the supervising "
    "officer will contact the person on supervision because of some kind of emergency.",
    StateScheduledSupervisionContactReason.GENERAL_CONTACT: "Used when the supervising officer "
    "will contact the person on supervision because of the general contact requirements "
    "of the person’s supervision conditions.",
    StateScheduledSupervisionContactReason.INITIAL_CONTACT: "Used when the supervising officer "
    "will contact the person on supervision because the person had just started on "
    "supervision.",
}


@unique
class StateScheduledSupervisionContactStatus(StateEntityEnum):
    DELETED = state_enum_strings.state_scheduled_supervision_contact_status_deleted
    SCHEDULED = state_enum_strings.state_scheduled_supervision_contact_status_scheduled
    PRESENT_WITHOUT_INFO = state_enum_strings.present_without_info
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The status of the scheduled contact between the supervising officer and the "
            "person on supervision."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SCHEDULED_SUPERVISION_CONTACT_STATUS_VALUE_DESCRIPTIONS


_STATE_SCHEDULED_SUPERVISION_CONTACT_STATUS_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateScheduledSupervisionContactStatus.DELETED: "Used when the contact was deleted and "
    "not successfully completed (e.g. the supervising officer scheduled a contact"
    "then deleted it to reschedule at a later time).",
    StateScheduledSupervisionContactStatus.SCHEDULED: "Used to represent a contact that is "
    "scheduled to occur in the future at the time the record was created. If the contact "
    "did occur, then a regular supervision contact entity would be used to capture this event.",
}


@unique
class StateScheduledSupervisionContactLocation(StateEntityEnum):
    """The location at which the supervising officer interacted with the
    person on supervision."""

    COURT = state_enum_strings.state_scheduled_supervision_contact_location_court
    FIELD = state_enum_strings.state_scheduled_supervision_contact_location_field
    JAIL = state_enum_strings.state_scheduled_supervision_contact_location_jail
    PLACE_OF_EMPLOYMENT = (
        state_enum_strings.state_scheduled_supervision_contact_location_place_of_employment
    )
    RESIDENCE = (
        state_enum_strings.state_scheduled_supervision_contact_location_residence
    )
    SUPERVISION_OFFICE = (
        state_enum_strings.state_scheduled_supervision_contact_location_supervision_office
    )
    TREATMENT_PROVIDER = (
        state_enum_strings.state_scheduled_supervision_contact_location_treatment_provider
    )
    LAW_ENFORCEMENT_AGENCY = (
        state_enum_strings.state_scheduled_supervision_contact_location_law_enforcement_agency
    )
    PAROLE_COMMISSION = (
        state_enum_strings.state_scheduled_supervision_contact_location_parole_commission
    )
    ALTERNATIVE_PLACE_OF_EMPLOYMENT = (
        state_enum_strings.state_scheduled_supervision_contact_location_alternative_place_of_employment
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The location at which the supervising officer will interact with the "
            "person on supervision."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_SCHEDULED_SUPERVISION_CONTACT_LOCATION_VALUE_DESCRIPTIONS


_STATE_SCHEDULED_SUPERVISION_CONTACT_LOCATION_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateScheduledSupervisionContactLocation.ALTERNATIVE_PLACE_OF_EMPLOYMENT: "A location of employment "
    "for the person on supervision that is not the person’s primary employment.",
    StateScheduledSupervisionContactLocation.COURT: "A courtroom.",
    StateScheduledSupervisionContactLocation.FIELD: "Used when a supervising officer will interact "
    "with a person on supervision somewhere outside of an official office that does "
    "not fall into any of the other `StateScheduledSupervisionContactLocation` categories.",
    StateScheduledSupervisionContactLocation.JAIL: "A jail.",
    StateScheduledSupervisionContactLocation.LAW_ENFORCEMENT_AGENCY: "A law enforcement agency "
    "location (e.g. a police station).",
    StateScheduledSupervisionContactLocation.PAROLE_COMMISSION: "Used when a supervising "
    "officer will make a contact with an individual on supervision during a parole board "
    "hearing.",
    StateScheduledSupervisionContactLocation.PLACE_OF_EMPLOYMENT: "The location of employment "
    "for the person on supervision.",
    StateScheduledSupervisionContactLocation.RESIDENCE: "The residence of the person on "
    "supervision.",
    StateScheduledSupervisionContactLocation.SUPERVISION_OFFICE: "The office where the "
    "supervising officer works.",
    StateScheduledSupervisionContactLocation.TREATMENT_PROVIDER: "A location at which the "
    "person on supervision is receiving treatment.",
}
