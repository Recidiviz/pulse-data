# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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

"""Constants shared across a few different entities."""
from enum import unique
from typing import Dict

from recidiviz.common.constants.state import (
    enum_canonical_strings as state_enum_strings,
)
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateActingBodyType(StateEntityEnum):
    """A type of actor or authoritative body within the criminal justice system."""

    COURT = state_enum_strings.state_acting_body_type_court
    PAROLE_BOARD = state_enum_strings.state_acting_body_type_parole_board
    SUPERVISION_OFFICER = state_enum_strings.state_acting_body_type_supervision_officer
    SENTENCED_PERSON = state_enum_strings.state_acting_body_type_sentenced_person
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateActingBodyType"]:
        return _STATE_ACTING_BODY_TYPE_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "A type of actor or authoritative body within the criminal justice system."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_ACTING_BODY_TYPE_VALUE_DESCRIPTIONS


_STATE_ACTING_BODY_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateActingBodyType.COURT: "A government entity authorized to resolve legal "
    "disputes. This term is often used to describe a single judge.",
    StateActingBodyType.PAROLE_BOARD: "A panel of people who decide whether an "
    "incarcerated individual should be released from prison onto parole after serving "
    "at least a minimum portion of their sentence as prescribed by the sentencing "
    "judge. This authoritative body also determines the conditions of one’s parole, "
    "and decides the outcomes of revocation hearings for individuals on parole.",
    StateActingBodyType.SENTENCED_PERSON: "An individual who is in the criminal "
    "justice system because they have been sentenced to some form of incarceration "
    "or supervision following a criminal conviction.",
    StateActingBodyType.SUPERVISION_OFFICER: "An official who oversees someone while "
    "they are on supervision. Also referred to as a probation/parole officer.",
}

_STATE_ACTING_BODY_TYPE_MAP = {
    "COURT": StateActingBodyType.COURT,
    "PAROLE BOARD": StateActingBodyType.PAROLE_BOARD,
    "SUPERVISION OFFICER": StateActingBodyType.SUPERVISION_OFFICER,
    "SENTENCED PERSON": StateActingBodyType.SENTENCED_PERSON,
    "INTERNAL UNKNOWN": StateActingBodyType.INTERNAL_UNKNOWN,
    "EXTERNAL UNKNOWN": StateActingBodyType.EXTERNAL_UNKNOWN,
}


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
@unique
class StateCustodialAuthority(StateEntityEnum):
    """The type of government entity directly responsible for the person on a period
    of incarceration or supervision. Generally the entity of the agent who is filling \
    out the paperwork and making recommendations for the person. This is not
    necessarily the decision-making authority on the period."""

    # TODO(#12648): Rename to COUNTY
    COURT = state_enum_strings.state_custodial_authority_court
    FEDERAL = state_enum_strings.state_custodial_authority_federal
    OTHER_COUNTRY = state_enum_strings.state_custodial_authority_other_country
    OTHER_STATE = state_enum_strings.state_custodial_authority_other_state
    SUPERVISION_AUTHORITY = (
        state_enum_strings.state_custodial_authority_supervision_authority
    )
    STATE_PRISON = state_enum_strings.state_custodial_authority_state_prison
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @staticmethod
    def _get_default_map() -> Dict[str, "StateCustodialAuthority"]:
        return _STATE_CUSTODIAL_AUTHORITY_MAP

    @classmethod
    def get_enum_description(cls) -> str:
        return (
            "The type of government entity directly responsible for the person "
            "on a period of incarceration or supervision. Generally the entity of "
            "the agent who is filling out the paperwork and making recommendations "
            "for the person. This is usually the field that is used to determine "
            "whether a person is counted in the state’s incarceration or supervision "
            "population. This is not necessarily the decision-making authority on "
            "the period. For example, when a person is on probation and under the "
            "custodial authority of the SUPERVISION_AUTHORITY of the state (since "
            "the person is regularly meeting with a supervision officer), the "
            "decision-making authority on that period is the sentencing judge (since "
            "the judge ultimately can revoke the supervision or grant early discharge, "
            "for instance). When a person is on parole, the decision-making authority "
            "on that period is the parole board, since that authoritative entity "
            "determines the conditions of the person’s supervision and has the power "
            "to revoke the parole."
        )

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_CUSTODIAL_AUTHORITY_VALUE_DESCRIPTIONS


_STATE_CUSTODIAL_AUTHORITY_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateCustodialAuthority.COURT: "Describes a county-level authority, "
    "usually a county jail entity. This is typically used when a person is being held "
    "in a county jail, and is under the authority of the county. It is rare to see "
    "this value on `StateSupervisionPeriod` entities. When used on "
    "`StateIncarcerationPeriod` entities, it usually indicates that we are "
    "receiving data directly from the county jail systems. If a person is being held "
    "in a county jail on behalf of the state prison (if, for example, the prison "
    "does not have room for everyone in their facilities) then we would see "
    "`StateCustodialAuthority.STATE_PRISON` and a "
    "`StateIncarcerationType.COUNTY_JAIL`. TODO(#12648): THIS WILL SOON BE RENAMED "
    "TO `COUNTY` .",
    StateCustodialAuthority.FEDERAL: "Represents some kind of federal authority.",
    StateCustodialAuthority.OTHER_COUNTRY: "Represents the authority of another "
    "country.",
    StateCustodialAuthority.OTHER_STATE: "Represents the authority of a state that "
    "is different than the state from which we received this data.",
    StateCustodialAuthority.STATE_PRISON: "Represents the authority of the state "
    "department of corrections. This value is seen on most `StateIncarcerationPeriod` "
    "entities, as most periods of incarceration in the data we receive from state "
    "department of corrections are overseen by the state prison authorities. This "
    "value is used on `StateSupervisionPeriod` entities when a person has an open "
    "supervision period while they are in custody in a state prison.",
    StateCustodialAuthority.SUPERVISION_AUTHORITY: "Describes the state-wide authority "
    "that administers supervision. In all states this entity is responsible for "
    "administering parole. In some states, where probation is administered at the "
    "state-level, this entity also oversees probation periods. In this context it "
    "means that the person responsible for the regular paperwork and oversight of "
    "the individual is a supervision officer. This value is seen on most "
    "`StateSupervisionPeriod` entities. It is possible, however, for a "
    "`StateIncarcerationPeriod`  to have a `custodial_authority` value of "
    "`SUPERVISION_AUTHORITY`. For example, this is used in US_PA when an "
    "individual is in a parole violator center, because the person responsible for "
    "the daily paperwork and oversight of the person is a supervision officer.",
}


_STATE_CUSTODIAL_AUTHORITY_MAP = {
    "COURT": StateCustodialAuthority.COURT,
    "EXTERNAL UNKNOWN": StateCustodialAuthority.EXTERNAL_UNKNOWN,
    "FEDERAL": StateCustodialAuthority.FEDERAL,
    "INTERNAL UNKNOWN": StateCustodialAuthority.INTERNAL_UNKNOWN,
    "OTHER COUNTRY": StateCustodialAuthority.OTHER_COUNTRY,
    "OTHER STATE": StateCustodialAuthority.OTHER_STATE,
    "SUPERVISION AUTHORITY": StateCustodialAuthority.SUPERVISION_AUTHORITY,
    "STATE PRISON": StateCustodialAuthority.STATE_PRISON,
}
