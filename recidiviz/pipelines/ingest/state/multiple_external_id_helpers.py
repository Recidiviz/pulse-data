# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Defines functions that tell us which external_id types can have multiple of the same
type per person.
"""
from recidiviz.common.constants.state.external_id_types import (
    US_AR_PARTYID,
    US_AZ_PERSON_ID,
    US_CA_BADGE_NO,
    US_CA_CDCNO,
    US_CA_DOC,
    US_IX_CIS_EMPL_CD,
    US_IX_EMPLOYEE,
    US_IX_STAFF_ID,
    US_ME_EMPLOYEE,
    US_MI_COMPAS_USER,
    US_MI_DOC_BOOK,
    US_MI_OMNI_USER,
    US_ND_DOCSTARS_OFFICER,
    US_ND_ELITE,
    US_ND_ELITE_BOOKING,
    US_ND_SID,
    US_NE_ID_NBR,
    US_NE_PAROLE_STAFF_ID,
    US_PA_CONT,
    US_PA_INMATE,
    US_PA_PBPP,
    US_PA_PBPP_POSNO,
    US_TX_EMAIL,
    US_TX_STAFF_ID,
    US_TX_TDCJ,
)
from recidiviz.common.constants.states import StateCode


def person_external_id_types_with_allowed_multiples_per_person(
    state_code: StateCode,
) -> set[str]:
    """Returns the person external_id id_types where we expect / allow that a single
    StatePerson has multiple StatePersonExternalId of this type.
    """

    # DO NOT ADD STATE / ID TYPES TO THIS UNLESS YOU FEEL CONFIDENT THAT IT'S "EXPECTED"
    # FOR A PERSON TO HAVE MULTIPLE IDS OF THE GIVEN TYPE (i.e. they get assigned a new
    # id for every new interaction with the system). IF ONLY A HANDFUL OF PEOPLE HAVE
    # DUPLICATES, IT'S LIKELY A DATA ENTRY ERROR AND YOU SHOULD FIX VIA RAW DATA
    # MIGRATIONS OR BY FILTERING OUT THE RAW DATA.
    allowed_types_by_state = {
        StateCode.US_CA: {US_CA_DOC, US_CA_CDCNO},
        StateCode.US_MI: {US_MI_DOC_BOOK},
        StateCode.US_ND: {US_ND_ELITE_BOOKING, US_ND_SID, US_ND_ELITE},
        StateCode.US_NE: {US_NE_ID_NBR},
        StateCode.US_PA: {US_PA_INMATE, US_PA_CONT, US_PA_PBPP},
        StateCode.US_TX: {US_TX_TDCJ},
    }

    return allowed_types_by_state.get(state_code, set())


def staff_external_id_types_with_allowed_multiples_per_person(
    state_code: StateCode,
) -> set[str]:
    """Returns the staff external_id id_types where we expect / allow that a single
    StateStaff has multiple StateStaffExternalId of this type.
    """

    # DO NOT ADD STATE / ID TYPES TO THIS UNLESS YOU FEEL CONFIDENT THAT IT'S "EXPECTED"
    # FOR A STAFF MEMBER TO HAVE MULTIPLE IDS OF THE GIVEN TYPE (i.e. they get assigned
    # a new id for every new stint of employment). IF ONLY A HANDFUL OF STAFF
    # HAVE DUPLICATES, IT'S LIKELY A DATA ENTRY ERROR AND YOU SHOULD FIX VIA RAW DATA
    # MIGRATIONS OR BY FILTERING OUT THE RAW DATA.
    allowed_types_by_state = {
        StateCode.US_AR: {US_AR_PARTYID},
        StateCode.US_AZ: {US_AZ_PERSON_ID},
        StateCode.US_CA: {US_CA_BADGE_NO},
        StateCode.US_IX: {US_IX_CIS_EMPL_CD, US_IX_EMPLOYEE, US_IX_STAFF_ID},
        StateCode.US_ME: {US_ME_EMPLOYEE},
        StateCode.US_MI: {US_MI_COMPAS_USER, US_MI_OMNI_USER},
        StateCode.US_ND: {US_ND_DOCSTARS_OFFICER},
        StateCode.US_NE: {US_NE_PAROLE_STAFF_ID},
        StateCode.US_PA: {US_PA_PBPP_POSNO},
        StateCode.US_TX: {US_TX_EMAIL, US_TX_STAFF_ID},
    }

    return allowed_types_by_state.get(state_code, set())
