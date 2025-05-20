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
"""Defines which person external_id types should be used in various situations for each
state.
"""
from recidiviz.common.constants.state.external_id_types import (
    US_AR_OFFENDERID,
    US_AZ_ADC_NUMBER,
    US_AZ_PERSON_ID,
    US_CA_CDCNO,
    US_CA_DOC,
    US_CO_PID,
    US_IA_OFFENDERCD,
    US_ID_DOC,
    US_IX_DOC,
    US_MA_COMMIT_NO,
    US_ME_DOC,
    US_MI_DOC,
    US_MO_DOC,
    US_ND_ELITE,
    US_ND_SID,
    US_NE_ID_NBR,
    US_OR_ID,
    US_PA_CONT,
    US_PA_PBPP,
    US_TN_DOC,
    US_TX_SID,
    US_UT_DOC,
)
from recidiviz.common.constants.state.state_system_type import StateSystemType
from recidiviz.common.constants.states import StateCode

# The "stable" ids should generally only have one value per real world person. If there
# is more than one value, we will pick the alphabetically lowest (for now, until we have
# a better solution).
_INCARCERATION_PRODUCT_STABLE_PERSON_EXTERNAL_ID_TYPES_BY_STATE = {
    StateCode.US_AR: US_AR_OFFENDERID,
    StateCode.US_AZ: US_AZ_PERSON_ID,
    StateCode.US_CA: US_CA_DOC,
    StateCode.US_CO: US_CO_PID,
    StateCode.US_IA: US_IA_OFFENDERCD,
    StateCode.US_ID: US_ID_DOC,
    StateCode.US_IX: US_IX_DOC,
    StateCode.US_MA: US_MA_COMMIT_NO,
    StateCode.US_ME: US_ME_DOC,
    StateCode.US_MI: US_MI_DOC,
    StateCode.US_MO: US_MO_DOC,
    StateCode.US_ND: US_ND_ELITE,
    StateCode.US_OR: US_OR_ID,
    StateCode.US_PA: US_PA_CONT,
    StateCode.US_TN: US_TN_DOC,
    StateCode.US_TX: US_TX_SID,
    StateCode.US_UT: US_UT_DOC,
}

_SUPERVISION_PRODUCT_STABLE_PERSON_EXTERNAL_ID_TYPES_BY_STATE = {
    StateCode.US_AR: US_AR_OFFENDERID,
    StateCode.US_AZ: US_AZ_PERSON_ID,
    StateCode.US_CA: US_CA_DOC,
    StateCode.US_IA: US_IA_OFFENDERCD,
    StateCode.US_ID: US_ID_DOC,
    StateCode.US_IX: US_IX_DOC,
    StateCode.US_ME: US_ME_DOC,
    StateCode.US_MI: US_MI_DOC,
    StateCode.US_MO: US_MO_DOC,
    StateCode.US_NE: US_NE_ID_NBR,
    StateCode.US_ND: US_ND_SID,
    StateCode.US_OR: US_OR_ID,
    StateCode.US_PA: US_PA_PBPP,
    StateCode.US_TN: US_TN_DOC,
    StateCode.US_TX: US_TX_SID,
    StateCode.US_UT: US_UT_DOC,
}


# The "display" id type might have more than one of a given type and we'll use the new
# is_current_display_id_for_type flag to prioritize
_INCARCERATION_PRODUCT_DISPLAY_PERSON_EXTERNAL_ID_TYPES_OVERRIDES = {
    StateCode.US_AZ: US_AZ_ADC_NUMBER,
}

_SUPERVISION_PRODUCT_DISPLAY_PERSON_EXTERNAL_ID_TYPES_OVERRIDES = {
    StateCode.US_CA: US_CA_CDCNO,
}


def _format_state_code_to_id_type_query(
    state_code_to_type_map: dict[StateCode, str]
) -> str:
    clauses = [
        f'SELECT "{state_code.value}" AS state_code, "{id_type}" AS id_type'
        for state_code, id_type in state_code_to_type_map.items()
    ]
    return "\nUNION ALL\n".join(clauses)


def get_product_stable_person_external_id_types_by_state(
    system_type: StateSystemType,
) -> dict[StateCode, str]:
    """Returns a dictionary mapping states to the external id type that should be used
    as a "stable" person external id type for all people in this state, in products with
    the given |system_type|. Ids of this type are sent to the product frontend and used
    to track persistent user state, so this id would ideally never change for a person
    over time.
    """
    match system_type:
        case StateSystemType.INCARCERATION:
            return _INCARCERATION_PRODUCT_STABLE_PERSON_EXTERNAL_ID_TYPES_BY_STATE
        case StateSystemType.SUPERVISION:
            return _SUPERVISION_PRODUCT_STABLE_PERSON_EXTERNAL_ID_TYPES_BY_STATE
        case StateSystemType.INTERNAL_UNKNOWN:
            raise ValueError(f"Unexpected system_type {system_type}")


def get_product_display_person_external_id_types_by_state(
    system_type: StateSystemType,
) -> dict[StateCode, str]:
    """Returns a dictionary mapping states to the external id type that should be used
    as a display person external id type for all people in this state, in products with
    the given |system_type|. Ids of this type are displayed in product UIs to identify
    a person to one of our users.
    """
    match system_type:
        case StateSystemType.INCARCERATION:
            return {
                **_INCARCERATION_PRODUCT_STABLE_PERSON_EXTERNAL_ID_TYPES_BY_STATE,
                **_INCARCERATION_PRODUCT_DISPLAY_PERSON_EXTERNAL_ID_TYPES_OVERRIDES,
            }
        case StateSystemType.SUPERVISION:
            return {
                **_SUPERVISION_PRODUCT_STABLE_PERSON_EXTERNAL_ID_TYPES_BY_STATE,
                **_SUPERVISION_PRODUCT_DISPLAY_PERSON_EXTERNAL_ID_TYPES_OVERRIDES,
            }
        case _:
            raise ValueError(f"Unexpected system_type {system_type}")


# TODO(#41554): Use this to build a view analogous to
#  `product_display_person_external_ids` and use that view everywhere we want to
#  select a stable external id of a given type to use in products.
def product_stable_external_id_types_query(system_type: StateSystemType) -> str:
    """Returns a query mapping states (state_code) to the external id type (id_type)
    that should be used as a "stable" person external id type for all people in this
    state, in products with the given |system_type|. Ids of this type are sent to the
    product frontend and used to track persistent user state, so this id would ideally
    never change for a person over time.
    """
    types_by_state_map = get_product_stable_person_external_id_types_by_state(
        system_type
    )
    return _format_state_code_to_id_type_query(types_by_state_map)


def product_display_external_id_types_query(system_type: StateSystemType) -> str:
    """Returns a query mapping states (state_code) to the external id type (id_type)
    that should be used as a display person external id type for all people in this
    state, in products with the given |system_type|. Ids of this type are displayed in
    product UIs to identify a person to one of our users.
    """
    types_by_state_map = get_product_display_person_external_id_types_by_state(
        system_type
    )
    return _format_state_code_to_id_type_query(types_by_state_map)
