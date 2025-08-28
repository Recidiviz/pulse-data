# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Helpers for gating ingest-related features."""

from enum import Enum

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

# TODO(#12390): This data structure can be removed when automatic
# raw data pruning is rolled out.
# Affectionately known as "extremely bad, lossy pruning", states that
# undergo this process have the the earliest and most recent raw data rows
# to avoid moving around very large amounts of data.
MANUAL_RAW_DATA_PRUNING_STATES = {
    StateCode.US_TN,
    StateCode.US_MI,
    StateCode.US_ND,
    StateCode.US_AR,
}


class RawDataPruningExemptionReason(Enum):
    """
    Enumerates reasons why a file is exempt from raw data pruning.
    If your use case is not covered by one of these reasons, please add a new
    enum value to facilitate discussion in your PR.
    """

    USED_AS_ALL_DEPENDENCY = (
        "This file is used in an ingest view with the @ALL filter type."
    )

    MIXED_INCREMENTAL_AND_HISTORICAL = (
        "This file is a mix of incremental and historical data in the transfer history, "
        "which must be addressed before including in raw data pruning."
    )


FILES_EXEMPT_FROM_RAW_DATA_PRUNING_BY_STATE: dict[
    StateCode, dict[str, RawDataPruningExemptionReason]
] = {
    StateCode.US_IA: {
        "IA_DOC_MAINT_Staff": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "IA_DOC_MAINT_StaffWorkUnits": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
    },
    StateCode.US_IX: {
        "employee": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "ref_Employee": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "RECIDIVIZ_REFERENCE_supervisor_roster": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
    },
    StateCode.US_NE: {
        "LOCATION_HIST": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "AggregateSentence": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "ORASClientRiskLevelAndNeeds": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
    },
    StateCode.US_OR: {
        "RCDVZ_PRDDTA_OP013P": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "RCDVZ_CISPRDDTA_CMCMST": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "RCDVZ_PRDDTA_OP054P": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
    },
    StateCode.US_ND: {
        # TODO(#33357): Account for these incrementals before rolling out automatic raw data pruning.
        "docstars_offenders": RawDataPruningExemptionReason.MIXED_INCREMENTAL_AND_HISTORICAL,
    },
    StateCode.US_TN: {
        "Address": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "Diversion": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "ISCSentence": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "JOCharge": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "RECIDIVIZ_REFERENCE_staff_supervisor_and_caseload_roster": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "Sentence": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "Staff": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "StaffEmailByAlias": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
    },
    StateCode.US_PA: {
        "RECIDIVIZ_REFERENCE_staff_roster": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
    },
    StateCode.US_MI: {
        "RECIDIVIZ_REFERENCE_leadership_roster": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "ADH_OFFENDER_SENTENCE": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "ADH_LEGAL_ORDER": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "COMS_Security_Classification": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "ADH_OFFENDER_ASSESSMENT": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
    },
    StateCode.US_AR: {
        "INTERVENTSANCTION": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "OFFENDERPROFILE": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
    },
    StateCode.US_UT: {
        "hrchy": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "est_dt": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
        "est_dt_hist": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
    },
    StateCode.US_NY: {
        "UNDER_CUSTODY": RawDataPruningExemptionReason.USED_AS_ALL_DEPENDENCY,
    },
}


def build_raw_data_pruning_exemption_error_message(
    filter_type: str, file_tag: str, state_code: StateCode
) -> str:
    error_msg = (
        f"Raw data file [{file_tag}] is an ingest view dependency with the "
        f"[{filter_type}] filter type. This cannot be used until it has been "
        "explicitly marked as exempt from raw data pruning. This ensures that the correct "
        "view of the data is avaiable to you in the future!"
        "\nPlease add this file to FILES_EXEMPT_FROM_RAW_DATA_PRUNING_BY_STATE in "
        "ingest/direct/gating.py to exempt it from raw data pruning. "
    )
    if state_code in MANUAL_RAW_DATA_PRUNING_STATES:
        return error_msg + (
            f"\nWARNING: Because {state_code.value} goes through manual raw data pruning, "
            "you may need to fully re-import this file's history to ensure all data is avaliable.\n"
        )
    return error_msg


def automatic_raw_data_pruning_enabled_for_state_and_instance(
    state_code: StateCode,
    instance: DirectIngestInstance,  # pylint: disable=unused-argument
) -> bool:
    if state_code in MANUAL_RAW_DATA_PRUNING_STATES:
        return False
    return False
