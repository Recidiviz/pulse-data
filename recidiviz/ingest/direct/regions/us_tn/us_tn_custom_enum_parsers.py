# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Custom enum parsers functions for US_TN. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_tn_custom_enum_parsers.<function name>
"""
from recidiviz.common.constants.state.state_program_assignment import (
    StateProgramAssignmentParticipationStatus,
)
from recidiviz.common.constants.state.state_staff_caseload_type import (
    StateStaffCaseloadType,
)
from recidiviz.common.constants.state.state_staff_role_period import (
    StateStaffRoleSubtype,
    StateStaffRoleType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionPeriodSupervisionType,
)


def supervision_type_from_fields(
    raw_text: str,
) -> StateSupervisionPeriodSupervisionType:
    """Parse supervision type based on ranking of supervision level, supervision type and assignment type"""
    sup_level, assign_type, sup_type = raw_text.split("-")
    if sup_level in ("9DP"):
        return StateSupervisionPeriodSupervisionType.DEPORTED
    if sup_level in ("9AB", "NIA", "ZAB", "ZAC", "ZAP") or sup_type == "ABS":
        return StateSupervisionPeriodSupervisionType.ABSCONSION
    if sup_level in ("9WR", "WRT", "ZWS"):
        return StateSupervisionPeriodSupervisionType.WARRANT_STATUS
    if sup_level not in ("9AB", "ZAB", "ZAC", "ZAP", "9WR", "NIA", "WRT", "ZWS") and (
        sup_type in ("UNP", "DET", "SAI", "DIV", "INA", "MIS", "INT", "PPO")
        or (sup_type in ("COM", "ISC") and assign_type == "PRO")
    ):
        return StateSupervisionPeriodSupervisionType.PROBATION
    if sup_level not in ("9AB", "ZAB", "ZAC", "ZAP", "9WR", "NIA", "WRT", "ZWS",) and (
        sup_type in ("MAN", "TNP")
        or (sup_type in ("COM", "ISC") and assign_type == "PAO")
    ):
        return StateSupervisionPeriodSupervisionType.PAROLE
    if sup_level not in (
        "9AB",
        "ZAB",
        "ZAC",
        "ZAP",
        "9WR",
        "NIA",
        "WRB",
        "WRT",
        "ZWS",
    ) and (sup_type == "CCO" or (sup_type in ("COM", "ISC") and assign_type == "CCC")):
        return StateSupervisionPeriodSupervisionType.COMMUNITY_CONFINEMENT
    return StateSupervisionPeriodSupervisionType.INTERNAL_UNKNOWN


def staff_role_type_from_staff_title(
    raw_text: str,
) -> StateStaffRoleType:
    if raw_text in ("PRBM", "PRBO", "PRBP", "PARO", "PAOS"):
        return StateStaffRoleType.SUPERVISION_OFFICER

    return StateStaffRoleType.INTERNAL_UNKNOWN


def staff_role_subtype_from_staff_title(
    raw_text: str,
) -> StateStaffRoleSubtype:
    if raw_text in ("PAOS"):
        return StateStaffRoleSubtype.SUPERVISION_OFFICER_SUPERVISOR

    if raw_text in ("PRBM", "PRBO", "PRBP", "PARO"):
        return StateStaffRoleSubtype.SUPERVISION_OFFICER

    return StateStaffRoleSubtype.INTERNAL_UNKNOWN


def program_participation_status(
    raw_text: str,
) -> StateProgramAssignmentParticipationStatus:
    """
    DISCHARGED_SUCCCESSFUL: CMP - Completed, SUC - Successful Completion, GED - GED Earned
    DISCHARGED_UNSUCCESSFUL: PCD - Poor Conduct, UNC - Unsuccessful Completion, RLD - Released
    DISCHARGED_OTHER: TRH - (Transfer, Revocation Hearing), DSP - Recommended on LIBK/LIBL,
                    TRN - Transferred, HLT - HEALTH, STR - START TAP RECOMMENDED PROGRAM,
                    CLC - CUSTODY LEVEL CHANGE
    DECEASED: DCD - Deceased
    REFUSED: STM - Self Termination
    """
    discharged_successful = {"CMP", "SUC", "GED"}
    discharged_unsuccessful = {"PCD", "UNC", "RLD"}
    discharged_other = {"TRH", "DSP", "TRN", "HLT", "STR", "CLC"}
    deceased = {"DCD"}
    refused = {"STM"}
    (
        start_date,
        end_date,
        recommendation_date,
        treatment_terminated_reason,
    ) = raw_text.split("@@")
    if start_date != "NONE" and end_date == "NONE":
        return StateProgramAssignmentParticipationStatus.IN_PROGRESS
    if start_date == "NONE" and end_date == "NONE" and recommendation_date != "NONE":
        return StateProgramAssignmentParticipationStatus.PENDING
    if start_date == "NONE" and end_date == "NONE" and recommendation_date == "NONE":
        return StateProgramAssignmentParticipationStatus.PRESENT_WITHOUT_INFO
    if treatment_terminated_reason in discharged_successful:
        return StateProgramAssignmentParticipationStatus.DISCHARGED_SUCCESSFUL
    if treatment_terminated_reason in discharged_unsuccessful:
        return StateProgramAssignmentParticipationStatus.DISCHARGED_UNSUCCESSFUL
    if treatment_terminated_reason in discharged_other:
        return StateProgramAssignmentParticipationStatus.DISCHARGED_OTHER
    if treatment_terminated_reason in deceased:
        return StateProgramAssignmentParticipationStatus.DECEASED
    if treatment_terminated_reason in refused:
        return StateProgramAssignmentParticipationStatus.REFUSED
    return StateProgramAssignmentParticipationStatus.INTERNAL_UNKNOWN


def parse_staff_caseload_type(
    raw_text: str,
) -> StateStaffCaseloadType:
    """Assign staff caseload type based on raw text strings"""
    # putting this first because if it comes after all SCU Admin cases would be assigned to Sex Offense caseload
    if any(keyword in raw_text for keyword in ["ADMIN", "ABSCONDER"]):
        return StateStaffCaseloadType.TRANSITIONAL
    ##  this is second because we want to ensure sex offense caseloads will be viewed seperately
    if any(keyword in raw_text for keyword in ["SEX", "SCU"]):
        return StateStaffCaseloadType.SEX_OFFENSE
    if any(keyword in raw_text for keyword in ["ROCS", "RECOVERY COURT"]):
        return StateStaffCaseloadType.DRUG_COURT
    if any(keyword in raw_text for keyword in ["REHAB", "DRUG", "RECOVERY", "DRC"]):
        return StateStaffCaseloadType.ALCOHOL_AND_DRUG
    if "COURT" in raw_text:
        return StateStaffCaseloadType.OTHER_COURT
    if (
        any(keyword in raw_text for keyword in ["COMPLIANT", "TELEPHONE"])
        or raw_text == "IOT"
    ):
        return StateStaffCaseloadType.ADMINISTRATIVE_SUPERVISION
    if any(
        keyword in raw_text
        for keyword in [
            "PPO",
            "PROB",
            "PAROLE",
            "REG",
            "INTAKE",  # short term caseload
            "PA",
            "CC2",  # correctional counselor 2
            "CCII",  # correctional counselor 2
            "PRO",
            "GENERAL",
            "MANAGER",  # mapping this here for now since sometimes managers have caseloads
            "PPM",  # Probation/Parole Manager - not typically caseload carrier but sometimes take them on
            "ICOT",  # compact cases managed same as other general cases
            "ISC",
            "INTRA-STATE",
        ]
    ):
        return StateStaffCaseloadType.GENERAL

    return StateStaffCaseloadType.INTERNAL_UNKNOWN
