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
    if sup_level in ("9AB", "ZAB", "ZAC", "ZAP") or sup_type == "ABS":
        return StateSupervisionPeriodSupervisionType.ABSCONSION
    if sup_level in ("9WR", "NIA", "WRB", "WRT", "ZWS"):
        return StateSupervisionPeriodSupervisionType.BENCH_WARRANT
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
    ) and (
        sup_type in ("UNP", "DET", "SAI", "DIV", "INA", "MIS", "INT", "PPO")
        or (sup_type in ("COM", "ISC") and assign_type == "PRO")
    ):
        return StateSupervisionPeriodSupervisionType.PROBATION
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
    ) and (
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
