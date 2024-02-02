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
"""Custom enum parsers functions for US_AZ. Can be referenced in an ingest view manifest
like this:

my_enum_field:
  $enum_mapping:
    $raw_text: MY_CSV_COL
    $custom_parser: us_az_custom_enum_parsers.<function name>
"""
from typing import Optional

from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodCustodyLevel,
    StateIncarcerationPeriodHousingUnitCategory,
    StateIncarcerationPeriodHousingUnitType,
)
from recidiviz.common.constants.state.state_person import StateEthnicity


def parse_ethnicity(
    ## Some ethnicities are only included as a race, so we pipe the race field into this
    ## parser to set ethnicities appropriately.
    raw_text: str,
) -> Optional[StateEthnicity]:
    hispanic_options = [
        "Mexican American",
        "Mexican National",
        "Cuban",
        "Puerto Rican",
        "Peru",
        "Spain",
        "Panama",
        "Boliva",
    ]
    if raw_text:
        ethnicity = raw_text.split("##")[0]
        race = raw_text.split("##")[1]
        if ethnicity in hispanic_options or race in hispanic_options:
            return StateEthnicity.HISPANIC
        return StateEthnicity.NOT_HISPANIC
    return StateEthnicity.INTERNAL_UNKNOWN


def parse_housing_unit_category(
    raw_text: str,
) -> Optional[StateIncarcerationPeriodHousingUnitCategory]:
    """Parse housing unit categories based on current use and custody level of housing unit.
    TODO(#27201): Clarify housing unit, custody level, and "current use" specifics with AZ."""
    if raw_text:
        if raw_text in (
            "Detention-Medical",
            "Detention-Detention",
            "Close-Detention",
            "Intake-Detention",
            "Maximum-Detention",
            "Detention-Mental Health",
            "Close-Protective Custody",
            "Medium-Protective Custody",
            "Maximum-Protective Custody",
            "Minimum-Protective Custody",
            "Detention-Transitory",
        ):
            return StateIncarcerationPeriodHousingUnitCategory.SOLITARY_CONFINEMENT
        return StateIncarcerationPeriodHousingUnitCategory.GENERAL
    return None


def parse_housing_unit_type(
    raw_text: str,
) -> Optional[StateIncarcerationPeriodHousingUnitType]:
    """Parse housing types categories based on current use and custody level of housing unit.
    TODO(#27201): Clarify housing unit, custody level, and "current use" specifics with AZ.
    """
    if raw_text:
        if raw_text in (
            "Intake-Transitory",  # Temporary solitary confinement?
            "Close-Close Management",
            "Close-Sex Offender",
            "Close-Transitory",  # Temporary solitary confinement?
            "Close-Mental Health",  # Mental health solitary confinement?
            "Close-General Population",
            "Minimum-Sex Offender",
            "Minimum-Transitory",  # Temporary solitary confinement?
            "Minimum-DUI",
            "Minimum-General Population",
            "Maximum-General Population",
            "Medium-Transitory",  # Temporary solitary confinement?
            "Medium-Sex Offender",
            "Maximum-Sex Offender",
            "Maximum-Return to Custody",
            "Maximum-Transitory",  # Temporary solitary confinement?
            "Medium-General Population",
            "Maximum-Mental Health",  # Mental health solitary confinement?
            "Medium-Mental Health",  # Mental health solitary confinement?
        ):
            return StateIncarcerationPeriodHousingUnitType.GENERAL

        if raw_text in (
            "Close-Medical",
            "Medium-Medical",
            "Maximum-Medical",
            "Detention-Medical",
        ):
            return StateIncarcerationPeriodHousingUnitType.HOSPITAL

        if raw_text in (
            "Detention-Detention",
            "Close-Detention",
            "Intake-Detention",
            "Maximum-Detention",
        ):
            return (
                StateIncarcerationPeriodHousingUnitType.DISCIPLINARY_SOLITARY_CONFINEMENT
            )
        if raw_text == "Detention-Mental Health":
            return (
                StateIncarcerationPeriodHousingUnitType.MENTAL_HEALTH_SOLITARY_CONFINEMENT
            )
        if raw_text in (
            "Close-Protective Custody",
            "Medium-Protective Custody",
            "Maximum-Protective Custody",
            "Minimum-Protective Custody",
        ):
            return StateIncarcerationPeriodHousingUnitType.PROTECTIVE_CUSTODY

        if raw_text == "Detention-Transitory":
            return (
                StateIncarcerationPeriodHousingUnitType.TEMPORARY_SOLITARY_CONFINEMENT
            )
        return StateIncarcerationPeriodHousingUnitType.INTERNAL_UNKNOWN
    return None


def parse_custody_level(
    raw_text: str,
) -> Optional[StateIncarcerationPeriodCustodyLevel]:
    """Parse custody level based on current use and documented custody level of housing unit."""
    if raw_text:
        if raw_text == "Intake-Transitory":
            return StateIncarcerationPeriodCustodyLevel.INTAKE
        if raw_text in (
            "Minimum-Sex Offender",
            "Minimum-Transitory",
            "Minimum-DUI",
            "Minimum-General Population",
        ):
            return StateIncarcerationPeriodCustodyLevel.MINIMUM

        if raw_text in (
            "Medium-Transitory",
            "Medium-Medical",
            "Medium-Mental Health",
            "Medium-General Population",
            "Medium-Sex Offender",
        ):
            return StateIncarcerationPeriodCustodyLevel.MEDIUM
        if raw_text in (
            "Maximum-Sex Offender",
            "Maximum-Medical",
            "Maximum-Return to Custody",
            "Maximum-Mental Health",
            "Maximum-Transitory",
            "Maximum-General Population",
        ):
            return StateIncarcerationPeriodCustodyLevel.MAXIMUM
        if raw_text in (
            "Close-Close Management",
            "Close-Sex Offender",
            "Close-Transitory",
            "Close-Mental Health",
            "Close-Medical",
            "Close-General Population",
        ):
            return StateIncarcerationPeriodCustodyLevel.CLOSE

        if raw_text in (
            "Detention-Medical",
            "Detention-Detention",
            "Close-Detention",
            "Intake-Detention",
            "Maximum-Detention",
            "Detention-Mental Health",
            "Close-Protective Custody",
            "Medium-Protective Custody",
            "Maximum-Protective Custody",
            "Minimum-Protective Custody",
            "Detention-Transitory",
        ):
            # This relies on the assumption that a documented custody level of "detention"
            # OR a unit's current use being "Detention" or "Protective Custody"
            # means a person in that unit is in restrictive housing (solitary).
            # TODO(#27201): Confirm with AZ.
            return StateIncarcerationPeriodCustodyLevel.SOLITARY_CONFINEMENT
        return StateIncarcerationPeriodCustodyLevel.INTERNAL_UNKNOWN
    return None
