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
import re
from typing import Optional

from recidiviz.common.constants.state.state_incarceration_incident import (
    StateIncarcerationIncidentOutcomeType,
    StateIncarcerationIncidentType,
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodHousingUnitCategory,
    StateIncarcerationPeriodHousingUnitType,
)
from recidiviz.common.constants.state.state_person import StateEthnicity
from recidiviz.common.constants.state.state_staff_caseload_type import (
    StateStaffCaseloadType,
)
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
)


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


def parse_incident_type(raw_text: str) -> Optional[StateIncarcerationIncidentType]:
    """Parse incarceration incident types based on description of rule violation."""
    if raw_text:
        if raw_text in (
            "POSSESSION OF DRUGS OR NARCOTICS"
            "POSSESSION OR MANUFACTURE OF INTOXICATING SUBSTANCE"
            "PROMOTING PRISON CONTRABAND"
            "POSSESSION OF DRUG PARAPHERNALIA"
            "POSSESSION OF A WEAPON"
            "POSSESSION OF COMMUNICATION DEVICE"
            "POSSESSION OF MINOR OR NUISANCE CONTRABAND"
        ):
            return StateIncarcerationIncidentType.CONTRABAND
        if raw_text == "DISORDERLY CONDUCT":
            return StateIncarcerationIncidentType.DISORDERLY_CONDUCT
        if raw_text == "ESCAPE":
            return StateIncarcerationIncidentType.ESCAPE
        if raw_text in (
            "RESISTING OR DISOBEYING A VERBAL OR WRITTEN ORDER"
            "DISRUPTING AN INSTITUTION COUNT AND/OR BEING OUT OF PLACE"
            "VIOLATION OF ANY PUBLISHED DEPARTMENT OR INSTITUTION RULE"
            "VIOLATION OF VISITATION RULES"
            "OBSTRUCTING STAFF"
            '"TATTOOING, BRANDS, SCARIFICATIONS AND PIERCING"'
            "DISRESPECT TO STAFF"
            "FALSE REPORTING"
            "FAILURE TO MAINTAIN SANITATION REQUIREMENTS"
            "FAILURE TO MAINTAIN GROOMING REQUIREMENTS"
            "SMOKING OR USE OF TOBACCO IN AN UNAUTHORIZED AREA"
        ):
            return StateIncarcerationIncidentType.MINOR_OFFENSE
        if raw_text in (
            "ARSON",
            "FIGHTING",
            "ASSAULT ON INMATE",
            "ASSAULT ON STAFF THAT DID NOT INVOLVE SERIOUS INJURY",
            "AGGRAVATED ASSAULT (INMATE ON INMATE)",
            "ASSAULT ON STAFF (THAT INVOLVED SERIOUS INJURY)",
            "MANSLAUGHTER",
            "THREATENING OR INTIMIDATING",
            "TAMPERING WITH SECURITY OR SAFETY DEVICES",
        ):
            return StateIncarcerationIncidentType.VIOLENCE
        return StateIncarcerationIncidentType.REPORT
    return StateIncarcerationIncidentType.PRESENT_WITHOUT_INFO


def parse_penalty_type(
    raw_text: str,
) -> Optional[StateIncarcerationIncidentOutcomeType]:
    penalty_type, penalty_type_free_text = str.split(raw_text, sep="@@")
    if penalty_type:
        if penalty_type in ("LOSS OF PRIVILEGE", "LOSS OF VISITS", "NON-CONTACT"):
            return StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS
        if penalty_type == "EXTRA DUTY":
            return StateIncarcerationIncidentOutcomeType.DISCIPLINARY_LABOR
        if penalty_type == "RESTITUTION":
            return StateIncarcerationIncidentOutcomeType.FINANCIAL_PENALTY
        if penalty_type == "EARNED RELEASE CREDITS":
            return StateIncarcerationIncidentOutcomeType.GOOD_TIME_LOSS
        return StateIncarcerationIncidentOutcomeType.INTERNAL_UNKNOWN
    # When the only outcome is a verbal reprimand, there is often no penalty type listed,
    # and only a note in the free text field.
    if re.search("REPRIMAND", str.upper(penalty_type_free_text)):
        return StateIncarcerationIncidentOutcomeType.WARNING
    # If there is no penalty listed and no verbal reprimand documented in the free text
    # field, assume the report was filed and dismissed without any other outcome.
    return StateIncarcerationIncidentOutcomeType.DISMISSED


def parse_staff_caseload_type(raw_text: str) -> Optional[StateStaffCaseloadType]:
    """Parses StateStaffCaseloadType enum values based on a staff member's location,
    since many specialized caseloads are denoted in location fields. We currently assume
    that that is the only place that specialized caseloads are denoted, so any officers
    who do not have a specialized value in their location have a general caseload."""
    if raw_text:
        if raw_text.upper() in (
            "3",  # "ELECTRONIC MONITORING UNIT"
            "36",  # "TUCSON ELECTRONIC MONITORING UNIT"
        ):
            return StateStaffCaseloadType.ELECTRONIC_MONITORING
        if raw_text.upper() == "16":  # "SEX OFFENDER COORDINATION UNIT"
            return StateStaffCaseloadType.SEX_OFFENSE
        if raw_text.upper() == "21":  # "WARRANT SERVICE AND HEARINGS UNIT"
            return StateStaffCaseloadType.OTHER_COURT
        if raw_text.upper() in (
            "25",  # "SPECIAL SUPERVISION UNIT"
            "4",  # "INTERSTATE COMPACT UNIT"
            "27",  # "ADULT ADMINISTRATOR OFFICE"
        ):
            return StateStaffCaseloadType.INTERNAL_UNKNOWN
        return StateStaffCaseloadType.GENERAL
    return StateStaffCaseloadType.INTERNAL_UNKNOWN


def parse_supervision_level(
    supervision_level: str, admission_reason: str
) -> Optional[StateSupervisionLevel]:
    """Parses a person's supervision level based first on whether the admission reason
    for the given subspan means they have absconded or are in custody, and if not, based on
    their stated supervision level."""
    if admission_reason == "Releasee Abscond":
        return StateSupervisionLevel.ABSCONSION
    if admission_reason in ("Temporary Placement", "In Custody - Other"):
        return StateSupervisionLevel.IN_CUSTODY

    # If neither of the above cases are true, then base supervision level off of the
    # supervision_level field directly.
    if supervision_level:
        if supervision_level == "MIN":
            return StateSupervisionLevel.MINIMUM
        if supervision_level == "MED":
            return StateSupervisionLevel.MEDIUM
        if supervision_level == "INT":
            return StateSupervisionLevel.HIGH
        if supervision_level == "MAX":
            return StateSupervisionLevel.MAXIMUM
        if supervision_level == "UNK":
            return StateSupervisionLevel.EXTERNAL_UNKNOWN
        return StateSupervisionLevel.INTERNAL_UNKNOWN

    # If none of the above cases are true, we do not know this person's supervision level.
    return StateSupervisionLevel.PRESENT_WITHOUT_INFO
