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

"""Constants related to the StateIncarcerationIncident entity."""
from enum import unique
from typing import Dict

import recidiviz.common.constants.state.enum_canonical_strings as state_enum_strings
from recidiviz.common.constants.state.state_entity_enum import StateEntityEnum


@unique
class StateIncarcerationIncidentType(StateEntityEnum):
    """Possible State Incarceration Incident types."""

    PRESENT_WITHOUT_INFO = state_enum_strings.present_without_info
    CONTRABAND = state_enum_strings.state_incarceration_incident_type_contraband
    DISORDERLY_CONDUCT = (
        state_enum_strings.state_incarceration_incident_type_disorderly_conduct
    )
    ESCAPE = state_enum_strings.state_incarceration_incident_type_escape
    MINOR_OFFENSE = state_enum_strings.state_incarceration_incident_type_minor_offense
    POSITIVE = state_enum_strings.state_incarceration_incident_type_positive
    REPORT = state_enum_strings.state_incarceration_incident_type_report
    VIOLENCE = state_enum_strings.state_incarceration_incident_type_violence
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The classification of an incident in a facility."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_INCARCERATION_INCIDENT_TYPE_VALUE_DESCRIPTIONS


_STATE_INCARCERATION_INCIDENT_TYPE_VALUE_DESCRIPTIONS: Dict[StateEntityEnum, str] = {
    StateIncarcerationIncidentType.CONTRABAND: "Used when a person is found with "
    "contraband, which is an item that is not allowed in the facility.",
    StateIncarcerationIncidentType.DISORDERLY_CONDUCT: "Used when a person is reported "
    "for some kind of disorderly conduct.",
    StateIncarcerationIncidentType.ESCAPE: "Used when a person is reported for "
    "escaping the facility.",
    StateIncarcerationIncidentType.MINOR_OFFENSE: "Used when a person is reported for "
    "committing a minor infraction in the facility.",
    StateIncarcerationIncidentType.POSITIVE: "Used when a report is submitted to "
    "document an instance of good behavior on behalf of a person.",
    StateIncarcerationIncidentType.REPORT: "Used when a report is submitted "
    "to document a negative incident that does not fall into any of the other "
    "categories.",
    StateIncarcerationIncidentType.VIOLENCE: "Used when a person is reported for "
    "violent behavior.",
}


@unique
class StateIncarcerationIncidentOutcomeType(StateEntityEnum):
    """Possible State Incarceration Incident outcome types."""

    # A form of confinement when a person cannot generally leave their own cell, regardless of who else occupies it
    CELL_CONFINEMENT = (
        state_enum_strings.state_incarceration_incident_outcome_cell_confinement
    )
    DISCIPLINARY_LABOR = (
        state_enum_strings.state_incarceration_incident_outcome_disciplinary_labor
    )
    DISMISSED = state_enum_strings.state_incarceration_incident_outcome_dismissed
    EXTERNAL_PROSECUTION = (
        state_enum_strings.state_incarceration_incident_outcome_external_prosecution
    )
    FINANCIAL_PENALTY = (
        state_enum_strings.state_incarceration_incident_outcome_financial_penalty
    )
    GOOD_TIME_LOSS = (
        state_enum_strings.state_incarceration_incident_outcome_good_time_loss
    )
    NOT_GUILTY = state_enum_strings.state_incarceration_incident_outcome_not_guilty
    PRIVILEGE_LOSS = (
        state_enum_strings.state_incarceration_incident_outcome_privilege_loss
    )
    # A form of confinement when a person is limited to certain areas of the facility, including their own cell/bunk
    RESTRICTED_CONFINEMENT = (
        state_enum_strings.state_incarceration_incident_outcome_restricted_confinement
    )
    # A form of confinement when a person cannot leave a separate solitary cell, generally at all
    SOLITARY = state_enum_strings.state_incarceration_incident_outcome_solitary
    TREATMENT = state_enum_strings.state_incarceration_incident_outcome_treatment
    WARNING = state_enum_strings.state_incarceration_incident_outcome_warning
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "The classification of the outcome following an incident in a facility."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_INCARCERATION_INCIDENT_OUTCOME_TYPE_VALUE_DESCRIPTIONS


_STATE_INCARCERATION_INCIDENT_OUTCOME_TYPE_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateIncarcerationIncidentOutcomeType.CELL_CONFINEMENT: "Describes an incident "
    "outcome in which a person is confined to their cell.",
    StateIncarcerationIncidentOutcomeType.DISCIPLINARY_LABOR: "Describes an incident "
    "outcome in which a person must perform labor.",
    StateIncarcerationIncidentOutcomeType.DISMISSED: "Used when the incident is "
    "dismissed and there is no other outcome.",
    StateIncarcerationIncidentOutcomeType.EXTERNAL_PROSECUTION: "Used when the "
    "incident is prosecuted as a new crime.",
    StateIncarcerationIncidentOutcomeType.FINANCIAL_PENALTY: "Describes an incident "
    "outcome in which a person is charged a fine.",
    StateIncarcerationIncidentOutcomeType.GOOD_TIME_LOSS: "Describes an incident "
    "outcome in which a person loses good time that they have earned against "
    "their sentence.",
    StateIncarcerationIncidentOutcomeType.NOT_GUILTY: "Used when the person is found "
    "to be not guilty of the reported incident.",
    StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS: "Describes an incident "
    "outcome in which a person loses some kind of privilege.",
    StateIncarcerationIncidentOutcomeType.RESTRICTED_CONFINEMENT: "Describes an "
    "incident outcome in which a person is confined to a restricted area. ",
    StateIncarcerationIncidentOutcomeType.SOLITARY: "Describes an incident outcome in "
    "which a person is placed in solitary confinement.",
    StateIncarcerationIncidentOutcomeType.TREATMENT: "Describes an incident outcome "
    "in which a person is placed in treatment.",
    StateIncarcerationIncidentOutcomeType.WARNING: "Describes an incident outcome in "
    "which a person is given a warning.",
}


@unique
class StateIncarcerationIncidentSeverity(StateEntityEnum):
    """Possible State Incarceration Incident Severity Levels."""

    HIGHEST = state_enum_strings.state_incarceration_incident_severity_highest
    SECOND_HIGHEST = (
        state_enum_strings.state_incarceration_incident_severity_second_highest
    )
    THIRD_HIGHEST = (
        state_enum_strings.state_incarceration_incident_severity_third_highest
    )
    FOURTH_HIGHEST = (
        state_enum_strings.state_incarceration_incident_severity_fourth_highest
    )
    FIFTH_HIGHEST = (
        state_enum_strings.state_incarceration_incident_severity_fifth_highest
    )
    SIXTH_HIGHEST = (
        state_enum_strings.state_incarceration_incident_severity_sixth_highest
    )
    SEVENTH_HIGHEST = (
        state_enum_strings.state_incarceration_incident_severity_seventh_highest
    )
    INTERNAL_UNKNOWN = state_enum_strings.internal_unknown
    EXTERNAL_UNKNOWN = state_enum_strings.external_unknown

    @classmethod
    def get_enum_description(cls) -> str:
        return "An enum indicating the severity of an incident."

    @classmethod
    def get_value_descriptions(cls) -> Dict["StateEntityEnum", str]:
        return _STATE_INCARCERATION_INCIDENT_SEVERITY_VALUE_DESCRIPTIONS


_STATE_INCARCERATION_INCIDENT_SEVERITY_VALUE_DESCRIPTIONS: Dict[
    StateEntityEnum, str
] = {
    StateIncarcerationIncidentSeverity.HIGHEST: "Highest incident severity level."
    "Incidents with the highest severity level should always be mapped to this value,"
    "regardless of the state's naming / numbering scheme for incident severity.",
    StateIncarcerationIncidentSeverity.SECOND_HIGHEST: "Second highest incident severity level."
    "Incidents with the second highest severity level should always be mapped to this value,"
    "regardless of the state's naming / numbering scheme for incident severity.",
    StateIncarcerationIncidentSeverity.THIRD_HIGHEST: "Third highest incident severity level."
    "Incidents with the third highest severity level should always be mapped to this value,"
    "regardless of the state's naming / numbering scheme for incident severity.",
    StateIncarcerationIncidentSeverity.FOURTH_HIGHEST: "Fourth highest incident severity level."
    "Incidents with the fourth highest severity level should always be mapped to this value,"
    "regardless of the state's naming / numbering scheme for incident severity.",
    StateIncarcerationIncidentSeverity.FIFTH_HIGHEST: "Fifth highest incident severity level."
    "Incidents with the fifth highest severity level should always be mapped to this value,"
    "regardless of the state's naming / numbering scheme for incident severity.",
    StateIncarcerationIncidentSeverity.SIXTH_HIGHEST: "Sixth highest incident severity level."
    "Incidents with the sixth highest severity level should always be mapped to this value,"
    "regardless of the state's naming / numbering scheme for incident severity.",
    StateIncarcerationIncidentSeverity.SEVENTH_HIGHEST: "Seventh highest incident severity level."
    "Incidents with the seventh highest severity level should always be mapped to this value,"
    "regardless of the state's naming / numbering scheme for incident severity.",
}
