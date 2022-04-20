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


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
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

    @staticmethod
    def _get_default_map() -> Dict[str, "StateIncarcerationIncidentType"]:
        return _STATE_INCARCERATION_INCIDENT_OFFENSE_MAP


# TODO(#8905): Delete _get_default_map() once all state ingest views have been
#  migrated to v2 mappings.
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
    MISCELLANEOUS = (
        state_enum_strings.state_incarceration_incident_outcome_miscellaneous
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

    @staticmethod
    def _get_default_map() -> Dict[str, "StateIncarcerationIncidentOutcomeType"]:
        return _STATE_INCARCERATION_INCIDENT_OUTCOME_MAP


_STATE_INCARCERATION_INCIDENT_OFFENSE_MAP = {
    "CONTRABAND": StateIncarcerationIncidentType.CONTRABAND,
    "DRUGS": StateIncarcerationIncidentType.CONTRABAND,
    "ALCOHOL": StateIncarcerationIncidentType.CONTRABAND,
    "TATTOO": StateIncarcerationIncidentType.CONTRABAND,
    "DISORDERLY CONDUCT": StateIncarcerationIncidentType.DISORDERLY_CONDUCT,
    "ESCAPE": StateIncarcerationIncidentType.ESCAPE,
    "MINOR": StateIncarcerationIncidentType.MINOR_OFFENSE,
    "MINOR OFFENSE": StateIncarcerationIncidentType.MINOR_OFFENSE,
    "PRESENT WITHOUT INFO": StateIncarcerationIncidentType.PRESENT_WITHOUT_INFO,
    "POSITIVE": StateIncarcerationIncidentType.POSITIVE,
    "REPORT": StateIncarcerationIncidentType.REPORT,
    "VIOLENT": StateIncarcerationIncidentType.VIOLENCE,
    "VIOLENCE": StateIncarcerationIncidentType.VIOLENCE,
    "INTERNAL UNKNOWN": StateIncarcerationIncidentType.INTERNAL_UNKNOWN,
    "EXTERNAL UNKNOWN": StateIncarcerationIncidentType.EXTERNAL_UNKNOWN,
}


_STATE_INCARCERATION_INCIDENT_OUTCOME_MAP = {
    "CELL CONFINEMENT": StateIncarcerationIncidentOutcomeType.CELL_CONFINEMENT,
    "DISCIPLINARY LABOR": StateIncarcerationIncidentOutcomeType.DISCIPLINARY_LABOR,
    "DISMISSED": StateIncarcerationIncidentOutcomeType.DISMISSED,
    "EXTERNAL PROSECUTION": StateIncarcerationIncidentOutcomeType.EXTERNAL_PROSECUTION,
    "FINANCIAL PENALTY": StateIncarcerationIncidentOutcomeType.FINANCIAL_PENALTY,
    "GOOD TIME LOSS": StateIncarcerationIncidentOutcomeType.GOOD_TIME_LOSS,
    "MISCELLANEOUS": StateIncarcerationIncidentOutcomeType.MISCELLANEOUS,
    "PRIVILEGE LOSS": StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS,
    "LOSS OF PRIVILEGE": StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS,
    "NOT GUILTY": StateIncarcerationIncidentOutcomeType.NOT_GUILTY,
    "RESTRICTED CONFINEMENT": StateIncarcerationIncidentOutcomeType.RESTRICTED_CONFINEMENT,
    "SOLITARY": StateIncarcerationIncidentOutcomeType.SOLITARY,
    "SOLITARY CONFINEMENT": StateIncarcerationIncidentOutcomeType.SOLITARY,
    "TREATMENT": StateIncarcerationIncidentOutcomeType.TREATMENT,
    "WARNING": StateIncarcerationIncidentOutcomeType.WARNING,
    "INTERNAL UNKNOWN": StateIncarcerationIncidentOutcomeType.INTERNAL_UNKNOWN,
    "EXTERNAL UNKNOWN": StateIncarcerationIncidentOutcomeType.EXTERNAL_UNKNOWN,
}
