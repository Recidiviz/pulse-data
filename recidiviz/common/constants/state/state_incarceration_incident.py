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

import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class StateIncarcerationIncidentType(EntityEnum, metaclass=EntityEnumMeta):
    CONTRABAND = \
        state_enum_strings.state_incarceration_incident_type_contraband
    DISORDERLY_CONDUCT = \
        state_enum_strings.state_incarceration_incident_type_disorderly_conduct
    ESCAPE = state_enum_strings.state_incarceration_incident_type_escape
    MINOR_OFFENSE = \
        state_enum_strings.state_incarceration_incident_type_minor_offense
    VIOLENCE = \
        state_enum_strings.state_incarceration_incident_type_violence

    @staticmethod
    def _get_default_map():
        return _STATE_INCARCERATION_INCIDENT_OFFENSE_MAP


class StateIncarcerationIncidentOutcomeType(EntityEnum,
                                            metaclass=EntityEnumMeta):
    DISCIPLINARY_LABOR = \
        state_enum_strings.\
        state_incarceration_incident_outcome_disciplinary_labor
    EXTERNAL_PROSECUTION = \
        state_enum_strings.\
        state_incarceration_incident_outcome_external_prosecution
    FINANCIAL_PENALTY = \
        state_enum_strings.\
        state_incarceration_incident_outcome_financial_penalty
    GOOD_TIME_LOSS = \
        state_enum_strings.\
        state_incarceration_incident_outcome_good_time_loss
    MISCELLANEOUS = \
        state_enum_strings.state_incarceration_incident_outcome_miscellaneous
    NOT_GUILTY = \
        state_enum_strings.state_incarceration_incident_outcome_not_guilty
    PRIVILEGE_LOSS = \
        state_enum_strings.state_incarceration_incident_outcome_privilege_loss
    SOLITARY = \
        state_enum_strings.state_incarceration_incident_outcome_solitary
    TREATMENT = \
        state_enum_strings.state_incarceration_incident_outcome_treatment
    WARNING = state_enum_strings.state_incarceration_incident_outcome_warning

    @staticmethod
    def _get_default_map():
        return _STATE_INCARCERATION_INCIDENT_OUTCOME_MAP


_STATE_INCARCERATION_INCIDENT_OFFENSE_MAP = {
    'CONTRABAND': StateIncarcerationIncidentType.CONTRABAND,
    'DRUGS': StateIncarcerationIncidentType.CONTRABAND,
    'ALCOHOL': StateIncarcerationIncidentType.CONTRABAND,
    'TATTOO': StateIncarcerationIncidentType.CONTRABAND,
    'DISORDERLY CONDUCT': StateIncarcerationIncidentType.DISORDERLY_CONDUCT,
    'ESCAPE': StateIncarcerationIncidentType.ESCAPE,
    'MINOR': StateIncarcerationIncidentType.MINOR_OFFENSE,
    'MINOR OFFENSE': StateIncarcerationIncidentType.MINOR_OFFENSE,
    'VIOLENT': StateIncarcerationIncidentType.VIOLENCE,
    'VIOLENCE': StateIncarcerationIncidentType.VIOLENCE,
}


_STATE_INCARCERATION_INCIDENT_OUTCOME_MAP = {
    'DISCIPLINARY LABOR':
        StateIncarcerationIncidentOutcomeType.DISCIPLINARY_LABOR,
    'EXTERNAL PROSECUTION':
        StateIncarcerationIncidentOutcomeType.EXTERNAL_PROSECUTION,
    'FINANCIAL PENALTY':
        StateIncarcerationIncidentOutcomeType.FINANCIAL_PENALTY,
    'GOOD TIME LOSS': StateIncarcerationIncidentOutcomeType.GOOD_TIME_LOSS,
    'MISCELLANEOUS': StateIncarcerationIncidentOutcomeType.MISCELLANEOUS,
    'PRIVILEGE LOSS': StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS,
    'LOSS OF PRIVILEGE': StateIncarcerationIncidentOutcomeType.PRIVILEGE_LOSS,
    'NOT GUILTY': StateIncarcerationIncidentOutcomeType.NOT_GUILTY,
    'SOLITARY': StateIncarcerationIncidentOutcomeType.SOLITARY,
    'SOLITARY CONFINEMENT': StateIncarcerationIncidentOutcomeType.SOLITARY,
    'TREATMENT': StateIncarcerationIncidentOutcomeType.TREATMENT,
    'WARNING': StateIncarcerationIncidentOutcomeType.WARNING,
}
