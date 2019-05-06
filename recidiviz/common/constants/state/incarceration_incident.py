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

"""Constants related to the IncarcerationIncident entity."""

import recidiviz.common.constants.state.enum_canonical_strings as \
    state_enum_strings
from recidiviz.common.constants.entity_enum import EntityEnum, EntityEnumMeta


class IncarcerationIncidentOffense(EntityEnum, metaclass=EntityEnumMeta):
    CONTRABAND = state_enum_strings.incarceration_incident_offense_contraband
    VIOLENT = state_enum_strings.incarceration_incident_offense_violent

    @staticmethod
    def _get_default_map():
        return _INCARCERATION_INCIDENT_OFFENSE_MAP


class IncarcerationIncidentOutcome(EntityEnum, metaclass=EntityEnumMeta):
    PRIVILEGE_LOSS = \
        state_enum_strings.incarceration_incident_outcome_privilege_loss
    SOLITARY = state_enum_strings.incarceration_incident_outcome_solitary
    WARNING = state_enum_strings.incarceration_incident_outcome_warning
    WRITE_UP = state_enum_strings.incarceration_incident_outcome_write_up

    @staticmethod
    def _get_default_map():
        return _INCARCERATION_INCIDENT_OUTCOME_MAP


_INCARCERATION_INCIDENT_OFFENSE_MAP = {
    'CONTRABAND': IncarcerationIncidentOffense.CONTRABAND,
    'VIOLENT': IncarcerationIncidentOffense.VIOLENT,
    'VIOLENCE': IncarcerationIncidentOffense.VIOLENT,
}


_INCARCERATION_INCIDENT_OUTCOME_MAP = {
    'PRIVILEGE LOSS': IncarcerationIncidentOutcome.PRIVILEGE_LOSS,
    'LOSS OF PRIVILEGE': IncarcerationIncidentOutcome.PRIVILEGE_LOSS,
    'SOLITARY': IncarcerationIncidentOutcome.SOLITARY,
    'SOLITARY CONFINEMENT': IncarcerationIncidentOutcome.SOLITARY,
    'WARNING': IncarcerationIncidentOutcome.WARNING,
    'WRITE_UP': IncarcerationIncidentOutcome.WRITE_UP,
    'SHOT': IncarcerationIncidentOutcome.WRITE_UP,
    'REPRIMAND': IncarcerationIncidentOutcome.WRITE_UP,
}
