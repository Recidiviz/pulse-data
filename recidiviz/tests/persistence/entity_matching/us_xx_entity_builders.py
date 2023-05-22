# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Helper functions for generating entities with state_code=US_XX."""
from typing import Any

from recidiviz.common.constants.state.state_charge import StateChargeStatus
from recidiviz.common.constants.state.state_sentence import StateSentenceStatus
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.state.entities import (
    StateAgent,
    StateCharge,
    StateIncarcerationIncident,
    StateIncarcerationIncidentOutcome,
    StateIncarcerationSentence,
    StatePerson,
    StatePersonExternalId,
    StatePersonRace,
    StateStaff,
    StateStaffExternalId,
    StateStaffRolePeriod,
)

_STATE_CODE = StateCode.US_XX.value


def make_person(**kwargs: Any) -> StatePerson:
    return StatePerson.new_with_defaults(state_code=_STATE_CODE, **kwargs)


def make_staff(**kwargs: Any) -> StateStaff:
    return StateStaff.new_with_defaults(state_code=_STATE_CODE, **kwargs)


def make_person_external_id(**kwargs: Any) -> StatePersonExternalId:
    return StatePersonExternalId.new_with_defaults(state_code=_STATE_CODE, **kwargs)


def make_staff_external_id(**kwargs: Any) -> StateStaffExternalId:
    return StateStaffExternalId.new_with_defaults(state_code=_STATE_CODE, **kwargs)


def make_person_race(**kwargs: Any) -> StatePersonRace:
    return StatePersonRace.new_with_defaults(state_code=_STATE_CODE, **kwargs)


def make_incarceration_incident(**kwargs: Any) -> StateIncarcerationIncident:
    return StateIncarcerationIncident.new_with_defaults(
        state_code=_STATE_CODE, **kwargs
    )


def make_staff_role_period(**kwargs: Any) -> StateStaffRolePeriod:
    return StateStaffRolePeriod.new_with_defaults(state_code=_STATE_CODE, **kwargs)


def make_incarceration_sentence(**kwargs: Any) -> StateIncarcerationSentence:
    return StateIncarcerationSentence.new_with_defaults(
        state_code=_STATE_CODE,
        status=StateSentenceStatus.PRESENT_WITHOUT_INFO,
        **kwargs,
    )


def make_state_charge(**kwargs: Any) -> StateCharge:
    return StateCharge.new_with_defaults(
        state_code=_STATE_CODE, status=StateChargeStatus.PRESENT_WITHOUT_INFO, **kwargs
    )


def make_agent(**kwargs: Any) -> StateAgent:
    return StateAgent.new_with_defaults(state_code=_STATE_CODE, **kwargs)


def make_incarceration_incident_outcome(
    **kwargs: Any,
) -> StateIncarcerationIncidentOutcome:
    return StateIncarcerationIncidentOutcome.new_with_defaults(
        state_code=_STATE_CODE, **kwargs
    )
