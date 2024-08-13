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
"""Models for spans of time when a person was counted towards a particular population."""
from typing import Optional

import attr

from recidiviz.common.attr_validators import is_opt
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration import StateIncarcerationType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateIncarcerationPeriodCustodyLevel,
    StateIncarcerationPeriodHousingUnitCategory,
    StateIncarcerationPeriodHousingUnitType,
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
)
from recidiviz.pipelines.utils.identifier_models import (
    IncludedInStateMixin,
    Span,
    SupervisionLocationMixin,
)


@attr.s(frozen=True)
class IncarcerationPopulationSpan(Span, IncludedInStateMixin):
    """Models a span of time that a person spent incarcerated"""

    incarceration_type: Optional[StateIncarcerationType] = attr.ib(
        default=None, validator=is_opt(StateIncarcerationType)
    )

    facility: Optional[str] = attr.ib(default=None)

    purpose_for_incarceration: Optional[
        StateSpecializedPurposeForIncarceration
    ] = attr.ib(default=None)

    custodial_authority: Optional[StateCustodialAuthority] = attr.ib(default=None)

    # The level of staff supervision and security employed for a person held in custody
    custody_level: Optional[StateIncarcerationPeriodCustodyLevel] = attr.ib(
        default=None
    )

    # The raw text value of the incarceration period custody level
    custody_level_raw_text: Optional[str] = attr.ib(default=None)

    # The housing unit within the facility in which the person currently resides
    housing_unit: Optional[str] = attr.ib(default=None)

    # The type of housing unit within the facility in which the person currently resides
    housing_unit_category: Optional[
        StateIncarcerationPeriodHousingUnitCategory
    ] = attr.ib(default=None)

    # The raw text value of the incarceration period housing unit type
    housing_unit_category_raw_text: Optional[str] = attr.ib(default=None)

    # Where the person is currently being housed regardless of technical assignment/custody level
    housing_unit_type: Optional[StateIncarcerationPeriodHousingUnitType] = attr.ib(
        default=None
    )

    # The raw text value of the incarceration period housing unit type
    housing_unit_type_raw_text: Optional[str] = attr.ib(default=None)


@attr.s(frozen=True)
class SupervisionPopulationSpan(Span, SupervisionLocationMixin, IncludedInStateMixin):
    """Models a span of time that a person is on supervision."""

    supervision_type: Optional[StateSupervisionPeriodSupervisionType] = attr.ib(
        default=None
    )

    supervision_level: Optional[StateSupervisionLevel] = attr.ib(default=None)

    supervision_level_raw_text: Optional[str] = attr.ib(default=None)

    case_type: Optional[StateSupervisionCaseType] = attr.ib(default=None)

    custodial_authority: Optional[StateCustodialAuthority] = attr.ib(default=None)

    # StateStaff id of officer who was supervising the person described by this metric
    supervising_officer_staff_id: Optional[int] = attr.ib(default=None)
