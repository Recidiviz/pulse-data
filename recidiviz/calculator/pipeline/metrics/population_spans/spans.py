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

from recidiviz.calculator.pipeline.utils.identifier_models import (
    IncludedInStateMixin,
    Span,
    SupervisionLocationMixin,
)
from recidiviz.common.constants.state.state_case_type import StateSupervisionCaseType
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority
from recidiviz.common.constants.state.state_supervision_period import (
    StateSupervisionLevel,
    StateSupervisionPeriodSupervisionType,
)


@attr.s(frozen=True)
class IncarcerationPopulationSpan(Span, IncludedInStateMixin):
    """Models a span of time that a person spent incarcerated"""

    facility: Optional[str] = attr.ib(default=None)

    purpose_for_incarceration: Optional[
        StateSpecializedPurposeForIncarceration
    ] = attr.ib(default=None)

    custodial_authority: Optional[StateCustodialAuthority] = attr.ib(default=None)

    # Area of jurisdictional coverage of the court that sentenced the person to this
    # incarceration
    judicial_district_code: Optional[str] = attr.ib(default=None)


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

    # External ID of the officer who is supervising the person during this span of time
    supervising_officer_external_id: Optional[str] = attr.ib(default=None)

    # Area of jurisdictional coverage of the court that sentenced the person to this
    # period of supervision
    judicial_district_code: Optional[str] = attr.ib(default=None)
