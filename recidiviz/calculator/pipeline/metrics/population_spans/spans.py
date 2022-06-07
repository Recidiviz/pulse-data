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
)
from recidiviz.common.constants.state.state_incarceration_period import (
    StateSpecializedPurposeForIncarceration,
)
from recidiviz.common.constants.state.state_shared_enums import StateCustodialAuthority


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
