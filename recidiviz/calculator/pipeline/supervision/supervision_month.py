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
"""Months on supervision that may have included a revocation."""
from typing import Optional

import attr

from recidiviz.common.attr_mixins import BuildableAttr
from recidiviz.common.constants.state.state_supervision import \
    StateSupervisionType
from recidiviz.common.constants.state.state_supervision_violation import \
    StateSupervisionViolationType
from recidiviz.common.constants.state.state_supervision_violation_response \
    import StateSupervisionViolationResponseRevocationType


# TODO(2642): Add supervision year metrics
@attr.s(frozen=True)
class SupervisionMonth(BuildableAttr):
    """Models details related to a month on supervision.

    This includes the information pertaining to time on supervision
    that we will want to track when calculating supervision and
    revocation metrics."""

    # The state where the supervision took place
    state_code: str = attr.ib()

    # Year for when the person was on supervision
    year: int = attr.ib()

    # Month for when the person was on supervision
    month: int = attr.ib()

    # The type of supervision the person was on
    supervision_type: Optional[StateSupervisionType] = attr.ib(default=None)


@attr.s(frozen=True)
class RevocationReturnSupervisionMonth(SupervisionMonth):
    """Models a SupervisionMonth where the person was incarcerated for a
    revocation."""

    # The type of revocation of supervision
    revocation_type: Optional[StateSupervisionViolationResponseRevocationType] \
        = attr.ib(default=None)

    # StateSupervisionViolationType enum for the type of violation that
    # eventually caused the revocation of supervision
    source_violation_type: Optional[StateSupervisionViolationType] = \
        attr.ib(default=None)


@attr.s(frozen=True)
class NonRevocationReturnSupervisionMonth(SupervisionMonth):
    """Models a SupervisionMonth where the person was not incarcerated for
    a revocation."""
