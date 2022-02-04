# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Events modeling violations and their responses."""
import datetime

import attr

from recidiviz.calculator.pipeline.utils.event_utils import (
    IdentifierEvent,
    ViolationResponseMixin,
)


@attr.s(frozen=True)
class ViolationEvent(IdentifierEvent):
    """Models details of an event related to a violation while on supervision."""

    # Violation Id
    supervision_violation_id: int = attr.ib()


@attr.s(frozen=True)
class ViolationWithResponseEvent(ViolationEvent, ViolationResponseMixin):
    """Models violations that have responses.

    Describes a date in which a person incurred the first action related to a violation.
    This will be the first response date for a given violation.
    """

    # Earliest response date associated with the supervision_violation_id
    @property
    def response_date(self) -> datetime.date:
        return self.event_date
