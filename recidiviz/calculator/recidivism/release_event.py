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

"""Releases that either lead to recidivism or non-recidivism for calculation."""

from datetime import date
from enum import Enum, auto
from typing import Optional

import attr

from recidiviz.common.attr_mixins import BuildableAttr


class ReincarcerationReturnType(Enum):
    # The person returned to incarceration on a new admission after being free.
    NEW_ADMISSION = auto()

    # The person returned to incarceration because their supervision was
    # revoked. Note this covers all reasons for revocation, including new
    # crimes that may have factored into the revocation decision.
    REVOCATION = auto()


class ReincarcerationReturnFromSupervisionType(Enum):
    # The person returned from being on parole
    PAROLE = auto()

    # The person returned from being on probation
    PROBATION = auto()


@attr.s
class ReleaseEvent(BuildableAttr):
    """Models details related to a release from incarceration.

    This includes the information pertaining to a release from incarceration
    that we will want to track when calculating recidivism metrics."""

    # A Date for when the person first was admitted for this period of
    # incarceration.
    original_admission_date: date = attr.ib(default=None)

    # A Date for when the person was last released from this period of
    # incarceration.
    release_date: date = attr.ib(default=None)

    # The facility that the person was last released from for this period of
    # incarceration.
    release_facility: Optional[str] = attr.ib(default=None)


@attr.s
class RecidivismReleaseEvent(ReleaseEvent):
    """Models a ReleaseEvent where the person was later reincarcerated."""

    # A Date for when the person was re-incarcerated.
    reincarceration_date: date = attr.ib(default=None)

    # The facility that the person entered into upon first return to
    # incarceration after the release.
    reincarceration_facility: Optional[str] = attr.ib(default=None)

    # ReincarcerationReturnType enum for the type of return to
    # incarceration this recidivism event describes.
    return_type: ReincarcerationReturnType = attr.ib(default=None)

    # ReincarcerationReturnFromSupervisionType enum for the type of
    # supervision the person was on before they returned to incarceration.
    from_supervision_type: \
        Optional[ReincarcerationReturnFromSupervisionType] = \
        attr.ib(default=None)

@attr.s
class NonRecidivismReleaseEvent(ReleaseEvent):
    """Models a ReleaseEvent where the person was not later reincarcerated."""
