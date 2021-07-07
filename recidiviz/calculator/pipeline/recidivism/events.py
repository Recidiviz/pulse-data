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
import logging
from datetime import date
from typing import Optional

import attr
from dateutil.relativedelta import relativedelta

from recidiviz.calculator.pipeline.utils.event_utils import IdentifierEvent


@attr.s
class ReleaseEvent(IdentifierEvent):
    """Models details related to a release from incarceration.

    This includes the information pertaining to a release from incarceration
    that we will want to track when calculating recidivism metrics."""

    # A Date for when the person first was admitted for this period of
    # incarceration.
    original_admission_date: date = attr.ib()

    # A Date for when the person was last released from this period of
    # incarceration.
    release_date: date = attr.ib()

    # The facility that the person was last released from for this period of
    # incarceration.
    release_facility: Optional[str] = attr.ib(default=None)

    # County of residence
    county_of_residence: Optional[str] = attr.ib(default=None)

    # The event_date for this IdentifierEvent is always the release_date
    event_date: date = attr.ib(init=False, default=release_date)

    @property
    def release_cohort(self) -> int:
        return self.release_date.year

    @property
    def stay_length(self) -> Optional[int]:
        """Calculates the length of facility stay of a given event in months.

        This is rounded down to the nearest month, so a stay from 2015-01-15 to
        2017-01-14 results in a stay length of 23 months. Note that bucketing in
        stay_length_bucketing is upper bound exclusive, so in this example the
        bucket would be 12-24, and if the stay ended on 2017-01-15, the stay length
        would be 24 months and the bucket would be 24-36.

        Returns:
            The length of the facility stay in months. None if the original
            admission date or release date is not known.
        """
        if self.original_admission_date is None or self.release_date is None:
            return None

        delta = relativedelta(self.release_date, self.original_admission_date)
        return delta.years * 12 + delta.months

    @property
    def stay_length_bucket(self) -> Optional[str]:
        """Calculates the stay length bucket that applies to measurement.

        Stay length buckets (upper bound exclusive) for measurement:
            <12, 12-24, 24-36, 36-48, 48-60, 60-72,
            72-84, 84-96, 96-108, 108-120, 120+.

        Returns:
            A string representation of the age bucket for the person.
        """

        stay_length = self.stay_length
        if stay_length is None:
            return None
        if stay_length < 12:
            return "<12"
        if stay_length < 24:
            return "12-24"
        if stay_length < 36:
            return "24-36"
        if stay_length < 48:
            return "36-48"
        if stay_length < 60:
            return "48-60"
        if stay_length < 72:
            return "60-72"
        if stay_length < 84:
            return "72-84"
        if stay_length < 96:
            return "84-96"
        if stay_length < 108:
            return "96-108"
        if stay_length < 120:
            return "108-120"
        return "120<"


@attr.s
class RecidivismReleaseEvent(ReleaseEvent):
    """Models a ReleaseEvent where the person was later reincarcerated."""

    # A Date for when the person was re-incarcerated.
    reincarceration_date: date = attr.ib(default=None)

    # The facility that the person entered into upon first return to
    # incarceration after the release.
    reincarceration_facility: Optional[str] = attr.ib(default=None)

    @property
    def days_at_liberty(self) -> int:
        """Returns the number of days between a release and a reincarceration."""
        release_date = self.release_date

        return_date = self.reincarceration_date

        delta = return_date - release_date

        if delta.days < 0:
            logging.warning(
                "Release date on RecidivismReleaseEvent is before admission date: %s."
                "The identifier step is not properly classifying releases.",
                self,
            )

        return delta.days


@attr.s
class NonRecidivismReleaseEvent(ReleaseEvent):
    """Models a ReleaseEvent where the person was not later reincarcerated."""

    @property
    def days_at_liberty(self) -> None:
        return None
