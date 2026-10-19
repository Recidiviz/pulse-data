# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""The result of computing a single sentence's term: its length in days, the date
it ends, and the date it reaches its (not yet overlap-adjusted) parole
eligibility."""
from datetime import date

import attr

from recidiviz.common import attr_validators
from recidiviz.sentence_calculator.models.tracked import TrackedValue


@attr.define(frozen=True, kw_only=True)
class SentenceTermCalculation:
    """The result of computing a single sentence's term: its length in days, the
    date it ends, and the date it reaches its (not yet overlap-adjusted) parole
    eligibility."""

    total_days: TrackedValue[int] = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    """Number of days in the sentence's term, as configured by the rule set."""

    end_date: date = attr.ib(validator=attr_validators.is_date)
    """Date the sentence's term is complete."""

    sentence_ped: TrackedValue[date] = attr.ib(
        validator=attr.validators.instance_of(TrackedValue)
    )
    """Date the sentence becomes eligible to go before the parole board: its start
    date plus its min term."""
