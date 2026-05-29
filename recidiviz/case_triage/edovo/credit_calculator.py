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
"""Earned-time credit calculation for Edovo course completions.

Per the API spec: pool content-hours across completions and emit 1 credit
per 6 accumulated hours.  The remainder carries over to future completions.
content_hours is Edovo's expected completion time for the course, not the
learner's actual time on task.
"""

from decimal import Decimal

from sqlalchemy import func

from recidiviz.persistence.database.schema.case_triage.schema import (
    EdevoCourseCompletion,
)
from recidiviz.persistence.database.session import Session

HOURS_PER_CREDIT = Decimal("6")


def new_credits_for_completion(
    prior_total_hours: Decimal,
    new_hours: Decimal,
) -> int:
    """Return the number of new credits triggered by adding |new_hours| to |prior_total_hours|.

    Credits are emitted at the rate of one per HOURS_PER_CREDIT accumulated
    hours; any remainder carries forward.
    """
    if new_hours <= 0:
        raise ValueError(f"new_hours must be positive, got {new_hours!r}")
    credits_before = int(prior_total_hours // HOURS_PER_CREDIT)
    credits_after = int((prior_total_hours + new_hours) // HOURS_PER_CREDIT)
    return credits_after - credits_before


def query_total_hours(
    session: Session,
    person_id: str,
    state_code: str,
) -> Decimal:
    """Return the total pooled content_hours for |person_id| in |state_code|.

    Callers should invoke this after flushing any pending completion so that
    the new record is included in the sum.
    """
    result = (
        session.query(func.sum(EdevoCourseCompletion.content_hours))
        .filter(
            EdevoCourseCompletion.person_id == person_id,
            EdevoCourseCompletion.state_code == state_code,
        )
        .scalar()
    )
    return Decimal(str(result)) if result is not None else Decimal("0")
