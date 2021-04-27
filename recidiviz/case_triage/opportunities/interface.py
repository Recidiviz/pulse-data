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
"""Implements common interface used to support opportunity deferrals."""
from datetime import datetime

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from recidiviz.case_triage.demo_helpers import (
    fake_officer_id_for_demo_user,
)
from recidiviz.case_triage.opportunities.types import OpportunityType
from recidiviz.persistence.database.schema.case_triage.schema import (
    ETLClient,
    ETLOfficer,
    OpportunityDeferral,
)


def _defer_opportunity(
    session: Session,
    officer_id: str,
    client: ETLClient,
    opportunity: OpportunityType,
    defer_until: datetime,
    reminder_requested: bool,
) -> None:
    """This method updates the opportunity_deferrals table with
    the given parameters."""
    insert_statement = (
        insert(OpportunityDeferral)
        .values(
            person_external_id=client.person_external_id,
            supervising_officer_external_id=officer_id,
            state_code=client.state_code,
            opportunity_type=opportunity.value,
            deferred_until=defer_until,
            reminder_was_requested=reminder_requested,
            opportunity_metadata={},
        )
        .on_conflict_do_update(
            constraint="unique_person_officer_opportunity_triple",
            set_={
                "deferred_until": defer_until,
                "reminder_was_requested": reminder_requested,
                "opportunity_metadata": {},
            },
        )
    )
    session.execute(insert_statement)
    session.commit()


class OpportunitiesInterface:
    """Implements interface for querying and modifying opportunities."""

    @staticmethod
    def defer_opportunity(
        session: Session,
        officer: ETLOfficer,
        client: ETLClient,
        opportunity: OpportunityType,
        defer_until: datetime,
        reminder_requested: bool,
    ) -> None:
        """Implements base opportunity deferral and commits back to database."""
        _defer_opportunity(
            session,
            officer.external_id,
            client,
            opportunity,
            defer_until,
            reminder_requested,
        )


class DemoOpportunitiesInterface:
    """Implements interface for querying and modifying opportunities as
    a demo user."""

    @staticmethod
    def defer_opportunity(
        session: Session,
        user_email: str,
        client: ETLClient,
        opportunity: OpportunityType,
        defer_until: datetime,
        reminder_requested: bool,
    ) -> None:
        """Implements base opportunity deferral and commits back to database."""
        _defer_opportunity(
            session,
            fake_officer_id_for_demo_user(user_email),
            client,
            opportunity,
            defer_until,
            reminder_requested,
        )
