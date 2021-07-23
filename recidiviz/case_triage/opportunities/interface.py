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

import sqlalchemy.orm.exc
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from recidiviz.case_triage.opportunities.types import (
    OpportunityDeferralType,
    OpportunityType,
)
from recidiviz.case_triage.querier.querier import CaseTriageQuerier
from recidiviz.case_triage.user_context import UserContext
from recidiviz.persistence.database.schema.case_triage.schema import (
    ETLClient,
    OpportunityDeferral,
)


class OpportunityDeferralDoesNotExistError(ValueError):
    pass


class OpportunitiesInterface:
    """Implements interface for querying and modifying opportunities."""

    @staticmethod
    def defer_opportunity(
        session: Session,
        user_context: UserContext,
        client: ETLClient,
        opportunity_type: OpportunityType,
        deferral_type: OpportunityDeferralType,
        defer_until: datetime,
        reminder_requested: bool,
    ) -> None:
        """Implements base opportunity deferral and commits back to database."""
        opportunity = CaseTriageQuerier.fetch_opportunity(
            session, user_context, client, opportunity_type
        )
        officer_id = user_context.officer_id
        person_external_id = user_context.person_id(client)
        insert_statement = (
            insert(OpportunityDeferral)
            .values(
                person_external_id=person_external_id,
                supervising_officer_external_id=officer_id,
                state_code=opportunity.state_code,
                opportunity_type=opportunity.opportunity_type,
                deferral_type=deferral_type.value,
                deferred_until=defer_until,
                reminder_was_requested=reminder_requested,
                opportunity_metadata=opportunity.opportunity_metadata,
            )
            .on_conflict_do_update(
                constraint="unique_person_officer_opportunity_triple",
                set_={
                    "deferral_type": deferral_type.value,
                    "deferred_until": defer_until,
                    "reminder_was_requested": reminder_requested,
                    "opportunity_metadata": opportunity.opportunity_metadata,
                },
            )
        )
        session.execute(insert_statement)
        session.commit()

    @staticmethod
    def delete_opportunity_deferral(
        session: Session, user_context: UserContext, deferral_id: str
    ) -> OpportunityDeferral:
        try:
            opportunity_deferral = (
                session.query(OpportunityDeferral)
                .filter(
                    OpportunityDeferral.supervising_officer_external_id
                    == user_context.officer_id,
                    OpportunityDeferral.deferral_id == deferral_id,
                )
                .one()
            )
        except sqlalchemy.orm.exc.NoResultFound as e:
            raise OpportunityDeferralDoesNotExistError(
                f"Could not find deferral for officer: {user_context.officer_id} deferral_id: {deferral_id}"
            ) from e

        session.delete(opportunity_deferral)
        session.commit()

        return opportunity_deferral
