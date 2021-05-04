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

from recidiviz.case_triage.demo_helpers import (
    fake_officer_id_for_demo_user,
)
from recidiviz.case_triage.opportunities.types import (
    OpportunityDeferralType,
    OpportunityType,
)
from recidiviz.case_triage.querier.querier import (
    DemoCaseTriageQuerier,
    CaseTriageQuerier,
)
from recidiviz.persistence.database.schema.case_triage.schema import (
    ETLClient,
    ETLOfficer,
    ETLOpportunity,
    OpportunityDeferral,
)


class OpportunityDeferralDoesNotExistError(ValueError):
    pass


def _defer_opportunity(
    session: Session,
    officer_id: str,
    etl_opportunity: ETLOpportunity,
    deferral_type: OpportunityDeferralType,
    defer_until: datetime,
    reminder_requested: bool,
) -> None:
    """This method updates the opportunity_deferrals table with
    the given parameters."""

    insert_statement = (
        insert(OpportunityDeferral)
        .values(
            person_external_id=etl_opportunity.person_external_id,
            supervising_officer_external_id=officer_id,
            state_code=etl_opportunity.state_code,
            opportunity_type=etl_opportunity.opportunity_type,
            deferral_type=deferral_type.value,
            deferred_until=defer_until,
            reminder_was_requested=reminder_requested,
            opportunity_metadata=etl_opportunity.opportunity_metadata,
        )
        .on_conflict_do_update(
            constraint="unique_person_officer_opportunity_triple",
            set_={
                "deferral_type": deferral_type.value,
                "deferred_until": defer_until,
                "reminder_was_requested": reminder_requested,
                "opportunity_metadata": etl_opportunity.opportunity_metadata,
            },
        )
    )
    session.execute(insert_statement)
    session.commit()


def _delete_opportunity_deferral(
    session: Session, officer_external_id: str, deferral_id: str
) -> OpportunityDeferral:
    try:
        opportunity_deferral = (
            session.query(OpportunityDeferral)
            .filter(
                OpportunityDeferral.supervising_officer_external_id
                == officer_external_id,
                OpportunityDeferral.deferral_id == deferral_id,
            )
            .one()
        )
    except sqlalchemy.orm.exc.NoResultFound as e:
        raise OpportunityDeferralDoesNotExistError(
            f"Could not find deferral for officer: {officer_external_id} deferral_id: {deferral_id}"
        ) from e

    session.delete(opportunity_deferral)
    session.commit()

    return opportunity_deferral


class OpportunitiesInterface:
    """Implements interface for querying and modifying opportunities."""

    @staticmethod
    def defer_opportunity(
        session: Session,
        officer: ETLOfficer,
        client: ETLClient,
        opportunity_type: OpportunityType,
        deferral_type: OpportunityDeferralType,
        defer_until: datetime,
        reminder_requested: bool,
    ) -> None:
        """Implements base opportunity deferral and commits back to database."""
        etl_opportunity = CaseTriageQuerier.fetch_etl_opportunity(
            session, officer, client, opportunity_type
        )
        _defer_opportunity(
            session,
            officer.external_id,
            etl_opportunity,
            deferral_type,
            defer_until,
            reminder_requested,
        )

    @staticmethod
    def delete_opportunity_deferral(
        session: Session, officer: ETLOfficer, deferral_id: str
    ) -> OpportunityDeferral:
        return _delete_opportunity_deferral(session, officer.external_id, deferral_id)


class DemoOpportunitiesInterface:
    """Implements interface for querying and modifying opportunities as
    a demo user."""

    @staticmethod
    def defer_opportunity(
        session: Session,
        user_email: str,
        client: ETLClient,
        opportunity: OpportunityType,
        deferral_type: OpportunityDeferralType,
        defer_until: datetime,
        reminder_requested: bool,
    ) -> None:
        """Implements base opportunity deferral and commits back to database."""
        etl_opportunity = DemoCaseTriageQuerier.fetch_etl_opportunity(
            client, opportunity
        )
        _defer_opportunity(
            session,
            fake_officer_id_for_demo_user(user_email),
            etl_opportunity,
            deferral_type,
            defer_until,
            reminder_requested,
        )

    @staticmethod
    def delete_opportunity_deferral(
        session: Session, user_email: str, deferral_id: str
    ) -> OpportunityDeferral:
        return _delete_opportunity_deferral(
            session, fake_officer_id_for_demo_user(user_email), deferral_id
        )
