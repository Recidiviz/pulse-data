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
"""Implements the Querier abstraction that is responsible for considering multiple
data sources and coalescing answers for downstream consumers."""
from collections import defaultdict
from typing import Dict, List, Optional

import sqlalchemy.orm.exc
from sqlalchemy.orm import Session, joinedload

from recidiviz.case_triage.demo_helpers import (
    fake_officer_id_for_demo_user,
    get_fixture_clients,
    get_fixture_opportunities,
)
from recidiviz.case_triage.querier.case_presenter import CasePresenter
from recidiviz.case_triage.querier.opportunity_presenter import OpportunityPresenter
from recidiviz.persistence.database.schema.case_triage.schema import (
    ETLClient,
    ETLOfficer,
    ETLOpportunity,
    CaseUpdate,
    OpportunityDeferral,
)


class PersonDoesNotExistError(ValueError):
    pass


class CaseTriageQuerier:
    """Implements Querier abstraction for Case Triage data sources."""

    @staticmethod
    def etl_client_for_officer(
        session: Session, officer: ETLOfficer, person_external_id: str
    ) -> ETLClient:
        try:
            return (
                session.query(ETLClient)
                .filter(
                    ETLClient.state_code == officer.state_code,
                    ETLClient.supervising_officer_external_id == officer.external_id,
                    ETLClient.person_external_id == person_external_id,
                )
                .one()
            )
        except sqlalchemy.orm.exc.NoResultFound as e:
            raise PersonDoesNotExistError(
                f"could not find id: {person_external_id}"
            ) from e

    @staticmethod
    def case_for_client_and_officer(
        session: Session, client: ETLClient, officer: ETLOfficer
    ) -> CasePresenter:
        try:
            case_updates = (
                session.query(CaseUpdate)
                .filter_by(
                    person_external_id=client.person_external_id,
                    officer_external_id=officer.external_id,
                    state_code=client.state_code,
                )
                .all()
            )
        except sqlalchemy.orm.exc.NoResultFound:
            case_updates = None
        return CasePresenter(client, case_updates)

    @staticmethod
    def clients_for_officer(
        session: Session, officer: ETLOfficer
    ) -> List[CasePresenter]:
        """Outputs the list of clients for a given officer in CasePresenter form."""
        clients = (
            session.query(ETLClient)
            .filter(
                ETLClient.etl_officer == officer,
                ETLClient.state_code == officer.state_code,
            )
            .options(joinedload(ETLClient.case_updates))
            .all()
        )

        return [CasePresenter(client, client.case_updates) for client in clients]

    @staticmethod
    def officer_for_email(session: Session, officer_email: str) -> ETLOfficer:
        email = officer_email.lower()
        return session.query(ETLOfficer).filter_by(email_address=email).one()

    @staticmethod
    def opportunities_for_officer(
        session: Session, officer: ETLOfficer
    ) -> List[OpportunityPresenter]:
        opportunity_info = (
            session.query(ETLOpportunity, OpportunityDeferral)
            .outerjoin(
                OpportunityDeferral,
                (
                    ETLOpportunity.person_external_id
                    == OpportunityDeferral.person_external_id
                )
                & (ETLOpportunity.state_code == OpportunityDeferral.state_code)
                & (
                    ETLOpportunity.supervising_officer_external_id
                    == OpportunityDeferral.supervising_officer_external_id
                )
                & (
                    ETLOpportunity.opportunity_type
                    == OpportunityDeferral.opportunity_type
                ),
            )
            .filter(
                ETLOpportunity.supervising_officer_external_id == officer.external_id,
                ETLOpportunity.state_code == officer.state_code,
            )
            .all()
        )
        return [OpportunityPresenter(info[0], info[1]) for info in opportunity_info]


class DemoCaseTriageQuerier:
    """Implements some querying abstractions for use by demo users."""

    @staticmethod
    def clients_for_demo_user(
        session: Session, user_email_address: str
    ) -> List[CasePresenter]:
        case_updates = (
            session.query(CaseUpdate)
            .filter(
                CaseUpdate.officer_external_id
                == fake_officer_id_for_demo_user(user_email_address)
            )
            .all()
        )
        client_ids_to_case_updates = defaultdict(list)
        for case_update in case_updates:
            client_ids_to_case_updates[case_update.person_external_id].append(
                case_update
            )

        clients = get_fixture_clients()
        return [
            CasePresenter(client, client_ids_to_case_updates[client.person_external_id])
            for client in clients
        ]

    @staticmethod
    def etl_client_with_id(person_external_id: str) -> ETLClient:
        clients = get_fixture_clients()

        for client in clients:
            if client.person_external_id == person_external_id:
                return client

        raise PersonDoesNotExistError(f"could not find id: {person_external_id}")

    @staticmethod
    def opportunities_for_demo_user(
        session: Session, user_email_address: str
    ) -> List[OpportunityPresenter]:
        opportunity_deferrals = (
            session.query(OpportunityDeferral)
            .filter(
                OpportunityDeferral.supervising_officer_external_id
                == fake_officer_id_for_demo_user(user_email_address)
            )
            .all()
        )

        # Map from person -> opportunity type -> optional deferral
        opportunity_to_deferral: Dict[
            str, Dict[str, Optional[OpportunityDeferral]]
        ] = defaultdict(dict)
        for deferral in opportunity_deferrals:
            opportunity_to_deferral[deferral.person_external_id][
                deferral.opportunity_type
            ] = deferral

        opportunities = get_fixture_opportunities()
        return [
            OpportunityPresenter(
                opportunity,
                opportunity_to_deferral[opportunity.person_external_id].get(
                    opportunity.opportunity_type
                ),
            )
            for opportunity in opportunities
        ]
