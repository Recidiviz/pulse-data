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
from itertools import groupby
from typing import Dict, List, Optional, Tuple

import sqlalchemy.orm.exc
from sqlalchemy.orm import Session, joinedload

from recidiviz.case_triage.demo_helpers import (
    get_fixture_clients,
    get_fixture_opportunities,
    unconvert_fake_person_id_for_demo_user,
)
from recidiviz.case_triage.opportunities.models import ComputedOpportunity
from recidiviz.case_triage.opportunities.types import (
    Opportunity,
    OpportunityDoesNotExistError,
    OpportunityType,
)
from recidiviz.case_triage.querier.case_presenter import CasePresenter
from recidiviz.case_triage.querier.opportunity_presenter import OpportunityPresenter
from recidiviz.case_triage.user_context import UserContext
from recidiviz.persistence.database.schema.case_triage.schema import (
    CaseUpdate,
    ClientInfo,
    ETLClient,
    ETLOfficer,
    ETLOpportunity,
    OfficerNote,
    OpportunityDeferral,
)


class PersonDoesNotExistError(ValueError):
    pass


class CaseTriageQuerier:
    """Implements Querier abstraction for Case Triage data sources."""

    @staticmethod
    def fetch_etl_client(
        session: Session, person_external_id: str, state_code: str
    ) -> ETLClient:
        try:
            return (
                session.query(ETLClient)
                .filter(
                    ETLClient.state_code == state_code,
                    ETLClient.person_external_id == person_external_id,
                )
                .one()
            )
        except sqlalchemy.orm.exc.NoResultFound as e:
            raise PersonDoesNotExistError(
                f"could not find id: {person_external_id}"
            ) from e

    @staticmethod
    def etl_client_for_officer(
        session: Session, user_context: UserContext, person_external_id: str
    ) -> ETLClient:
        """Finds the appropriate client context for a given officer."""
        if user_context.should_see_demo:
            clients = get_fixture_clients()

            unconverted_person_id = unconvert_fake_person_id_for_demo_user(
                person_external_id
            )

            for client in clients:
                if client.person_external_id == unconverted_person_id:
                    return client

            raise PersonDoesNotExistError(f"could not find id: {person_external_id}")
        if user_context.current_user:
            try:
                return (
                    session.query(ETLClient)
                    .filter(
                        ETLClient.state_code == user_context.officer_state_code,
                        ETLClient.supervising_officer_external_id
                        == user_context.officer_id,
                        ETLClient.person_external_id == person_external_id,
                    )
                    .one()
                )
            except sqlalchemy.orm.exc.NoResultFound as e:
                raise PersonDoesNotExistError(
                    f"could not find id: {person_external_id}"
                ) from e
        raise ValueError("Not authorized to view clients")

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
        session: Session, user_context: UserContext
    ) -> List[CasePresenter]:
        """Outputs the list of clients for a given officer in CasePresenter form."""
        if user_context.should_see_demo:
            # Organize CaseUpdates
            case_updates = (
                session.query(CaseUpdate)
                .filter(CaseUpdate.officer_external_id == user_context.officer_id)
                .all()
            )
            client_ids_to_case_updates = defaultdict(list)
            for case_update in case_updates:
                client_ids_to_case_updates[case_update.person_external_id].append(
                    case_update
                )

            clients = get_fixture_clients()
            for client in clients:
                client.person_external_id = user_context.person_id(client)

            # Organize ClientInfo structs
            client_infos = (
                session.query(ClientInfo)
                .filter(
                    ClientInfo.person_external_id.in_(
                        (client.person_external_id for client in clients)
                    )
                )
                .all()
            )
            client_ids_to_client_info = {}
            for client_info in client_infos:
                client_ids_to_client_info[client_info.person_external_id] = client_info

            # Organize OfficerNotes
            notes = session.query(OfficerNote).filter(
                OfficerNote.person_external_id.in_(
                    (client.person_external_id for client in clients)
                )
            )
            client_ids_to_notes = defaultdict(list)
            for note in notes:
                client_ids_to_notes[note.person_external_id].append(note)

            for client in clients:
                if client_info := client_ids_to_client_info.get(
                    client.person_external_id
                ):
                    client.client_info = client_info
                client.notes = client_ids_to_notes[client.person_external_id]

            return [
                CasePresenter(
                    client, client_ids_to_case_updates[client.person_external_id]
                )
                for client in clients
            ]
        if user_context.current_user:
            clients = (
                session.query(ETLClient)
                .filter(
                    ETLClient.etl_officer == user_context.current_user,
                    ETLClient.state_code == user_context.officer_state_code,
                )
                .options(joinedload(ETLClient.case_updates))
                .options(joinedload(ETLClient.client_info))
                .all()
            )
            return [CasePresenter(client, client.case_updates) for client in clients]

        raise ValueError("Not authorized to view clients")

    @staticmethod
    def officer_for_email(session: Session, officer_email: str) -> ETLOfficer:
        email = officer_email.lower()
        return session.query(ETLOfficer).filter_by(email_address=email).one()

    @staticmethod
    def opportunities_for_officer(
        session: Session, user_context: UserContext
    ) -> List[OpportunityPresenter]:
        """Fetches all opportunities for an officer."""
        if user_context.should_see_demo:
            opportunity_deferrals = (
                session.query(OpportunityDeferral)
                .filter(
                    OpportunityDeferral.supervising_officer_external_id
                    == user_context.officer_id
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

            etl_opportunities = get_fixture_opportunities()
            for opportunity in etl_opportunities:
                opportunity.person_external_id = user_context.opportunity_id(
                    opportunity
                )

            # TODO(#8077): Unify computed and augmented opportunities.
            clients = CaseTriageQuerier.clients_for_officer(session, user_context)
            unemployed = [
                ComputedOpportunity(
                    state_code=user_context.officer_state_code,
                    supervising_officer_external_id=user_context.officer_id,
                    person_external_id=user_context.person_id(c.etl_client),
                    opportunity_type=OpportunityType.EMPLOYMENT.value,
                    opportunity_metadata={},
                )
                for c in clients
                if c.etl_client.employer is None
                and not c.etl_client.receiving_ssi_or_disability_income
            ]

            opportunities: List[Opportunity] = [*etl_opportunities, *unemployed]

            return [
                OpportunityPresenter(
                    opportunity,
                    opportunity_to_deferral[opportunity.person_external_id].get(
                        opportunity.opportunity_type
                    ),
                )
                for opportunity in opportunities
            ]
        if user_context.current_user:
            etl_opportunity_info = (
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
                    ETLOpportunity.supervising_officer_external_id
                    == user_context.officer_id,
                    ETLOpportunity.state_code == user_context.officer_state_code,
                )
                .all()
            )
            # we'll fill this with opportunities computed on the fly based on client conditions
            computed_opportunity_info: List[
                Tuple[ComputedOpportunity, Optional[OpportunityDeferral]]
            ] = []

            # one query to fetch all clients and their associated opportunity deferrals
            client_opportunities: List[
                Tuple[ETLClient, Optional[OpportunityDeferral]]
            ] = (
                session.query(ETLClient, OpportunityDeferral)
                .filter(
                    ETLClient.state_code == user_context.officer_state_code,
                    ETLClient.supervising_officer_external_id
                    == user_context.officer_id,
                )
                .outerjoin(
                    OpportunityDeferral,
                    OpportunityDeferral.person_external_id
                    == ETLClient.person_external_id,
                )
                .order_by(ETLClient.person_external_id)
                .all()
            )
            # deferrals are not grouped in DB result because there isn't a proper relationship
            # to clients; now we group them and iterate over clients to find opportunities
            for client, client_rows in groupby(
                client_opportunities, lambda row: row[0]
            ):
                deferrals = [row[1] for row in client_rows if row[1] is not None]
                # employment opportunities
                if (
                    client.employer is None
                    and not client.receiving_ssi_or_disability_income
                ):
                    opp = ComputedOpportunity(
                        state_code=user_context.officer_state_code,
                        supervising_officer_external_id=user_context.officer_id,
                        person_external_id=user_context.person_id(client),
                        opportunity_type=OpportunityType.EMPLOYMENT.value,
                        opportunity_metadata={},
                    )
                    deferral = next(
                        (
                            d
                            for d in deferrals
                            if d.opportunity_type == OpportunityType.EMPLOYMENT.value
                        ),
                        None,
                    )
                    computed_opportunity_info.append((opp, deferral))
                # TODO(#8077): compare time since contact and assessment dates to us_id_policy_requirements
                # to generate opportunities for upcoming/overdue contact and assessment

            return [
                *[
                    OpportunityPresenter(*info)
                    for info in [*etl_opportunity_info, *computed_opportunity_info]
                ],
            ]

        raise ValueError("Not authorized to view client opportunities.")

    @staticmethod
    def fetch_etl_opportunity(
        session: Session,
        user_context: UserContext,
        client: ETLClient,
        opportunity_type: OpportunityType,
    ) -> ETLOpportunity:
        """Fetches a given opportunity for an officer and client."""
        if user_context.should_see_demo:
            opportunities = get_fixture_opportunities()
            for opp in opportunities:
                if (
                    opp.person_external_id == client.person_external_id
                    and opp.opportunity_type == opportunity_type.value
                ):
                    return opp
        elif user_context.current_user and client:
            try:
                return (
                    session.query(ETLOpportunity)
                    .filter(
                        ETLOpportunity.state_code
                        == user_context.client_state_code(client),
                        ETLOpportunity.supervising_officer_external_id
                        == user_context.officer_id,
                        ETLOpportunity.person_external_id
                        == user_context.person_id(client),
                        ETLOpportunity.opportunity_type == opportunity_type.value,
                    )
                    .one()
                )
            except sqlalchemy.orm.exc.NoResultFound as e:
                raise OpportunityDoesNotExistError(
                    f"Could not find opportunity for officer: {user_context.officer_id}, "
                    f"person: {user_context.person_id(client)}, opportunity_type: {opportunity_type}"
                ) from e

        raise OpportunityDoesNotExistError(
            f"No opportunity exists with type {opportunity_type} and "
            f"for person {user_context.person_id(client)}"
        )
