# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Interface for working with the AgencyUserAccountAssociation model."""
from sqlalchemy.orm import Session

from recidiviz.auth.auth0_client import Auth0Client
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.exceptions import JusticeCountsServerError
from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema


class AgencyUserAccountAssociationInterface:
    """Contains methods for working with the AgencyUserAccountAssociation."""

    @staticmethod
    def invite_user_to_agency(
        name: str,
        email: str,
        agency_id: int,
        auth0_client: Auth0Client,
        session: Session,
    ) -> None:
        """This method sends an invitation email to a new user via Auth0 and saves
        the cashed metadata in the Justice Counts DB so that we don't have to query
        Auth0 for information about which agencies the user belongs to, or their
        role/invitation status at each agency."""
        # First, check if user already exists, but is part of another agency.
        existing_users = UserAccountInterface.get_users_by_email(
            session=session, emails={email}
        )
        auth0_user_id = None
        existing_agencies = []
        if len(existing_users) > 1:
            raise JusticeCountsServerError(
                code="justice_counts_user_uniqueness",
                description=f"Multiple users exist with the same email: {email}",
            )

        if len(existing_users) == 1:
            # If there is an existing user, update the users list of agency ids in auth0.
            agency_ids = [agency.id for agency in existing_users[0].agencies] + [
                agency_id
            ]
            auth0_client.update_user_app_metadata(
                app_metadata={"agency_ids": agency_ids}
            )
            auth0_user_id = existing_users[0].auth0_user_id
            existing_agencies = existing_users[0].agencies + [
                AgencyInterface.get_agency_by_id(session=session, agency_id=agency_id)
            ]

        if len(existing_users) == 0:
            # If there is no existing user, create one in auth0.
            auth0_user = auth0_client.create_JC_user(
                name=name, email=email, agency_id=agency_id
            )
            auth0_user_id = auth0_user["user_id"]
            existing_agencies = [
                AgencyInterface.get_agency_by_id(session=session, agency_id=agency_id)
            ]

        # Update / create user in Justice Counts DB. This call will update the invitation status well.
        UserAccountInterface.create_or_update_user(
            session=session,
            name=name,
            auth0_user_id=auth0_user_id,
            agencies=existing_agencies,
            email=email,
            agency_id_to_invitation_status={
                agency_id: schema.UserAccountInvitationStatus.PENDING
            },
        )
