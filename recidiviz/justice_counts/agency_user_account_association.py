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
        """This method does the following:
        1. Checks if the user already exists, but is part of another agency.
        2. If so, we update the users's list of agencies in Auth0, and in our DB.
        3. If the user does not exist, we create the user in Auth0 and in our DB,
        and then we add them to the agency.
        """
        # First, check if user already exists, but is part of another agency.
        existing_users = UserAccountInterface.get_users_by_email(
            session=session, emails={email}
        )
        if len(existing_users) > 1:
            raise JusticeCountsServerError(
                code="justice_counts_user_uniqueness",
                description=f"Multiple users exist with the same email: {email}",
            )

        new_agency = AgencyInterface.get_agency_by_id(
            session=session, agency_id=agency_id
        )

        if len(existing_users) == 1:
            # If there is an existing user, update the users list of agency ids in auth0.
            existing_user = existing_users[0]
            existing_agency_ids = {
                assoc.agency.id for assoc in existing_user.agency_assocs
            }
            if agency_id in existing_agency_ids:
                # User already belongs to this agency
                pass

            # Update Auth0
            agency_ids = list(existing_agency_ids) + [agency_id]
            auth0_client.update_user_app_metadata(
                user_id=existing_user.auth0_user_id,
                app_metadata={"agency_ids": agency_ids},
            )

            # Update our DB
            UserAccountInterface.add_or_update_user_agency_association(
                session=session,
                user=existing_users[0],
                agencies=[new_agency],
                invitation_status=schema.UserAccountInvitationStatus.PENDING,
            )

        elif len(existing_users) == 0:
            # If there is no existing user, create one in Auth0.
            auth0_user = auth0_client.create_JC_user(
                name=name, email=email, agency_id=agency_id
            )
            auth0_user_id = auth0_user["user_id"]

            # Create user in our DB
            user = UserAccountInterface.create_or_update_user(
                session=session,
                name=name,
                auth0_user_id=auth0_user_id,
                email=email,
            )

            # Add the user to the agency
            UserAccountInterface.add_or_update_user_agency_association(
                session=session,
                user=user,
                agencies=[new_agency],
                invitation_status=schema.UserAccountInvitationStatus.PENDING,
            )

    @staticmethod
    def remove_user_from_agency(
        email: str,
        agency_id: int,
        auth0_client: Auth0Client,
        session: Session,
    ) -> None:
        """This method removes the agency_id from the users metadata in auth0 and
        deletes in the AgencyUserAccountAssociation between the user and the agency
        in the Justice Counts DB."""
        users = UserAccountInterface.get_users_by_email(session=session, emails={email})
        # Remove agency from users list of of agency ids in auth0.
        if len(users) > 1:
            raise JusticeCountsServerError(
                code="justice_counts_user_uniqueness",
                description=f"Multiple users exist with the same email: {email}",
            )
        user = users[0]
        agency_ids = [
            agency_assoc.agency_id
            for agency_assoc in user.agency_assocs
            if agency_assoc.agency_id != agency_id
        ]
        auth0_client.update_user_app_metadata(
            user_id=user.auth0_user_id, app_metadata={"agency_ids": agency_ids}
        )

        # Update user in Justice Counts DB
        UserAccountInterface.remove_user_from_agency(
            session=session,
            user=user,
            agency_id=agency_id,
        )

    @staticmethod
    def update_user_role(
        session: Session, role: str, agency: schema.Agency, user: schema.UserAccount
    ) -> None:
        db_role = schema.UserAccountRole(role)
        UserAccountInterface.add_or_update_user_agency_association(
            session=session, user=user, agencies=[agency], role=db_role
        )
