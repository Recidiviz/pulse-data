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
"""Interface for working with the User model."""

from typing import List, Optional, Set

from sqlalchemy.orm import Session, joinedload

from recidiviz.persistence.database.schema.justice_counts.schema import (
    Agency,
    AgencyUserAccountAssociation,
    UserAccount,
    UserAccountInvitationStatus,
    UserAccountRole,
)


class UserAccountInterface:
    """Contains methods for setting and getting User info."""

    @staticmethod
    def create_or_update_user(
        session: Session,
        auth0_user_id: str,
        name: Optional[str] = None,
        email: Optional[str] = None,
    ) -> UserAccount:
        """If there is an existing user in our DB with this Auth0 ID,
        then update their name and/or email. Else, create a new user
        in our DB with this Auth0 ID, name, and email.
        NOTE: This method does not create or update the user's info in Auth0.
        That is handled in api.py via a call to auth0_client.update_user.
        """
        existing_user = UserAccountInterface.get_user_by_auth0_user_id(
            session=session, auth0_user_id=auth0_user_id
        )

        if existing_user is not None:
            if name is not None:
                existing_user.name = name
            if email is not None:
                existing_user.email = email
            return existing_user

        user = UserAccount(
            name=name,
            auth0_user_id=auth0_user_id,
            email=email,
        )

        session.add(user)
        return user

    @staticmethod
    def add_or_update_user_agency_association(
        session: Session,
        user: UserAccount,
        agencies: List[Agency],
        invitation_status: Optional[UserAccountInvitationStatus] = None,
        role: Optional[UserAccountRole] = None,
    ) -> None:
        """If there is an existing association between the user and given agency in our DB,
        then update the invitation status and role. Else, create a new association.
        NOTE: This method does not create or update the user's list of agencies in Auth0.
        That must be done via auth0_client.update_user_app_metadata.
        """
        existing_agency_assocs_by_id = {
            assoc.agency_id: assoc for assoc in user.agency_assocs
        }

        for agency in agencies:
            existing_assoc = existing_agency_assocs_by_id.get(agency.id)
            if existing_assoc is not None:
                if invitation_status is not None:
                    existing_assoc.invitation_status = invitation_status
                if role is not None:
                    existing_assoc.role = role
                continue

            session.add(
                AgencyUserAccountAssociation(
                    user_account=user,
                    agency=agency,
                    invitation_status=invitation_status,
                )
            )

    @staticmethod
    def remove_user_from_agency(
        session: Session,
        user: UserAccount,
        agency_id: int,
    ) -> None:
        existing_agency_assocs_by_id = {
            assoc.agency.id: assoc for assoc in user.agency_assocs
        }
        existing_assoc = existing_agency_assocs_by_id.get(agency_id)
        session.delete(existing_assoc)

    @staticmethod
    def get_users(session: Session) -> List[UserAccount]:
        return session.query(UserAccount).order_by(UserAccount.id).all()

    @staticmethod
    def get_user_by_auth0_user_id(
        session: Session, auth0_user_id: str, include_agencies: bool = True
    ) -> UserAccount:
        q = session.query(UserAccount).filter(
            UserAccount.auth0_user_id == auth0_user_id
        )

        if include_agencies:
            q.options(joinedload(UserAccount.agency_assocs))

        return q.one_or_none()

    @staticmethod
    def get_user_by_id(session: Session, user_account_id: int) -> UserAccount:
        return (
            session.query(UserAccount).filter(UserAccount.id == user_account_id).one()
        )

    @staticmethod
    def get_users_by_id(
        session: Session, user_account_ids: Set[int]
    ) -> List[UserAccount]:
        return (
            session.query(UserAccount)
            .filter(UserAccount.id.in_(user_account_ids))
            .all()
        )

    @staticmethod
    def get_users_by_email(session: Session, emails: Set[str]) -> List[UserAccount]:
        return session.query(UserAccount).filter(UserAccount.email.in_(emails)).all()
