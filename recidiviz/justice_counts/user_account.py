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

from sqlalchemy import and_, not_, or_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from recidiviz.justice_counts.control_panel.utils import is_demo_agency
from recidiviz.persistence.database.schema.justice_counts.schema import (
    Agency,
    AgencyUserAccountAssociation,
    UserAccount,
    UserAccountInvitationStatus,
    UserAccountRole,
)
from recidiviz.utils import metadata
from recidiviz.utils.environment import (
    GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION,
    GCP_PROJECT_JUSTICE_COUNTS_STAGING,
)


class UserAccountInterface:
    """Contains methods for setting and getting User info."""

    @staticmethod
    def get_role_from_email(email: str, agency_name: str) -> UserAccountRole:
        if "@recidiviz.org" in email:
            return UserAccountRole.JUSTICE_COUNTS_ADMIN

        if "@csg.org" in email:
            if (
                metadata.project_id() == GCP_PROJECT_JUSTICE_COUNTS_PRODUCTION
                and is_demo_agency(agency_name=agency_name)
            ) or metadata.project_id() == GCP_PROJECT_JUSTICE_COUNTS_STAGING:
                return UserAccountRole.AGENCY_ADMIN

            return UserAccountRole.READ_ONLY

        return UserAccountRole.AGENCY_ADMIN

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
        preserve_role: Optional[bool] = False,
    ) -> None:
        """If there is an existing association between the user and given agency in our DB,
        then update the invitation status and role (if preserve_role is False).
        Else, create a new association.
        """

        # Flush session to insure that any newly created agencies/user have ids.
        session.flush()

        # Prepare all the values that should be "upserted" to the DB
        values = []
        for agency in agencies:
            value = {"agency_id": agency.id, "user_account_id": user.id}
            if invitation_status is not None:
                value["invitation_status"] = invitation_status
            if preserve_role is False:
                role = (
                    role
                    if role is not None
                    else UserAccountInterface.get_role_from_email(
                        email=user.email, agency_name=agency.name
                    )
                )
                value["role"] = role

            values.append(value)

        insert_statement = insert(AgencyUserAccountAssociation).values(values)

        # update_columns represents the columns that should be updated on conflict
        update_columns = {}
        if preserve_role is False:
            update_columns["role"] = insert_statement.excluded.role
        if invitation_status is not None:
            update_columns[
                "invitation_status"
            ] = insert_statement.excluded.invitation_status
        if len(update_columns) > 0:
            # If invitation status or role is provided, update any existing the
            # agency_user_account_associations with the role / invitation status.
            insert_statement = insert_statement.on_conflict_do_update(
                constraint="agency_user_account_association_pkey",
                set_=update_columns,
            )
        if len(update_columns) == 0:
            # If invitation status or role is not provided, do nothing if an
            # agency_user_account_association already exists for that user / agency.
            insert_statement = insert_statement.on_conflict_do_nothing(
                constraint="agency_user_account_association_pkey",
            )
        session.execute(insert_statement)

    @staticmethod
    def remove_user_from_agencies(
        session: Session,
        user: UserAccount,
        agency_ids: List[int],
    ) -> None:
        existing_agency_assocs_by_id = {
            assoc.agency.id: assoc for assoc in user.agency_assocs
        }
        for agency_id in agency_ids:
            existing_assoc = existing_agency_assocs_by_id.get(int(agency_id))
            if existing_assoc is not None:
                session.delete(existing_assoc)

    @staticmethod
    def get_users(session: Session) -> List[UserAccount]:
        return session.query(UserAccount).order_by(UserAccount.id).all()

    @staticmethod
    def get_user_by_auth0_user_id(session: Session, auth0_user_id: str) -> UserAccount:
        return (
            session.query(UserAccount)
            .filter(UserAccount.auth0_user_id == auth0_user_id)
            .one_or_none()
        )

    @staticmethod
    def get_user_by_id(session: Session, user_account_id: int) -> UserAccount:
        return (
            session.query(UserAccount).filter(UserAccount.id == user_account_id).one()
        )

    @staticmethod
    def get_users_by_email(session: Session, emails: Set[str]) -> List[UserAccount]:
        return session.query(UserAccount).filter(UserAccount.email.in_(emails)).all()

    @staticmethod
    def get_user_by_email(session: Session, email: str) -> Optional[UserAccount]:
        return (
            session.query(UserAccount).filter(UserAccount.email == email).one_or_none()
        )

    @staticmethod
    def get_csg_users(session: Session) -> List[UserAccount]:
        return (
            session.query(UserAccount)
            .filter(UserAccount.email.contains("@csg.org"))
            .all()
        )

    @staticmethod
    def get_csg_and_recidiviz_users(session: Session) -> List[UserAccount]:
        return (
            session.query(UserAccount)
            .filter(
                or_(
                    UserAccount.email.contains("@csg.org"),
                    UserAccount.email.contains("@recidiviz.org"),
                )
            )
            .all()
        )

    @staticmethod
    def get_non_csg_and_recidiviz_users(session: Session) -> List[UserAccount]:
        return (
            session.query(UserAccount)
            .filter(
                and_(
                    not_(UserAccount.email.contains("@csg.org")),
                    not_(UserAccount.email.contains("@recidiviz.org")),
                )
            )
            .all()
        )
