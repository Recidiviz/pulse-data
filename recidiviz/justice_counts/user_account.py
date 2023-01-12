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

from typing import Dict, List, Optional, Set

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import coalesce

from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    AgencyUserAccountAssociation,
    UserAccount,
)


class UserAccountInterface:
    """Contains methods for setting and getting User info."""

    @staticmethod
    def create_or_update_user(
        session: Session,
        agencies: Optional[List[schema.Agency]] = None,
        name: Optional[str] = None,
        auth0_user_id: Optional[str] = None,
        email: Optional[str] = None,
        agency_id_to_invitation_status: Optional[
            Dict[int, schema.UserAccountInvitationStatus]
        ] = None,
    ) -> UserAccount:
        """Creates a user or updates an existing user"""
        insert_statement = insert(UserAccount).values(
            name=name,
            auth0_user_id=auth0_user_id,
            email=email,
        )

        insert_statement = insert_statement.on_conflict_do_update(
            constraint="unique_auth0_user_id",
            set_=dict(
                auth0_user_id=coalesce(  # coalesce chooses the first non-null item on the list
                    insert_statement.excluded.auth0_user_id,  # excluded refers to the row that failed to insert due to the conflict
                    UserAccount.auth0_user_id,  # this refers to the existing row to be updated.
                ),  # Altogether, this statement updates the existing value with the new value if the new value is not null
                name=coalesce(insert_statement.excluded.name, UserAccount.name),
                email=coalesce(insert_statement.excluded.email, UserAccount.email),
            ),
        )
        result = session.execute(insert_statement)
        user = session.query(UserAccount).get(result.inserted_primary_key)
        if agencies is not None:
            user.agencies = agencies

        if agency_id_to_invitation_status is not None:
            user_accounts_associations = (
                session.query(AgencyUserAccountAssociation)
                .filter(
                    AgencyUserAccountAssociation.user_account_id == user.id,
                    AgencyUserAccountAssociation.agency_id.in_(
                        list(agency_id_to_invitation_status.keys())
                    ),
                )
                .all()
            )
            for association in user_accounts_associations:
                association.invitation_status = agency_id_to_invitation_status.get(
                    association.agency_id
                )

        return user

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
