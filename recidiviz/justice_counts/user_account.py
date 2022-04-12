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

from typing import List, Optional

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import coalesce

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.persistence.database.schema.justice_counts.schema import UserAccount
from recidiviz.reporting.email_reporting_utils import validate_email_address


class UserAccountInterface:
    """Contains methods for setting and getting User info."""

    @staticmethod
    def create_user(
        session: Session,
        email_address: str,
        name: Optional[str] = None,
        auth0_user_id: Optional[str] = None,
        agency_ids: Optional[List[int]] = None,
    ) -> UserAccount:
        """Creates a user"""
        validate_email_address(email_address)
        agencies = []

        if agency_ids is not None:
            agencies = AgencyInterface.get_agencies_by_id(
                session=session, agency_ids=agency_ids
            )

        user = UserAccount(
            email_address=email_address,
            name=name,
            auth0_user_id=auth0_user_id,
            agencies=agencies,
        )

        session.add(user)
        session.commit()
        return user

    @staticmethod
    def create_or_update_user(
        session: Session,
        email_address: str,
        name: Optional[str] = None,
        auth0_user_id: Optional[str] = None,
        agency_ids: Optional[List[int]] = None,
    ) -> UserAccount:
        """Creates a user or updates an existing user"""
        validate_email_address(email_address)
        insert_statement = insert(UserAccount).values(
            email_address=email_address, name=name, auth0_user_id=auth0_user_id
        )
        insert_statement = insert_statement.on_conflict_do_update(
            constraint="unique_email_address",
            set_=dict(
                auth0_user_id=coalesce(  # coalesce chooses the first non-null item on the list
                    insert_statement.excluded.auth0_user_id,  # excluded refers to the row that failed to insert due to the conflict
                    UserAccount.auth0_user_id,  # this refers to the existing row to be updated.
                ),  # Altogether, this statement updates the existing value with the new value if the new value is not null
                name=coalesce(insert_statement.excluded.name, UserAccount.name),
            ),
        )

        result = session.execute(insert_statement)

        user = session.query(UserAccount).get(result.inserted_primary_key)
        if agency_ids is not None:
            agencies = AgencyInterface.get_agencies_by_id(
                session=session, agency_ids=agency_ids
            )
            user.agencies = agencies

        session.commit()
        return user

    @staticmethod
    def get_users(session: Session) -> List[UserAccount]:
        return session.query(UserAccount).all()

    @staticmethod
    def get_user_by_email_address(session: Session, email_address: str) -> UserAccount:
        return (
            session.query(UserAccount)
            .filter(UserAccount.email_address == email_address)
            .one()
        )

    @staticmethod
    def get_user_by_auth0_user_id(session: Session, auth0_user_id: str) -> UserAccount:
        return (
            session.query(UserAccount)
            .filter(UserAccount.auth0_user_id == auth0_user_id)
            .one()
        )

    @staticmethod
    def get_user_by_id(session: Session, user_account_id: int) -> UserAccount:
        return session.query(UserAccount).get(user_account_id)

    @staticmethod
    def add_agency_to_user(
        session: Session, email_address: str, agency_name: str
    ) -> None:
        user = UserAccountInterface.get_user_by_email_address(
            session=session, email_address=email_address
        )
        agency = AgencyInterface.get_agency_by_name(session=session, name=agency_name)
        user.agencies.append(agency)
        session.add(user)
        session.commit()
