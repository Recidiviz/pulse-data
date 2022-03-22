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

from sqlalchemy.orm import Session

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.persistence.database.schema.justice_counts.schema import User


class UserInterface:
    """Contains methods for setting and getting User info."""

    @staticmethod
    def create_user(session: Session, auth0_user_id: str) -> None:
        session.add(User(auth0_user_id=auth0_user_id))

    @staticmethod
    def get_user_by_auth0_user_id(session: Session, auth0_user_id: str) -> User:
        return (
            session.query(User)
            .filter(User.auth0_user_id == auth0_user_id)
            .one_or_none()
        )

    @staticmethod
    def add_agency_to_user(
        session: Session, auth0_user_id: str, agency_name: str
    ) -> None:
        user = UserInterface.get_user_by_auth0_user_id(
            session=session, auth0_user_id=auth0_user_id
        )
        agency = AgencyInterface.get_agency_by_name(session=session, name=agency_name)
        user.agencies.append(agency)
        session.add(user)
