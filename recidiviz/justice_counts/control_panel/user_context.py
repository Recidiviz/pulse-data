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
"""Class to sotre information about the currently logged in user of the Justice Counts Control Panel."""


from typing import List, Optional

import attr
from flask_sqlalchemy_session import current_session

from recidiviz.justice_counts.user_account import UserAccountInterface
from recidiviz.persistence.database.schema.justice_counts import schema


@attr.define
class UserContext:
    """Stores information about the currently logged in user."""

    auth0_user_id: str
    user_account: schema.UserAccount = attr.field()
    permissions: Optional[List[str]] = None

    @user_account.default
    def _user_account_factory(self) -> schema.UserAccount:
        return UserAccountInterface.get_user_by_auth0_user_id(
            current_session, auth0_user_id=self.auth0_user_id
        )
