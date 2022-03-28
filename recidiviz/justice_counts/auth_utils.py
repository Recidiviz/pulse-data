# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Utils for Justice Counts"""
from flask import session

from recidiviz.justice_counts.exceptions import JusticeCountsAuthorizationError
from recidiviz.utils.auth.auth0 import TokenClaims, update_session_with_user_info


def on_successful_authorization(jwt_claims: TokenClaims) -> None:
    auth_error = JusticeCountsAuthorizationError(
        code="no_justice_counts_access",
        description="You are not authorized to access this application",
    )

    update_session_with_user_info(session, jwt_claims, auth_error)
