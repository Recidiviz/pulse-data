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
from typing import Any, Dict, List, Optional, Union

from recidiviz.auth.auth0_client import (
    Auth0User,
    CaseTriageAuth0AppMetadata,
    JusticeCountsAuth0AppMetadata,
)

from ..rest import RestClientOptions

class Users:
    def list(
        self, per_page: int, fields: List[str], q: Optional[str] = None
    ) -> Dict[str, Any]: ...
    def update(
        self,
        id: str,
        body: Dict[
            str, Union[CaseTriageAuth0AppMetadata, JusticeCountsAuth0AppMetadata]
        ],
    ) -> Auth0User: ...
    def get(self, id: str) -> Auth0User: ...

class Jobs:
    def send_verification_email(self, body: Dict[str, Any]) -> None: ...

class Auth0:
    users: Users
    jobs: Jobs
    def __init__(
        self, domain: str, token: str, rest_options: RestClientOptions
    ) -> None: ...
