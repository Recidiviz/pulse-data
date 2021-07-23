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
"""A UserContext for all of the operations for Case Triage"""
from base64 import b64encode
from datetime import datetime
from enum import Enum
from functools import cached_property
from hashlib import sha256
from typing import Dict, Optional

import attr
import pytz

from recidiviz.case_triage.authorization import (
    KNOWN_EXPERIMENTS,
    AccessPermissions,
    AuthorizationStore,
)
from recidiviz.case_triage.demo_helpers import (
    DEMO_STATE_CODE,
    fake_officer_id_for_demo_user,
    fake_person_id_for_demo_user,
)
from recidiviz.persistence.database.schema.case_triage.schema import (
    ETLClient,
    ETLOfficer,
    ETLOpportunity,
)


class Permission(Enum):
    """Identifies various permissions that a user should have."""

    # User can perform all read and write operations.
    READ_WRITE = "READ_WRITE"

    # User can perform read operations only.
    READ_ONLY = "READ_ONLY"


@attr.s
class UserContext:
    """Storing context and permissions for all of the operations for Case Triage"""

    email: str = attr.ib()
    authorization_store: AuthorizationStore = attr.ib()
    current_user: ETLOfficer = attr.ib(default=None)

    @classmethod
    def base_context_for_email(
        cls, email: str, authorization_store: AuthorizationStore
    ) -> "UserContext":
        return UserContext(email=email, authorization_store=authorization_store)

    def can_impersonate(self, other_officer: ETLOfficer) -> bool:
        return self.authorization_store.can_impersonate(self.email, other_officer)

    @property
    def can_see_demo_data(self) -> bool:
        return self.authorization_store.can_see_demo_data(self.email)

    @property
    def known_experiments(self) -> Dict[str, Optional[str]]:
        return {
            exp: self.authorization_store.get_feature_variant(exp, self.email)
            for exp in KNOWN_EXPERIMENTS
        }

    @property
    def permission(self) -> Permission:
        if self.should_see_demo:
            return Permission.READ_WRITE
        if self.email == self.current_user.email_address:
            return Permission.READ_WRITE
        return Permission.READ_ONLY

    @property
    def segment_user_id(self) -> Optional[str]:
        if self.should_see_demo:
            return None
        return self.segment_user_id_for_email(self.email)

    @property
    def should_see_demo(self) -> bool:
        return self.current_user is None and self.can_see_demo_data

    @property
    def officer_id(self) -> str:
        if self.should_see_demo:
            return fake_officer_id_for_demo_user(self.email)
        return self.current_user.external_id

    def person_id(self, client: ETLClient) -> str:
        if self.should_see_demo:
            return fake_person_id_for_demo_user(self.email, client.person_external_id)
        return client.person_external_id

    def opportunity_id(self, opportunity: ETLOpportunity) -> str:
        if self.should_see_demo:
            return fake_person_id_for_demo_user(
                self.email, opportunity.person_external_id
            )
        return opportunity.person_external_id

    def client_state_code(self, client: ETLClient) -> str:
        if self.should_see_demo:
            return DEMO_STATE_CODE
        return client.state_code

    @property
    def officer_state_code(self) -> str:
        if self.should_see_demo:
            return DEMO_STATE_CODE
        return self.current_user.state_code

    @cached_property
    def access_permissions(self) -> AccessPermissions:
        return self.authorization_store.get_access_permissions(self.email)

    def now(self) -> datetime:
        return datetime.now(tz=pytz.UTC)

    @staticmethod
    def segment_user_id_for_email(email: str) -> str:
        email_as_bytes = email.lower().encode("ascii")
        digest = sha256(email_as_bytes).digest()
        return b64encode(digest).decode("utf-8")
