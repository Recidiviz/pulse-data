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
from datetime import date, datetime, timedelta
from hashlib import sha256
from typing import Dict, Optional

import attr
import pytz

from recidiviz.case_triage.authorization import KNOWN_EXPERIMENTS, AuthorizationStore
from recidiviz.case_triage.demo_helpers import (
    DEMO_FROZEN_DATE,
    DEMO_FROZEN_DATETIME,
    DEMO_STATE_CODE,
    fake_officer_id_for_demo_user,
    fake_person_id_for_demo_user,
)
from recidiviz.persistence.database.schema.case_triage.schema import (
    ETLClient,
    ETLOfficer,
    ETLOpportunity,
)


@attr.s
class UserContext:
    """Storing context and permissions for all of the operations for Case Triage"""

    email: str = attr.ib()
    can_impersonate: bool = attr.ib(default=False)
    can_see_demo_data: bool = attr.ib(default=False)
    current_user: ETLOfficer = attr.ib(default=None)
    known_experiments: Dict[str, Optional[str]] = attr.ib(default=dict)

    @classmethod
    def base_context_for_email(
        cls, email: str, authorization_store: AuthorizationStore
    ) -> "UserContext":
        return UserContext(
            email=email,
            can_impersonate=authorization_store.can_impersonate_others(email),
            can_see_demo_data=authorization_store.can_see_demo_data(email),
            known_experiments={
                exp: authorization_store.get_feature_variant(exp, email)
                for exp in KNOWN_EXPERIMENTS
            },
        )

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

    @property
    def demo_timedelta_shift_from_today(self) -> Optional[timedelta]:
        if self.should_see_demo:
            return date.today() - DEMO_FROZEN_DATE
        return None

    @property
    def demo_timedelta_shift_to_today(self) -> Optional[timedelta]:
        if self.should_see_demo:
            return DEMO_FROZEN_DATE - date.today()
        return None

    @property
    def demo_datetime(self) -> Optional[datetime]:
        if self.should_see_demo:
            return DEMO_FROZEN_DATETIME
        return None

    def now(self) -> datetime:
        if self.should_see_demo:
            return DEMO_FROZEN_DATETIME
        return datetime.now(tz=pytz.UTC)

    @staticmethod
    def segment_user_id_for_email(email: str) -> str:
        email_as_bytes = email.lower().encode("ascii")
        digest = sha256(email_as_bytes).digest()
        return b64encode(digest).decode("utf-8")
