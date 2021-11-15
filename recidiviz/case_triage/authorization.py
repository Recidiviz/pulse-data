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
"""
This module contains various pieces related to the Case Triage authentication /
authorization flow. It also has added support for feature gating and variant
selection.
"""
import json
import logging
from datetime import date
from typing import Any, Dict, List, Optional, Set

import attr
import dateutil.parser
import sqlalchemy.orm.exc

from recidiviz.case_triage.util import CASE_TRIAGE_STATES, get_local_file
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.persistence.database.schema.case_triage.schema import (
    DashboardUserRestrictions,
    ETLOfficer,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.utils.environment import in_gcp
from recidiviz.utils.metadata import project_id

KNOWN_EXPERIMENTS: List[str] = [
    "can-see-client-timeline",
]


@attr.s(auto_attribs=True)
class FeatureGateInfo:
    """Represents what variant a user belongs to and at what time
    that variant is active."""

    variant: str
    active_date: Optional[date] = attr.ib(default=None)

    @staticmethod
    def from_json(json_dict: Dict[str, str]) -> "FeatureGateInfo":
        f = FeatureGateInfo(variant=json_dict["variant"])
        if date_str := json_dict.get("active_date"):
            try:
                f.active_date = dateutil.parser.parse(date_str).date()
            except (TypeError, ValueError):
                logging.warning(
                    "invalid date found in feature gate json: %s", json_dict
                )
        return f

    def get_current_variant(self, on_date: Optional[date] = None) -> Optional[str]:
        if on_date is None:
            on_date = date.today()
        if self.active_date is not None and self.active_date > on_date:
            return None
        return self.variant


@attr.s(auto_attribs=True)
class AccessPermissions:
    can_access_case_triage: bool
    can_access_leadership_dashboard: bool
    # The state codes that are accessible by users, by default empty
    impersonatable_state_codes: Set[str]

    def to_json(self) -> Dict[str, Any]:
        return {
            "canAccessCaseTriage": self.can_access_case_triage,
            "canAccessLeadershipDashboard": self.can_access_leadership_dashboard,
            "impersonatableStateCodes": list(self.impersonatable_state_codes),
        }


class AuthorizationStore:
    """
    Simple store that fetches the allowlist of early Case Triage users' email addresses.
    The allowlist is manually managed by Recidiviz employees.
    """

    def __init__(self) -> None:
        self.database_key = SQLAlchemyDatabaseKey.for_schema(SchemaType.CASE_TRIAGE)

        prefix = "" if not in_gcp() else f"{project_id()}-"
        self.allowlist_path = GcsfsFilePath.from_absolute_path(
            f"{prefix}case-triage-data/allowlist_v2.json"
        )
        self.feature_gate_path = GcsfsFilePath.from_absolute_path(
            f"{prefix}case-triage-data/feature_variants.json"
        )

        self.case_triage_allowed_users: List[str] = []
        self.case_triage_admin_users: List[str] = []
        self.case_triage_demo_users: List[str] = []

        # Map from feature name to a map of email addresses to variants
        # of the feature that they are in.
        self.feature_variants: Dict[str, Dict[str, FeatureGateInfo]] = {}

    def refresh(self) -> None:
        self._refresh_auth_info()
        self._refresh_feature_gates()

    def _refresh_auth_info(self) -> None:
        data = json.loads(get_local_file(self.allowlist_path))
        self.case_triage_allowed_users = [struct["email"] for struct in data]
        self.case_triage_admin_users = [
            struct["email"] for struct in data if struct.get("is_admin")
        ]
        self.case_triage_demo_users = [
            struct["email"] for struct in data if struct.get("is_demo_user")
        ]

    def _refresh_feature_gates(self) -> None:
        feature_gate_json = json.loads(get_local_file(self.feature_gate_path))

        self.feature_variants = {
            feature: {
                email: FeatureGateInfo.from_json(gate_info)
                for email, gate_info in submap.items()
            }
            for feature, submap in feature_gate_json.items()
        }

    def get_access_permissions(self, email: str) -> AccessPermissions:
        """Retrieves the appropriate access permissions for different users of Case Triage.

        This method works by assuming no access and then adding access based on different
        access-granting sources.
        """
        email = email.lower()

        can_access_case_triage = False
        can_access_leadership_dashboard = False
        impersonatable_state_codes = set()

        if email in self.case_triage_allowed_users:
            can_access_case_triage |= True

        if email in self.case_triage_admin_users:
            impersonatable_state_codes |= CASE_TRIAGE_STATES

        if _is_recidiviz_employee(email):
            can_access_case_triage |= True
            can_access_leadership_dashboard |= True

        with SessionFactory.using_database(
            self.database_key, autocommit=False
        ) as session:
            try:
                permissions = (
                    session.query(DashboardUserRestrictions)
                    .filter(DashboardUserRestrictions.restricted_user_email == email)
                    .one()
                )

                can_access_case_triage |= permissions.can_access_case_triage
                can_access_leadership_dashboard |= (
                    permissions.can_access_leadership_dashboard
                )
                if permissions.can_access_leadership_dashboard:
                    # People who generally can accesss the leadership dashboard cannot impersonate others,
                    # but people who are marked as being able to access the leadership dashboard within
                    # dashboard_user_restrictions are currently allowed to impersonate others in their
                    # same state.
                    impersonatable_state_codes |= {permissions.state_code}
            except sqlalchemy.orm.exc.NoResultFound:
                logging.debug(
                    "Email %s has no entry in dashboard_user_restrictions", email
                )

        return AccessPermissions(
            can_access_case_triage=can_access_case_triage,
            can_access_leadership_dashboard=can_access_leadership_dashboard,
            impersonatable_state_codes=impersonatable_state_codes,
        )

    def can_impersonate(self, email: str, other_officer: ETLOfficer) -> bool:
        """Determines whether or not the current user with email
        can impersonate another user with other_email."""
        access_permissions = self.get_access_permissions(email.lower())
        return other_officer.state_code in access_permissions.impersonatable_state_codes

    def can_refresh_auth_store(self, email: str) -> bool:
        return email in self.case_triage_admin_users

    def can_see_demo_data(self, email: str) -> bool:
        return (
            email in self.case_triage_admin_users
            or email in self.case_triage_demo_users
            or _is_recidiviz_employee(email)
        )

    def get_feature_variant(
        self, feature: str, email: str, on_date: Optional[date] = None
    ) -> Optional[str]:
        """This returns the feature variant for a given user and returns
        None if the user is in the control (no variant assigned)."""
        feature_gate_info = self.feature_variants.get(feature, {}).get(email)
        if not feature_gate_info:
            return None
        return feature_gate_info.get_current_variant(on_date=on_date)


def _is_recidiviz_employee(email: str) -> bool:
    return email.lower().endswith("@recidiviz.org")
