# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Implements helper functions for use in Auth endpoint."""

import base64
import hashlib
from typing import Any


def generate_user_hash(email: str) -> bytes:
    return base64.b64encode(hashlib.sha256(email.encode("utf-8")).digest())


def format_user_info(user: Any) -> dict[str, str]:
    return {
        "emailAddress": user.email_address,
        "stateCode": user.state_code,
        "externalId": user.external_id,
        "role": user.role,
        "district": user.district,
        "firstName": user.first_name,
        "lastName": user.last_name,
        "allowedSupervisionLocationIds": user.district
        if user.state_code == "US_MO"
        else "",
        "allowedSupervisionLocationLevel": "level_1_supervision_location"
        if user.state_code == "US_MO" and user.district is not None
        else "",
        "canAccessLeadershipDashboard": user.can_access_leadership_dashboard,
        "canAccessCaseTriage": user.can_access_case_triage,
        "shouldSeeBetaCharts": user.should_see_beta_charts,
        "routes": user.routes,
        "featureVariants": user.feature_variants,
        "blocked": user.blocked,
        "userHash": user.user_hash,
    }
