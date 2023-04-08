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
import logging
from typing import Any, Optional, Tuple

from flask import request

from recidiviz.utils import metadata, validate_jwt

_UNKNOWN_USER = "unknown"
_REASON_KEY = "reason"


def replace_char_0_slash(user_hash: str) -> str:
    return user_hash[:1].replace("/", "_") + user_hash[1:]


def generate_user_hash(email: str) -> str:
    user_hash = base64.b64encode(hashlib.sha256(email.encode("utf-8")).digest()).decode(
        "utf-8"
    )
    return replace_char_0_slash(user_hash)


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
        "routes": user.routes,
        "featureVariants": user.feature_variants,
        "blocked": user.blocked,
        "userHash": user.user_hash,
    }


def get_authenticated_user_email() -> Tuple[str, Optional[str]]:
    jwt = request.headers.get("x-goog-iap-jwt-assertion")
    if not jwt:
        return (_UNKNOWN_USER, None)

    project_id = metadata.project_id()
    project_number = metadata.project_number()
    (
        _user_id,
        user_email,
        error_str,
    ) = validate_jwt.validate_iap_jwt_from_app_engine(jwt, project_number, project_id)
    return user_email or _UNKNOWN_USER, error_str


def log_reason(request_dict: dict[str, Any], action: str) -> None:
    reason = request_dict.pop(_REASON_KEY, None)
    if not reason:
        raise ValueError("Request is missing a reason")

    authenticated_user, error_str = get_authenticated_user_email()
    if error_str:
        logging.error("Error determining logged-in user: %s", error_str)

    logging.info(
        "State User Permissions: [%s] is %s with reason: %s",
        authenticated_user,
        action,
        reason,
    )
