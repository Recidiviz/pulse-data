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
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from flask import request
from sqlalchemy import inspect
from sqlalchemy.engine.row import Row

from recidiviz.admin_panel.constants import LOAD_BALANCER_SERVICE_ID_SECRET_NAME
from recidiviz.auth.constants import PREDEFINED_ROLES
from recidiviz.persistence.database.schema.case_triage.schema import (
    Roster,
    UserOverride,
)
from recidiviz.utils import metadata, validate_jwt
from recidiviz.utils.secrets import get_secret

_UNKNOWN_USER = "unknown"
_REASON_KEY = "reason"


def replace_char_0_slash(user_hash: str) -> str:
    return user_hash[:1].replace("/", "_") + user_hash[1:]


def generate_user_hash(email: str) -> str:
    user_hash = base64.b64encode(hashlib.sha256(email.encode("utf-8")).digest()).decode(
        "utf-8"
    )
    return replace_char_0_slash(user_hash)


def generate_pseudonymized_id(
    state_code: str, external_id: Optional[str]
) -> Optional[str]:
    if not external_id:
        return None
    # urlsafe_b64encode ubstitutes - for + and _ for /
    # https://docs.python.org/3/library/base64.html#base64.urlsafe_b64encode
    # Needs to be kept in sync with recidiviz.calculator.query.bq_utils.get_pseudonymized_id_query_str
    # and any uses of it for pseudonymizing staff ids.
    return base64.urlsafe_b64encode(
        hashlib.sha256(f"{state_code}{external_id}".encode("utf-8")).digest()
    ).decode("utf-8")[:16]


def format_user_info(user: Any) -> dict[str, str]:
    return {
        "emailAddress": user.email_address,
        "stateCode": user.state_code,
        "externalId": user.external_id,
        "role": user.role,
        "district": user.district,
        "firstName": user.first_name,
        "lastName": user.last_name,
        "allowedSupervisionLocationIds": (
            user.district if user.state_code == "US_MO" else ""
        ),
        "allowedSupervisionLocationLevel": (
            "level_1_supervision_location"
            if user.state_code == "US_MO" and user.district is not None
            else ""
        ),
        "routes": user.routes,
        "featureVariants": user.feature_variants,
        "blocked": user.blocked,
        "userHash": user.user_hash,
    }


def get_authenticated_user_email() -> Tuple[str, Optional[str]]:
    jwt = request.headers.get("x-goog-iap-jwt-assertion")
    if not jwt:
        return (_UNKNOWN_USER, None)

    project_number = metadata.project_number()
    if not project_number:
        raise RuntimeError("Expected project_number to be set")

    backend_service_id = get_secret(LOAD_BALANCER_SERVICE_ID_SECRET_NAME)
    if not backend_service_id:
        raise RuntimeError(
            f"Missing backend service id secret named {LOAD_BALANCER_SERVICE_ID_SECRET_NAME}"
        )

    (
        _user_id,
        user_email,
        error_str,
    ) = validate_jwt.validate_iap_jwt_from_compute_engine(
        jwt, project_number, backend_service_id
    )
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


def merge_permissions(permissions_list: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Reconciles/prioritizes permissions if user has multiple roles. For routes, if any
    of a user's roles grant permission to a route, ignore/override when any of their
    other roles don't grant that permission. For feature variants, when comparing
    different permissions for the same variant, order of priority is:
        1) always on (value of variant will not have an activeDate attribute)
        2) earliest active date
        3) false
    """
    new_permissions: Dict[str, Any] = {}
    for permission in permissions_list:
        for k, v in permission.items():
            if k not in new_permissions or new_permissions[k] is False:
                new_permissions[k] = v
            elif (
                isinstance(new_permissions[k], dict)
                and "activeDate" in new_permissions[k]
            ):
                if isinstance(v, dict) and "activeDate" in v:
                    new_permissions[k]["activeDate"] = (
                        min(
                            datetime.fromisoformat(new_permissions[k]["activeDate"]),
                            datetime.fromisoformat(v["activeDate"]),
                        )
                        .isoformat(timespec="milliseconds")
                        .replace("+00:00", "Z")
                    )
                elif v is not False:
                    new_permissions[k] = v

    return new_permissions


def convert_to_dict_single_result(row: Row) -> Dict[str, Any]:
    row_dict = dict(row)
    row_dict["routes"] = merge_permissions(row_dict["routes"])
    row_dict["feature_variants"] = merge_permissions(row_dict["feature_variants"])
    return row_dict


def convert_to_dict_multiple_results(rows: List[Row]) -> List[Dict[str, Any]]:
    return [convert_to_dict_single_result(row) for row in rows]


def validate_roles(user_dict: Dict[str, Any]) -> None:
    email_address = user_dict.get("email_address")
    roles = user_dict.get("roles")

    if roles is not None and not any(role in PREDEFINED_ROLES for role in roles):
        raise ValueError(
            f"User {email_address} must have at least one of the following roles: {', '.join(PREDEFINED_ROLES)}"
        )


def convert_user_object_to_dict(obj: Roster | UserOverride) -> dict[str, Any]:
    """
    Convert a Roster or UserOverride SQLAlchemy object to a dictionary, excluding
    created/updated dates.
    """
    mapper = inspect(obj.__class__)

    result = {}
    for column in mapper.columns:
        attr_name = column.key

        date_fields = ["created_datetime", "updated_datetime"]
        if any(date_field in attr_name.lower() for date_field in date_fields):
            continue

        result[attr_name] = getattr(obj, attr_name)

    return result
