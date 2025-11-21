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
"""Implements helper functions for use in Auth endpoint tests."""

import json
from datetime import datetime
from typing import Any, List, Optional, Union

from sqlalchemy import sql

from recidiviz.auth.helpers import generate_user_hash
from recidiviz.persistence.database.schema.case_triage.schema import (
    CaseTriageBase,
    PermissionsOverride,
    Roster,
    StateRolePermissions,
    UserOverride,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


def add_entity_to_database_session(
    database_key: SQLAlchemyDatabaseKey, entities: List[CaseTriageBase]
) -> None:
    with SessionFactory.using_database(database_key) as session:
        for entity in entities:
            session.add(entity)


def generate_fake_rosters(
    email: str,
    region_code: str,
    roles: list[str],
    external_id: Optional[str] = None,
    district: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    pseudonymized_id: Optional[str] = None,
    created_datetime: Optional[datetime] = None,
) -> Roster:
    return Roster(
        state_code=region_code,
        email_address=email,
        external_id=external_id,
        roles=roles,
        district=district,
        first_name=first_name,
        last_name=last_name,
        pseudonymized_id=pseudonymized_id,
        user_hash=generate_user_hash(email.lower()),
        created_datetime=created_datetime,
    )


def generate_fake_default_permissions(
    state: str,
    role: str,
    allowed_apps: Optional[dict[str, bool]] = None,
    routes: Optional[dict[str, bool]] = None,
    feature_variants: Optional[dict] = None,
    jii_permissions: Optional[dict[str, bool]] = None,
) -> StateRolePermissions:
    return StateRolePermissions(
        state_code=state,
        role=role,
        allowed_apps=allowed_apps,
        routes=routes,
        feature_variants=feature_variants,
        jii_permissions=jii_permissions,
    )


def generate_fake_user_overrides(
    *,
    email: str,
    region_code: str,
    external_id: Optional[str] = None,
    roles: Optional[list[str]] = None,
    district: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    blocked_on: Optional[datetime] = None,
    pseudonymized_id: Optional[str] = None,
    created_datetime: Optional[datetime] = None,
) -> UserOverride:
    return UserOverride(
        state_code=region_code,
        email_address=email,
        external_id=external_id,
        roles=roles,
        district=district,
        first_name=first_name,
        last_name=last_name,
        blocked_on=blocked_on,
        user_hash=generate_user_hash(email.lower()),
        pseudonymized_id=pseudonymized_id,
        created_datetime=created_datetime,
    )


def generate_fake_permissions_overrides(
    email: str,
    routes: dict[str, bool] = sql.null(),
    feature_variants: dict = sql.null(),
    allowed_apps: Optional[dict[str, bool]] = None,
    jii_permissions: Optional[dict[str, bool]] = None,
) -> PermissionsOverride:
    return PermissionsOverride(
        email_address=email,
        routes=routes,
        allowed_apps=allowed_apps,
        feature_variants=feature_variants,
        jii_permissions=jii_permissions,
    )


def convert_value_to_json(value: Any) -> Union[str, list[str]]:
    if isinstance(value, list):
        return [json.dumps(item) for item in value]

    return json.dumps(value)


def convert_list_values_to_json(
    data: list[dict[str, Any]]
) -> list[dict[str, Union[str, list[str]]]]:
    return [
        {key: convert_value_to_json(value) for key, value in row.items()}
        for row in data
    ]
