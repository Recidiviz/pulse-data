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

from typing import List, Optional

from sqlalchemy import sql

from recidiviz.auth.helpers import generate_user_hash
from recidiviz.persistence.database.schema.case_triage.schema import (
    CaseTriageBase,
    DashboardUserRestrictions,
    PermissionsOverride,
    Roster,
    StateRolePermissions,
    UserOverride,
)
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey


def generate_fake_user_restrictions(
    region_code: str,
    email: str,
    allowed_supervision_location_ids: str = "",
    allowed_supervision_location_level: str = "level_1_supervision_location",
    can_access_leadership_dashboard: bool = True,
    can_access_case_triage: bool = False,
    should_see_beta_charts: bool = False,
    routes: Optional[dict] = None,
    include_hash: bool = True,
) -> DashboardUserRestrictions:
    return DashboardUserRestrictions(
        state_code=region_code,
        restricted_user_email=email,
        allowed_supervision_location_ids=allowed_supervision_location_ids,
        allowed_supervision_location_level=allowed_supervision_location_level,
        internal_role="supervision_staff",
        can_access_leadership_dashboard=can_access_leadership_dashboard,
        can_access_case_triage=can_access_case_triage,
        should_see_beta_charts=should_see_beta_charts,
        routes=routes,
        user_hash=f"{email}::hashed" if include_hash else None,
    )


def add_entity_to_database_session(
    database_key: SQLAlchemyDatabaseKey, entities: List[CaseTriageBase]
) -> None:
    with SessionFactory.using_database(database_key) as session:
        for entity in entities:
            session.add(entity)


def generate_fake_rosters(
    email: str,
    region_code: str,
    role: str,
    external_id: Optional[str] = None,
    district: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
) -> Roster:
    return Roster(
        state_code=region_code,
        email_address=email,
        external_id=external_id,
        role=role,
        district=district,
        first_name=first_name,
        last_name=last_name,
        user_hash=generate_user_hash(email.lower()),
    )


def generate_fake_default_permissions(
    state: str,
    role: str,
    routes: Optional[dict[str, bool]] = None,
    feature_variants: Optional[dict] = None,
) -> StateRolePermissions:
    return StateRolePermissions(
        state_code=state,
        role=role,
        routes=routes,
        feature_variants=feature_variants,
    )


def generate_fake_user_overrides(
    email: str,
    region_code: str,
    external_id: Optional[str] = None,
    role: Optional[str] = None,
    district: Optional[str] = None,
    first_name: Optional[str] = None,
    last_name: Optional[str] = None,
    blocked: bool = False,
) -> UserOverride:
    return UserOverride(
        state_code=region_code,
        email_address=email,
        external_id=external_id,
        role=role,
        district=district,
        first_name=first_name,
        last_name=last_name,
        blocked=blocked,
        user_hash=generate_user_hash(email.lower()),
    )


def generate_fake_permissions_overrides(
    email: str,
    routes: dict[str, bool] = sql.null(),
    feature_variants: dict = sql.null(),
) -> PermissionsOverride:
    return PermissionsOverride(
        email_address=email,
        routes=routes,
        feature_variants=feature_variants,
    )
