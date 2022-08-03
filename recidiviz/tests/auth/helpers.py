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

from typing import List

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
    routes: dict = None,
    include_hash: bool = True,
) -> DashboardUserRestrictions:
    return DashboardUserRestrictions(
        state_code=region_code,
        restricted_user_email=email,
        allowed_supervision_location_ids=allowed_supervision_location_ids,
        allowed_supervision_location_level=allowed_supervision_location_level,
        internal_role="level_1_access_role",
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
    external_id: str = None,
    district: str = None,
    first_name: str = None,
    last_name: str = None,
) -> Roster:
    return Roster(
        state_code=region_code,
        email_address=email,
        external_id=external_id,
        role=role,
        district=district,
        first_name=first_name,
        last_name=last_name,
    )


def generate_fake_default_permissions(
    state: str,
    role: str,
    can_access_leadership_dashboard: bool = None,
    can_access_case_triage: bool = None,
    should_see_beta_charts: bool = None,
    routes: dict = None,
) -> StateRolePermissions:
    return StateRolePermissions(
        state_code=state,
        role=role,
        can_access_leadership_dashboard=can_access_leadership_dashboard,
        can_access_case_triage=can_access_case_triage,
        should_see_beta_charts=should_see_beta_charts,
        routes=routes,
    )


def generate_fake_user_overrides(
    email: str,
    region_code: str,
    external_id: str = None,
    role: str = None,
    district: str = None,
    first_name: str = None,
    last_name: str = None,
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
    )


def generate_fake_permissions_overrides(
    email: str,
    can_access_leadership_dashboard: bool = None,
    can_access_case_triage: bool = None,
    should_see_beta_charts: bool = None,
    routes: dict = None,
) -> PermissionsOverride:
    return PermissionsOverride(
        user_email=email,
        can_access_leadership_dashboard=can_access_leadership_dashboard,
        can_access_case_triage=can_access_case_triage,
        should_see_beta_charts=should_see_beta_charts,
        routes=routes,
    )
