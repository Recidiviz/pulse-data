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
    DashboardUserRestrictions,
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
    )


def add_users_to_database_session(
    database_key: SQLAlchemyDatabaseKey, users: List[DashboardUserRestrictions]
) -> None:
    with SessionFactory.using_database(database_key) as session:
        for user in users:
            session.add(user)
