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
# ============================================================================
"""Formerly defined the ORM schema objects that map directly to the database for the Case Triage
application; now only defines tables for roster management of the dashboard application as a whole.

"""

from sqlalchemy import Boolean, Column, DateTime, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeMeta, declarative_base
from sqlalchemy.sql import func

# Defines the base class for all table classes in the case triage schema.
# For actual schema definitions, see /case_triage/schema.py.
from recidiviz.persistence.database.database_entity import DatabaseEntity

CaseTriageBase: DeclarativeMeta = declarative_base(
    cls=DatabaseEntity, name="CaseTriageBase"
)


class CreatedAndUpdatedDateTimesMixin:
    created_datetime = Column(DateTime, nullable=False, server_default=func.now())
    updated_datetime = Column(
        DateTime, nullable=False, server_default=func.now(), onupdate=func.now()
    )


class Roster(CaseTriageBase, CreatedAndUpdatedDateTimesMixin):
    """Represents the list of users for each state."""

    __tablename__ = "roster"
    state_code = Column(String(255), nullable=False)
    email_address = Column(String(255), nullable=False, primary_key=True)
    external_id = Column(String(255), nullable=True)
    role = Column(String(255), nullable=False)
    district = Column(String(255), nullable=True)
    first_name = Column(String(255), nullable=True)
    last_name = Column(String(255), nullable=True)
    user_hash = Column(String(255), nullable=False)
    pseudonymized_id = Column(String(255), nullable=True)


class UserOverride(CaseTriageBase, CreatedAndUpdatedDateTimesMixin):
    """Used when a single user needs to be added, removed, or modified without uploading a new roster."""

    __tablename__ = "user_override"
    state_code = Column(String(255), nullable=False)
    email_address = Column(String(255), nullable=False, primary_key=True)
    external_id = Column(String(255), nullable=True)
    role = Column(String(255), nullable=True)
    district = Column(String(255), nullable=True)
    first_name = Column(String(255), nullable=True)
    last_name = Column(String(255), nullable=True)
    blocked = Column(Boolean, nullable=True, default=False)
    user_hash = Column(String(255), nullable=False)
    pseudonymized_id = Column(String(255), nullable=True)


class StateRolePermissions(CaseTriageBase, CreatedAndUpdatedDateTimesMixin):
    """Represents the default permissions for a given state/role combination."""

    __tablename__ = "state_role_permissions"
    state_code = Column(String(255), nullable=False, primary_key=True)
    role = Column(String(255), nullable=False, primary_key=True)
    routes = Column(JSONB(none_as_null=True), nullable=True)
    feature_variants = Column(JSONB(none_as_null=True), nullable=True)


class PermissionsOverride(CaseTriageBase, CreatedAndUpdatedDateTimesMixin):
    """Used when a specific user's permissions need to be changed from the default for their state/role."""

    __tablename__ = "permissions_override"
    email_address = Column(String(255), nullable=False, primary_key=True)
    routes = Column(JSONB(none_as_null=True), nullable=True)
    feature_variants = Column(JSONB(none_as_null=True), nullable=True)
