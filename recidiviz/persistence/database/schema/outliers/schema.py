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
# ============================================================================
"""Define the ORM schema objects that map directly to the database, for Outliers related entities."""

from typing import Any, Dict

from sqlalchemy import Boolean, Column, DateTime, Integer, String, text
from sqlalchemy.orm import DeclarativeMeta, declarative_base

from recidiviz.persistence.database.database_entity import DatabaseEntity

# Key to pass to __table_args__["info"] to keep the table up-to-date with alembic migrations.
RUN_MIGRATIONS = "run_migrations"

# Defines the base class for all table classes in the Outliers schema.
OutliersBase: DeclarativeMeta = declarative_base(
    cls=DatabaseEntity, name="OutliersBase"
)


class Configuration(OutliersBase):
    """Table containing Outliers information that is configured by Recidiviz users via the admin panel"""

    __tablename__ = "configurations"
    # Other tables are deleted/recreated at import time, but this table needs to be kept up to date
    # via alembic migrations.
    __table_args__ = {"info": {RUN_MIGRATIONS: True}}

    id = Column(Integer, primary_key=True, autoincrement=True)
    updated_by = Column(String, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    status = Column(String, nullable=False)
    feature_variant = Column(String)
    supervision_officer_label = Column(String, nullable=False)
    supervision_district_label = Column(String, nullable=False)
    supervision_unit_label = Column(String, nullable=False)
    supervision_supervisor_label = Column(String, nullable=False)
    supervision_district_manager_label = Column(String, nullable=False)
    supervision_jii_label = Column(String, nullable=False)
    learn_more_url = Column(String, nullable=False)
    none_are_outliers_label = Column(String, nullable=False)
    worse_than_rate_label = Column(String, nullable=False)
    exclusion_reason_description = Column(String, nullable=False)
    slightly_worse_than_rate_label = Column(String, nullable=False)
    at_or_below_rate_label = Column(String, nullable=False)
    absconders_label = Column(String, nullable=False, server_default="absconders")
    at_or_above_rate_label = Column(
        String, nullable=False, server_default="At or above statewide rate"
    )
    outliers_hover = Column(
        String,
        nullable=False,
        server_default="Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
    )
    doc_label = Column(String, nullable=False, server_default="DOC")
    # Internally used field to avoid conflicts when exporting this table's data
    # and importing it as a CSV via the Cloud SQL UI
    duplicate_write = Column(Boolean, nullable=False, server_default=text("FALSE"))
    # When adding new columns below, be sure to set a default value with the
    # server_default parameter and autogenerate a migration so that existing values
    # in the database have this column hydrated.
    # Be sure to add this column to the below:
    #     - recidiviz/admin_panel/line_staff_tools/outliers_api_schemas.py::ConfigurationSchema
    #     - recidiviz/outliers/types.py::OutliersProductConfiguration
    #     - frontends/admin-panel/src/InsightsStore/models/InsightsConfiguration.ts
    #     - frontends/admin-panel/src/components/Insights/AddConfigForm.tsx

    def to_dict(self) -> Dict[str, Any]:
        return {
            field.name: getattr(self, field.name, None) for field in self.__table__.c
        }


class UserMetadata(OutliersBase):
    """Insights-specific iformation about a user. Updated via the app itself, not ETL."""

    __tablename__ = "user_metadata"
    # Other tables are deleted/recreated at import time, but this table needs to be kept up to date
    # via alembic migrations.
    __table_args__ = {"info": {RUN_MIGRATIONS: True}}

    pseudonymized_id = Column(String, primary_key=True)
    has_seen_onboarding = Column(Boolean, default=False)
    # Internally used field to avoid conflicts when exporting this table's data
    # and importing it as a CSV via the Cloud SQL UI
    duplicate_write = Column(Boolean, nullable=False, server_default=text("FALSE"))
