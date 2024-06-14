#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  ============================================================================
"""Define the ORM schema objects for workflows"""
import enum

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Enum,
    ForeignKeyConstraint,
    Integer,
    String,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeMeta, declarative_base

from recidiviz.persistence.database.database_entity import DatabaseEntity

# Key to pass to __table_args__["info"] to keep the table up-to-date with alembic migrations.
RUN_MIGRATIONS = "run_migrations"

# Defines the base class for all table classes in the Outliers schema.
WorkflowsBase: DeclarativeMeta = declarative_base(
    cls=DatabaseEntity, name="WorkflowsBase"
)


class Opportunity(WorkflowsBase):
    """Base configuration data for an opportunity."""

    __tablename__ = "opportunity"

    # Manage this table via alembic
    __table_args__ = {"info": {RUN_MIGRATIONS: True}}

    state_code = Column(String, primary_key=True)
    opportunity_type = Column(String, primary_key=True)
    updated_by = Column(String, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    gating_feature_variant = Column(String, nullable=True)
    # TODO(#27733): Migrate fields from WorkflowsOpportunityConfig


class OpportunityStatus(enum.Enum):
    ACTIVE = 1
    INACTIVE = 2


class OpportunityConfiguration(WorkflowsBase):
    """Detailed opportunity configuration data"""

    __tablename__ = "opportunity_configuration"

    __table_args__ = (
        ForeignKeyConstraint(
            ("state_code", "opportunity_type"),
            [
                "opportunity.state_code",
                "opportunity.opportunity_type",
            ],
            name="fk_opportunity_state_code_opportunty_type",
        ),
        # Manage this table via alembic
        {"info": {RUN_MIGRATIONS: True}},
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    state_code = Column(String, nullable=False)
    opportunity_type = Column(String, nullable=False)

    # The person who created this version of the config and when
    created_by = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False)

    # A short description of the config
    description = Column(String, nullable=False)

    # ACTIVE, INACTIVE, or some other future option
    status = Column(Enum(OpportunityStatus), nullable=False)

    # An optional feature variant to gate this version of the config behind
    feature_variant = Column(String, nullable=True)

    # The name shown for the opportunity on the front end
    display_name = Column(String, nullable=False)

    # The location of the methodology document
    methodology_url = Column(String, nullable=False)

    # Is this opportunity an alert
    is_alert = Column(Boolean, nullable=False)

    # Header shown while in the null search state
    initial_header = Column(String, nullable=True)

    # Map from code to description for denial reasons
    denial_reasons = Column(JSONB, nullable=False, server_default="{}")

    # Templatized copy to show for eligibility criteria
    eligible_criteria_copy = Column(JSONB, nullable=False, server_default="{}")
    ineligible_criteria_copy = Column(JSONB, nullable=False, server_default="{}")

    # Text shown when results are found
    dynamic_eligibility_text = Column(String, nullable=False)

    # Custom text for the eligibility date
    eligibility_date_text = Column(String, nullable=True)

    # Should the denial undo option be hidden?
    hide_denial_revert = Column(Boolean, nullable=False, server_default="false")

    # Custom text for the eligibility tooltip
    tooltip_eligibility_text = Column(String, nullable=True)

    # CTA text shown on the opportunity results page
    call_to_action = Column(String, nullable=False)

    # Custom override text for the denial button
    denial_text = Column(String, nullable=True)

    # Configuration blob for the snooze feature
    snooze = Column(JSONB, nullable=True)

    # Sidebar components to display
    sidebar_components = Column(ARRAY(String), nullable=False, server_default="{}")

    # Configuration blob for eligibility tab groups
    tab_groups = Column(JSONB, nullable=True)

    # Configuration blob for person sorting
    compare_by = Column(JSONB, nullable=True)
