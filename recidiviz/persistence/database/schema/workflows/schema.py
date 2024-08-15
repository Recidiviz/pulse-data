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
from sqlalchemy.dialects.postgresql import ARRAY, JSON
from sqlalchemy.orm import DeclarativeMeta, declarative_base

from recidiviz.persistence.database.database_entity import DatabaseEntity

# Key to pass to __table_args__["info"] to keep the table up-to-date with alembic migrations.
RUN_MIGRATIONS = "run_migrations"

# Defines the base class for all table classes in the Outliers schema.
WorkflowsBase: DeclarativeMeta = declarative_base(
    cls=DatabaseEntity, name="WorkflowsBase"
)


class CaseNoteSearchRecord(WorkflowsBase):
    """Record case note search queries and results."""

    __tablename__ = "case_note_search_record"

    # Manage this table via alembic
    __table_args__ = {"info": {RUN_MIGRATIONS: True}}

    id = Column(Integer, primary_key=True, autoincrement=True)

    # ID of the Staff member viewing the page
    user_external_id = Column(String)

    # ID of the Client whose page we’re on
    client_external_id = Column(String)

    # The US state for which this search occurred.
    state_code = Column(String)

    # The time that the query occurred.
    timestamp = Column(DateTime(timezone=True))

    # -- Request Values --

    # The search term entered into the tool
    search_term = Column(String)

    # The page size limit used in the request.
    page_size = Column(Integer)

    # The filter conditions sent to the Discovery Engine.
    filter_conditions = Column(JSON)

    # The max number of extractive answers the engine should return.
    max_extractive_answer_count = Column(Integer)

    # The max number of snippets the engine should return.
    max_snippet_count = Column(Integer)

    # The number of summary results to return.
    summary_result_count = Column(Integer)

    # -- Result Values --

    # The ids of the case notes returned by the Discovery Engine.
    case_note_ids = Column(ARRAY(String))

    # The extractive answer returned by the Discovery Engine, if requested.
    extractive_answer = Column(String)

    # The snippet returned by the Discovery Engine, if requested.
    snippet = Column(String)

    # The summary returned by the Discovery Engine, if requested.
    summary = Column(String)


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
    homepage_position = Column(Integer, nullable=False)
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

    # Priority level of this opportunity
    priority = Column(String, nullable=False, server_default="NORMAL")

    # Header shown while in the null search state
    initial_header = Column(String, nullable=True)

    # Map from code to description for denial reasons
    denial_reasons = Column(JSON, nullable=False, server_default="{}")

    # Templatized copy to show for eligibility criteria
    eligible_criteria_copy = Column(JSON, nullable=False, server_default="{}")
    ineligible_criteria_copy = Column(JSON, nullable=False, server_default="{}")

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

    # Subheading for the new policy copy
    subheading = Column(String, nullable=True)

    # Custom override text for the denial button
    denial_text = Column(String, nullable=True)

    # Configuration blob for the snooze feature
    snooze = Column(JSON, nullable=True)

    # Sidebar components to display
    sidebar_components = Column(ARRAY(String), nullable=False, server_default="{}")

    # Configuration blob for eligibility tab groups
    tab_groups = Column(JSON, nullable=True)

    # Configuration blob for person sorting
    compare_by = Column(JSON, nullable=True)

    # Notification blob
    notifications = Column(JSON, nullable=True)

    # Hydrated in production to indicate which configuration in staging this
    # entity corresponds to
    staging_id = Column(Integer, nullable=True)
