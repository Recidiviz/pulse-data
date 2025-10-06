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


class Opportunity(WorkflowsBase):
    """Base configuration data for an opportunity."""

    __tablename__ = "opportunity"

    # Manage this table via alembic
    __table_args__ = {"info": {RUN_MIGRATIONS: True}}

    state_code = Column(String, primary_key=True)
    opportunity_type = Column(String, primary_key=True)
    updated_by = Column(String, nullable=False)
    updated_at = Column(DateTime(timezone=True), nullable=False)
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
    created_at = Column(DateTime(timezone=True), nullable=False)

    # A short description of the config variant
    variant_description = Column(String, nullable=False)

    # A short description of the config revision: what was changed?
    revision_description = Column(String, nullable=False)

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
    call_to_action = Column(String, nullable=True)

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

    # Optional tooltip for the zero grants pill
    zero_grants_tooltip = Column(String, nullable=True)

    # Text for the tab of "denied" opportunities
    denied_tab_title = Column(String, nullable=True)

    # Control the sidebar option for marking someone as denied
    denial_adjective = Column(String, nullable=True)
    denial_noun = Column(String, nullable=True)

    # Does this opportunity support submitted/in-progress/pending status?
    supports_submitted = Column(Boolean, nullable=False, server_default="true")

    # Whether to include ineligible opportunities in the opportunity view
    supports_ineligible = Column(Boolean, nullable=False, server_default="false")

    # Text for the tab of submitted/in-progress/pending opportunities
    submitted_tab_title = Column(String, nullable=True)

    # Blob of text to display in tabs without people
    empty_tab_copy = Column(JSON, nullable=False, server_default="{}")

    # Blob of text to display at the top of each tab
    tab_preface_copy = Column(JSON, nullable=False, server_default="{}")

    # Options related to subcategories:
    # Mapping between internal names for subcategories and copy to display
    subcategory_headings = Column(JSON, nullable=False, server_default="{}")
    # Mapping of tabs to orders of subcategories within that tab
    subcategory_orderings = Column(JSON, nullable=False, server_default="{}")
    # Mapping of tabs to all possible subcategories of submitted that one can transition to
    mark_submitted_options_by_tab = Column(JSON, nullable=False, server_default="{}")

    # Header for Eligible and Almost Eligible criteria
    oms_criteria_header = Column(String, nullable=True)

    # Header and criteria items that are required but we don't validate
    non_oms_criteria_header = Column(String, nullable=True)
    non_oms_criteria = Column(JSON, nullable=False, server_default="{}")

    # Should we separate opportunities of this type into a highlighted component in the
    # workflows homepage?
    highlight_cases_on_homepage = Column(
        Boolean, nullable=False, server_default="false"
    )
    # Label for the highlighted component
    highlighted_case_cta_copy = Column(String, nullable=True)

    # Label for a linked overdue opportunity
    overdue_opportunity_callout_copy = Column(String, nullable=True)

    # Opportunity types that should be concurrently snoozed when an opportunity is snoozed
    snooze_companion_opportunity_types = Column(
        ARRAY(String), nullable=False, server_default="{}"
    )

    # Optional label for case notes -- should be displayed in "other relevant information" section
    case_notes_title = Column(String, nullable=True)
