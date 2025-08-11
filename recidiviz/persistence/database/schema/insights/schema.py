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
"""Define the ORM schema objects that map directly to the database, for Insights related entities."""

from typing import Any, Dict

from flask import json
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKeyConstraint,
    Identity,
    Integer,
    String,
    UniqueConstraint,
    sql,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeMeta, declarative_base

from recidiviz.persistence.database.database_entity import DatabaseEntity

# Key to pass to __table_args__["info"] to keep the table up-to-date with alembic migrations.
RUN_MIGRATIONS = "run_migrations"

# Action strategies default copy
ACTION_STRATEGIES_DEFAULT_COPY = {
    "ACTION_STRATEGY_OUTLIER": {
        "prompt": "How might I investigate what is driving this metric?",
        "body": "Try conducting case reviews and direct observations:\n1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.\n2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.\n4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.\n\nSee this and other action strategies [here](https://www.recidiviz.org).",
    },
    "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
        "prompt": "How might I discuss this with the agent in a constructive way?",
        "body": "First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.\nAfter investigating, try having a positive meeting 1:1 with the agent:\n1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.\n2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.\n3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.\n4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.\n5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.\n\nSee this and other action strategies [here](https://www.recidiviz.org).",
    },
    "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
        "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
        "body": "Try prioritizing rapport-building activities between the agent and the client:\n1. Suggest to this agent that they should prioritize:\n    - accommodating client work schedules for meetings\n    - building rapport with clients early-on\n    - building relationships with community-based providers to connect with struggling clients.\n 2. Implement unit-wide strategies to encourage client engagement, such as:\n    - early meaningful contact with all new clients\n    - clear explanations of absconding and reengagement to new clients during their orientation and beyond\n    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.\n\nSee more details on this and other action strategies [here](https://www.recidiviz.org).",
    },
    "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
        "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
        "body": "Try pairing agents up to shadow each other on a regular basis:\n1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.\n 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.\n 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.\n\nSee more details on this and other action strategies [here](https://www.recidiviz.org).",
    },
    "ACTION_STRATEGY_60_PERC_OUTLIERS": {
        "prompt": "How might I work with my team to improve these metrics?",
        "body": "Try setting positive, collective goals with your team:\n1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.\n2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.\n3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.\n4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.\n5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.\n\nSee more details on this and other action strategies [here](https://www.recidiviz.org).",
    },
}

# Defines the base class for all table classes in the Outliers schema.
InsightsBase: DeclarativeMeta = declarative_base(
    cls=DatabaseEntity, name="InsightsBase"
)


class PersonBase:
    """Base class that includes attributes that all person entities must define"""

    state_code = Column(String, primary_key=True)
    external_id = Column(String, primary_key=True)
    # Unique identifier generated by the Recidiviz system
    staff_id = Column(BigInteger, nullable=False)
    # Should follow the Recidiviz-standard JSON struct string representation
    full_name = Column(JSONB(none_as_null=True), nullable=True)
    pseudonymized_id = Column(String, nullable=False)


class SupervisionOfficer(PersonBase, InsightsBase):
    """ETL data imported from `recidiviz.calculator.query.state.views.outliers.supervision_officers`"""

    __tablename__ = "supervision_officers"

    email = Column(String, nullable=True)
    supervisor_external_id = Column(String, nullable=True)
    # List of supervisors for this officer
    supervisor_external_ids = Column(ARRAY(String), nullable=True)
    # Id of the supervision district the officer is assigned to
    supervision_district = Column(String, nullable=True)
    # specialized caseload type, if applicable
    specialized_caseload_type = Column(String, nullable=True)
    # earliest date that this officer was assigned a caseload
    earliest_person_assignment_date = Column(Date, nullable=True)
    # latest date that this officer logged in
    latest_login_date = Column(Date, nullable=True)


class SupervisionOfficerSupervisor(PersonBase, InsightsBase):
    """ETL data imported from `recidiviz.calculator.query.state.views.outliers.supervision_officer_supervisors`"""

    __tablename__ = "supervision_officer_supervisors"

    # Supervision location to show on the supervisor list page
    supervision_location_for_list_page = Column(String, nullable=True)
    # Supervision location to show on the individual supervisor page
    supervision_location_for_supervisor_page = Column(String, nullable=True)
    email = Column(String, nullable=True)


class SupervisionDistrictManager(InsightsBase):
    """ETL data imported from `recidiviz.calculator.query.state.views.outliers.supervision_district_managers`"""

    __tablename__ = "supervision_district_managers"

    state_code = Column(String, primary_key=True)
    external_id = Column(String, primary_key=True)
    full_name = Column(JSONB(none_as_null=True), nullable=True)
    email = Column(String, nullable=True)
    # Id of the supervision district the manager is assigned to
    supervision_district = Column(String, nullable=False)


class MetricBase:
    """Base class that includes attributes that all metrics must define"""

    state_code = Column(String, primary_key=True)
    # The name of the metric, which corresponds to a column in the supervision_x_aggregated_metrics_materialized table
    metric_id = Column(String, primary_key=True)
    # The value of the given metric
    metric_value = Column(Float)
    # The end date for the period
    end_date = Column(Date, primary_key=True)
    # The period that this metric applies to (see recidiviz.aggregated_metrics.metric_time_period_config.py)
    period = Column(
        String,
        primary_key=True,
        nullable=False,
    )
    # The type of the value, e.g. "RATE" or "COUNT". (see recidiviz.outliers.types.OutliersMetricValueType)
    value_type = Column(String, primary_key=True)


class SupervisionOfficerMetric(MetricBase, InsightsBase):
    """ETL data imported from `recidiviz.calculator.query.state.views.outliers.supervision_officer_metrics`"""

    __tablename__ = "supervision_officer_metrics"

    # The id of the officer the metric is measured for
    officer_id = Column(String, primary_key=True)
    # Category this caseload type is part of, e.g. SEX_OFFENSE_BINARY. 'ALL' indicates the metric
    # benchmark is statewide.
    category_type = Column(String, primary_key=True)
    # Caseload category of the officer within category_type during the period during which this metric was measured
    caseload_category = Column(String)

    __tableargs__ = (
        ForeignKeyConstraint(
            ["state_code", "officer_id"],
            [
                "supervision_officers.state_code",
                "supervision_officers.external_id",
            ],
        ),
    )


class MetricBenchmark(InsightsBase):
    """ETL data imported from `recidiviz.calculator.query.state.views.outliers.metric_benchmarks`"""

    __tablename__ = "metric_benchmarks"

    state_code = Column(String, primary_key=True)
    # The name of the metric, which corresponds to a column in the supervision_x_aggregated_metrics_materialized table
    metric_id = Column(String, primary_key=True)
    # The period that this metric applies to (see recidiviz.aggregated_metrics.metric_time_period_config.py)
    period = Column(
        String,
        primary_key=True,
        nullable=False,
    )
    # The end date for the period
    end_date = Column(Date, primary_key=True)
    # The target rate for the given metric
    target = Column(Float, nullable=False)
    # The threshold for the given metric, specifically the IQR
    threshold = Column(Float, nullable=False)
    # Caseload category, if applicable. 'ALL' indicates the metric benchmark is statewide.
    caseload_category = Column(String, primary_key=True)
    # Category this caseload type is part of, e.g. SEX_OFFENSE_BINARY. 'ALL' indicates the metric
    # benchmark is statewide.
    category_type = Column(String, primary_key=True)
    # If it exists, the top x% of officers to highlight for this metric
    top_x_pct = Column(Integer)
    # The percentile value to compare an individual rate to and determine if they are in the top_x_pct
    top_x_pct_percentile_value = Column(Float)


class SupervisionOfficerOutlierStatus(InsightsBase):
    """ETL data imported from `recidiviz.calculator.query.state.views.outliers.supervision_officer_outlier_status`"""

    __tablename__ = "supervision_officer_outlier_status"

    state_code = Column(String, primary_key=True)
    # The id of the officer the metric is measured for
    officer_id = Column(String, primary_key=True)
    # Caseload category within the category_type. 'ALL' value indicates the status is compared to a statewide metric.
    caseload_category = Column(String, primary_key=True)
    # Category this caseload type is part of, e.g. SEX_OFFENSE_BINARY. 'ALL' indicates the status
    # is compared to a statewide metric.
    category_type = Column(String, primary_key=True)
    # The name of the metric, which corresponds to a column in the supervision_x_aggregated_metrics_materialized table
    metric_id = Column(String, primary_key=True)
    # The period that this metric applies to (see recidiviz.aggregated_metrics.metric_time_period_config.py)
    period = Column(
        String,
        primary_key=True,
        nullable=False,
    )
    # The end date for the period
    end_date = Column(Date, primary_key=True)
    # The rate of the given metric
    metric_rate = Column(Float, nullable=False)
    # The target rate for the given metric
    target = Column(Float, nullable=False)
    # The threshold for the given metric, specifically the IQR
    threshold = Column(Float, nullable=False)
    # Whether the officer is either FAR, MET or NEAR the metric's target
    status = Column(String, nullable=False)
    # If it exists, highlight the top x% of officers for this metric
    top_x_pct = Column(Integer)
    # The percentile value to compare an individual rate to and determine if they are in the top_x_pct
    top_x_pct_percentile_value = Column(Float)
    # Whether the officer's metric rate is in the top x% of individuals for this metric
    is_top_x_pct = Column(Boolean)


class SupervisionClientEvent(InsightsBase):
    """ETL data imported from `recidiviz.calculator.query.state.views.outliers.supervision_client_events`"""

    __tablename__ = "supervision_client_events"

    # An autoincrementing primary key to allow duplicate rows.
    _id = Column(Integer, Identity(), primary_key=True)
    state_code = Column(String)
    # The metric id that this event applies to
    metric_id = Column(String)
    # The date the event occurred
    event_date = Column(Date)
    # The id of the JII the event applies to
    client_id = Column(String)
    # Should follow the Recidiviz-standard JSON struct string representation
    client_name = Column(JSONB(none_as_null=True), nullable=True)
    # The external id of the officer assigned to this person at the time the event occurred
    officer_id = Column(String, nullable=True)
    # The start date of the officer assignment period that this event occurred in
    officer_assignment_date = Column(Date, nullable=True)
    # The end date of the officer assignment that this event occurred in
    officer_assignment_end_date = Column(Date, nullable=True)
    # The start date of the supervision period that this event occurred in
    supervision_start_date = Column(Date, nullable=True)
    # The end date of the supervision period that this event occurred in
    supervision_end_date = Column(Date, nullable=True)
    # The supervision type at the time of the event
    supervision_type = Column(String, nullable=True)
    # JSON object of information related to this event
    attributes = Column(JSONB(none_as_null=True), nullable=True)
    pseudonymized_client_id = Column(String, nullable=False)
    pseudonymized_officer_id = Column(String, nullable=True)


class SupervisionClients(InsightsBase):
    """ETL data imported from `recidiviz.calculator.query.state.views.outliers.supervision_clients`"""

    __tablename__ = "supervision_clients"

    state_code = Column(String, primary_key=True)
    client_id = Column(String, primary_key=True)
    pseudonymized_client_id = Column(String, nullable=False)
    client_name = Column(JSONB(none_as_null=True), nullable=True)
    birthdate = Column(Date, nullable=True)
    gender = Column(String, nullable=True)
    race_or_ethnicity = Column(String, nullable=True)


class Configuration(InsightsBase):
    """Table containing Outliers information that is configured by Recidiviz users via the admin panel"""

    __tablename__ = "configurations"
    # Other tables are deleted/recreated at import time, but this table needs to be kept up to date
    # via alembic migrations.
    __table_args__ = {"info": {RUN_MIGRATIONS: True}}

    id = Column(Integer, Identity(), primary_key=True)
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
    # When adding new columns below, be sure to set a default value with the
    # server_default parameter and autogenerate a migration so that existing values
    # in the database have this column hydrated.
    supervisor_has_no_outlier_officers_label = Column(
        String, nullable=False, server_default="Supervisor has no outlier officers"
    )
    officer_has_no_outlier_metrics_label = Column(
        String, nullable=False, server_default="Officer has no outlier metrics"
    )
    supervisor_has_no_officers_with_eligible_clients_label = Column(
        String,
        nullable=False,
        server_default="Supervisor has no officers with eligible clients",
    )
    officer_has_no_eligible_clients_label = Column(
        String, nullable=False, server_default="Officer has no eligible clients"
    )
    absconders_label = Column(String, nullable=False, server_default="absconders")
    at_or_above_rate_label = Column(
        String, nullable=False, server_default="At or above statewide rate"
    )
    outliers_hover = Column(
        String,
        nullable=False,
        server_default="Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
    )
    # Be sure to add this column to the below:
    #     - recidiviz/admin_panel/line_staff_tools/outliers_api_schemas.py::ConfigurationSchema
    #     - recidiviz/outliers/types.py::OutliersProductConfiguration
    #     - frontends/admin-panel/src/InsightsStore/models/InsightsConfiguration.ts
    #     - frontends/admin-panel/src/components/Insights/AddConfigForm.tsx

    action_strategy_copy = Column(
        JSONB,
        nullable=False,
        server_default=json.dumps(ACTION_STRATEGIES_DEFAULT_COPY),
    )

    vitals_metrics_methodology_url = Column(
        String, nullable=False, server_default="https://www.recidiviz.org/"
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            field.name: getattr(self, field.name, None) for field in self.__table__.c
        }


class UserMetadata(InsightsBase):
    """Insights-specific iformation about a user. Updated via the app itself, not ETL."""

    __tablename__ = "user_metadata"
    # Other tables are deleted/recreated at import time, but this table needs to be kept up to date
    # via alembic migrations.
    __table_args__ = {"info": {RUN_MIGRATIONS: True}}

    pseudonymized_id = Column(String, primary_key=True)
    has_seen_onboarding = Column(Boolean, nullable=False, server_default=sql.false())
    has_dismissed_data_unavailable_note = Column(
        Boolean, nullable=False, server_default=sql.false()
    )
    has_dismissed_rate_over_100_percent_note = Column(
        Boolean, nullable=False, server_default=sql.false()
    )


class ActionStrategySurfacedEvents(InsightsBase):
    """Table containing data about which action strategies have been surfaced to a given user"""

    __tablename__ = "action_strategy_surfaced_events"
    # Other tables are deleted/recreated at import time, but this table needs to be kept up to date
    # via alembic migrations.

    _id = Column(Integer, Identity(), primary_key=True)
    state_code = Column(String)
    user_pseudonymized_id = Column(
        String,
        nullable=False,
    )
    officer_pseudonymized_id = Column(
        String,
        nullable=True,
    )
    action_strategy = Column(
        String,
        nullable=False,
    )
    timestamp = Column(Date, nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "user_pseudonymized_id",
            "officer_pseudonymized_id",
            "action_strategy",
            "timestamp",
            name="action_strategy_surfaced_events_uniq_constraint",
        ),
        {"info": {RUN_MIGRATIONS: True}},
    )
