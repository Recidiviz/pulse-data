# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Tests for build_aggregated_metric_query.py"""
import unittest

from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    EventCountMetric,
    PeriodEventAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.aggregated_metrics.query_building.build_aggregated_metric_query import (
    build_aggregated_metric_query_template,
)
from recidiviz.observations.event_selector import EventSelector
from recidiviz.observations.event_type import EventType

MY_DRUG_SCREENS_METRIC = EventCountMetric(
    name="my_drug_screens",
    display_name="My Drug Screens",
    description="Number of my drug screens",
    event_selector=EventSelector(
        event_type=EventType.DRUG_SCREEN,
        event_conditions_dict={},
    ),
)

MY_CONTACTS_COMPLETED_METRIC = EventCountMetric(
    name="my_contacts_completed",
    display_name="Contacts: Completed",
    description="Number of completed contacts",
    event_selector=EventSelector(
        event_type=EventType.SUPERVISION_CONTACT,
        event_conditions_dict={"status": ["COMPLETED"]},
    ),
)

MY_LOGINS_BY_PRIMARY_WORKFLOWS = EventCountMetric(
    name="my_logins_primary_workflows_user",
    display_name="My Logins, Primary Workflows Users",
    description="Number of logins performed by primary Workflows users",
    event_selector=EventSelector(
        event_type=EventType.WORKFLOWS_USER_LOGIN,
        event_conditions_dict={},
    ),
)


class TestBuildSingleObservationTypeAggregatedMetricQueryTemplate(unittest.TestCase):
    """Tests for build_aggregated_metric_query_template()"""

    def test_build__single_unit_of_observation(self) -> None:

        result = build_aggregated_metric_query_template(
            population_type=MetricPopulationType.SUPERVISION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER,
            metric_class=PeriodEventAggregatedMetric,
            metrics=[
                MY_DRUG_SCREENS_METRIC,
                MY_CONTACTS_COMPLETED_METRIC,
            ],
            time_period=MetricTimePeriodConfig.month_periods(lookback_months=12),
        )
        expected_result = """
WITH 
person_assignments_by_time_period AS (
    SELECT * FROM `{project_id}.unit_of_analysis_assignments_by_time_period.supervision__person_to_officer__by_period__last_12_months_materialized`
),
output_row_keys AS (
    SELECT DISTINCT state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period
    FROM person_assignments_by_time_period
),
drug_screen_metrics AS (
    WITH
    observations AS (
        SELECT
            person_id,
            state_code,
            event_date
        FROM 
            `{project_id}.observations__person_event.drug_screen_materialized`
        WHERE
            TRUE
    )
    SELECT
        person_assignments_by_time_period.state_code,
        person_assignments_by_time_period.officer_id,
        person_assignments_by_time_period.metric_period_start_date,
        person_assignments_by_time_period.metric_period_end_date_exclusive,
        person_assignments_by_time_period.period,
        COUNT(DISTINCT IF(
            (TRUE),
            CONCAT(
                observations.person_id, observations.state_code, 
                observations.event_date
            ), NULL
        )) AS my_drug_screens
    FROM 
        person_assignments_by_time_period
    JOIN 
        observations
    ON
        observations.person_id = person_assignments_by_time_period.person_id
        AND observations.state_code = person_assignments_by_time_period.state_code
        -- Include events occurring on the last date of an end-date exclusive span,
        -- but exclude events occurring on the last date of an end-date exclusive 
        -- analysis period.
        AND observations.event_date >= person_assignments_by_time_period.event_applies_to_period_start_date
        AND observations.event_date <  person_assignments_by_time_period.event_applies_to_period_end_date_exclusive_nonnull
    GROUP BY state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period
),
supervision_contact_metrics AS (
    WITH
    observations AS (
        SELECT
            person_id,
            state_code,
            event_date,
            status
        FROM 
            `{project_id}.observations__person_event.supervision_contact_materialized`
        WHERE
            status IN ("COMPLETED")
    )
    SELECT
        person_assignments_by_time_period.state_code,
        person_assignments_by_time_period.officer_id,
        person_assignments_by_time_period.metric_period_start_date,
        person_assignments_by_time_period.metric_period_end_date_exclusive,
        person_assignments_by_time_period.period,
        COUNT(DISTINCT IF(
            (status IN ("COMPLETED")),
            CONCAT(
                observations.person_id, observations.state_code, 
                observations.event_date
            ), NULL
        )) AS my_contacts_completed
    FROM 
        person_assignments_by_time_period
    JOIN 
        observations
    ON
        observations.person_id = person_assignments_by_time_period.person_id
        AND observations.state_code = person_assignments_by_time_period.state_code
        -- Include events occurring on the last date of an end-date exclusive span,
        -- but exclude events occurring on the last date of an end-date exclusive 
        -- analysis period.
        AND observations.event_date >= person_assignments_by_time_period.event_applies_to_period_start_date
        AND observations.event_date <  person_assignments_by_time_period.event_applies_to_period_end_date_exclusive_nonnull
    GROUP BY state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period
)
SELECT
    state_code,
    officer_id,
    metric_period_start_date AS start_date,
    metric_period_end_date_exclusive AS end_date,
    period,
    IFNULL(my_contacts_completed, 0) AS my_contacts_completed,
    IFNULL(my_drug_screens, 0) AS my_drug_screens
FROM output_row_keys
LEFT OUTER JOIN
    drug_screen_metrics
USING (state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period)
LEFT OUTER JOIN
    supervision_contact_metrics
USING (state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period)
"""
        self.assertEqual(expected_result, result)

    def test_build__multiple_units_of_observation(self) -> None:
        result = build_aggregated_metric_query_template(
            population_type=MetricPopulationType.SUPERVISION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
            metric_class=PeriodEventAggregatedMetric,
            metrics=[
                MY_DRUG_SCREENS_METRIC,
                MY_CONTACTS_COMPLETED_METRIC,
                MY_LOGINS_BY_PRIMARY_WORKFLOWS,
            ],
            time_period=MetricTimePeriodConfig.month_periods(lookback_months=12),
        )

        expected_result = """
WITH 
all_metrics__person_unit_of_observation AS (
    WITH 
    person_assignments_by_time_period AS (
        SELECT * FROM `{project_id}.unit_of_analysis_assignments_by_time_period.supervision__person_to_district__by_period__last_12_months_materialized`
    ),
    output_row_keys AS (
        SELECT DISTINCT state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period
        FROM person_assignments_by_time_period
    ),
    drug_screen_metrics AS (
        WITH
        observations AS (
            SELECT
                person_id,
                state_code,
                event_date
            FROM 
                `{project_id}.observations__person_event.drug_screen_materialized`
            WHERE
                TRUE
        )
        SELECT
            person_assignments_by_time_period.state_code,
            person_assignments_by_time_period.district,
            person_assignments_by_time_period.metric_period_start_date,
            person_assignments_by_time_period.metric_period_end_date_exclusive,
            person_assignments_by_time_period.period,
            COUNT(DISTINCT IF(
                (TRUE),
                CONCAT(
                    observations.person_id, observations.state_code, 
                    observations.event_date
                ), NULL
            )) AS my_drug_screens
        FROM 
            person_assignments_by_time_period
        JOIN 
            observations
        ON
            observations.person_id = person_assignments_by_time_period.person_id
            AND observations.state_code = person_assignments_by_time_period.state_code
            -- Include events occurring on the last date of an end-date exclusive span,
            -- but exclude events occurring on the last date of an end-date exclusive 
            -- analysis period.
            AND observations.event_date >= person_assignments_by_time_period.event_applies_to_period_start_date
            AND observations.event_date <  person_assignments_by_time_period.event_applies_to_period_end_date_exclusive_nonnull
        GROUP BY state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period
    ),
    supervision_contact_metrics AS (
        WITH
        observations AS (
            SELECT
                person_id,
                state_code,
                event_date,
                status
            FROM 
                `{project_id}.observations__person_event.supervision_contact_materialized`
            WHERE
                status IN ("COMPLETED")
        )
        SELECT
            person_assignments_by_time_period.state_code,
            person_assignments_by_time_period.district,
            person_assignments_by_time_period.metric_period_start_date,
            person_assignments_by_time_period.metric_period_end_date_exclusive,
            person_assignments_by_time_period.period,
            COUNT(DISTINCT IF(
                (status IN ("COMPLETED")),
                CONCAT(
                    observations.person_id, observations.state_code, 
                    observations.event_date
                ), NULL
            )) AS my_contacts_completed
        FROM 
            person_assignments_by_time_period
        JOIN 
            observations
        ON
            observations.person_id = person_assignments_by_time_period.person_id
            AND observations.state_code = person_assignments_by_time_period.state_code
            -- Include events occurring on the last date of an end-date exclusive span,
            -- but exclude events occurring on the last date of an end-date exclusive 
            -- analysis period.
            AND observations.event_date >= person_assignments_by_time_period.event_applies_to_period_start_date
            AND observations.event_date <  person_assignments_by_time_period.event_applies_to_period_end_date_exclusive_nonnull
        GROUP BY state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period
    )
    SELECT
        state_code,
        district,
        metric_period_start_date,
        metric_period_end_date_exclusive,
        period,
        IFNULL(my_contacts_completed, 0) AS my_contacts_completed,
        IFNULL(my_drug_screens, 0) AS my_drug_screens
    FROM output_row_keys
    LEFT OUTER JOIN
        drug_screen_metrics
    USING (state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period)
    LEFT OUTER JOIN
        supervision_contact_metrics
    USING (state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period)
),
all_metrics__workflows_primary_user_unit_of_observation AS (
    WITH 
    workflows_primary_user_assignments_by_time_period AS (
        SELECT * FROM `{project_id}.unit_of_analysis_assignments_by_time_period.supervision__workflows_primary_user_to_district__by_period__last_12_months_materialized`
    ),
    output_row_keys AS (
        SELECT DISTINCT state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period
        FROM workflows_primary_user_assignments_by_time_period
    ),
    workflows_user_login_metrics AS (
        WITH
        observations AS (
            SELECT
                email_address,
                state_code,
                event_date
            FROM 
                `{project_id}.observations__workflows_primary_user_event.workflows_user_login_materialized`
            WHERE
                TRUE
        )
        SELECT
            workflows_primary_user_assignments_by_time_period.state_code,
            workflows_primary_user_assignments_by_time_period.district,
            workflows_primary_user_assignments_by_time_period.metric_period_start_date,
            workflows_primary_user_assignments_by_time_period.metric_period_end_date_exclusive,
            workflows_primary_user_assignments_by_time_period.period,
            COUNT(DISTINCT IF(
                (TRUE),
                CONCAT(
                    observations.email_address, observations.state_code, 
                    observations.event_date
                ), NULL
            )) AS my_logins_primary_workflows_user
        FROM 
            workflows_primary_user_assignments_by_time_period
        JOIN 
            observations
        ON
            observations.email_address = workflows_primary_user_assignments_by_time_period.email_address
            AND observations.state_code = workflows_primary_user_assignments_by_time_period.state_code
            -- Include events occurring on the last date of an end-date exclusive span,
            -- but exclude events occurring on the last date of an end-date exclusive 
            -- analysis period.
            AND observations.event_date >= workflows_primary_user_assignments_by_time_period.event_applies_to_period_start_date
            AND observations.event_date <  workflows_primary_user_assignments_by_time_period.event_applies_to_period_end_date_exclusive_nonnull
        GROUP BY state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period
    )
    SELECT
        state_code,
        district,
        metric_period_start_date,
        metric_period_end_date_exclusive,
        period,
        IFNULL(my_logins_primary_workflows_user, 0) AS my_logins_primary_workflows_user
    FROM output_row_keys
    LEFT OUTER JOIN
        workflows_user_login_metrics
    USING (state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period)
),
output_row_keys AS (
    SELECT DISTINCT state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period
    FROM all_metrics__person_unit_of_observation
    UNION DISTINCT
    SELECT DISTINCT state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period
    FROM all_metrics__workflows_primary_user_unit_of_observation
)
SELECT
    state_code,
    district,
    metric_period_start_date AS start_date,
    metric_period_end_date_exclusive AS end_date,
    period,
    my_contacts_completed,
    my_drug_screens,
    my_logins_primary_workflows_user
FROM output_row_keys
LEFT OUTER JOIN
    all_metrics__person_unit_of_observation
USING (state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period)
LEFT OUTER JOIN
    all_metrics__workflows_primary_user_unit_of_observation
USING (state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period)
"""

        self.assertEqual(expected_result, result)

    # TODO(#35895): Add tests for PeriodSpanAggregatedMetric
    # TODO(#35897): Add tests for AssignmentEventAggregatedMetric
    # TODO(#35898): Add tests for AssignmentSpanAggregatedMetric
