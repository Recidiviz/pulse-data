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
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
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
from recidiviz.tests.aggregated_metrics.fixture_aggregated_metrics import (
    MY_ANY_INCARCERATION_365,
    MY_AVG_DAILY_POPULATION,
    MY_AVG_LSIR_SCORE,
    MY_CONTACTS_COMPLETED_METRIC,
    MY_DAYS_SUPERVISED_365,
    MY_DAYS_TO_FIRST_INCARCERATION_100,
    MY_DRUG_SCREENS_METRIC,
    MY_LOGINS_BY_PRIMARY_WORKFLOWS,
    MY_MAX_DAYS_STABLE_EMPLOYMENT_365,
    MY_TASK_COMPLETIONS,
)


class TestBuildSingleObservationTypeAggregatedMetricQueryTemplate(unittest.TestCase):
    """Tests for build_aggregated_metric_query_template()"""

    def setUp(self) -> None:
        self.maxDiff = None

    def test_build__period_event__single_unit_of_observation(self) -> None:

        result = build_aggregated_metric_query_template(
            population_type=MetricPopulationType.SUPERVISION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
            metric_class=PeriodEventAggregatedMetric,
            metrics=[
                MY_DRUG_SCREENS_METRIC,
                MY_CONTACTS_COMPLETED_METRIC,
            ],
            time_period=MetricTimePeriodConfig.month_periods(lookback_months=12),
            disaggregate_by_observation_attributes=None,
        )
        expected_result = """
WITH 
person_assignments_by_time_period AS (
    SELECT * FROM `{project_id}.unit_of_analysis_assignments_by_time_period.supervision__person_to_officer_or_previous_if_transitional__by_intersection_event_attribution__last_12_months_materialized`
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
    ),
    observations_by_assignments AS (
        SELECT
            person_assignments_by_time_period.person_id,
            person_assignments_by_time_period.state_code,
            person_assignments_by_time_period.officer_id,
            person_assignments_by_time_period.metric_period_start_date,
            person_assignments_by_time_period.metric_period_end_date_exclusive,
            person_assignments_by_time_period.period,
            person_assignments_by_time_period.assignment_start_date,
            person_assignments_by_time_period.assignment_end_date_exclusive_nonnull,
            person_assignments_by_time_period.intersection_event_attribution_start_date,
            person_assignments_by_time_period.intersection_event_attribution_end_date_exclusive_nonnull,
            person_assignments_by_time_period.assignment_is_first_day_in_population,
            observations.event_date
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
            AND observations.event_date >= person_assignments_by_time_period.intersection_event_attribution_start_date
            AND observations.event_date <  person_assignments_by_time_period.intersection_event_attribution_end_date_exclusive_nonnull
    )
    SELECT
        state_code,
        officer_id,
        metric_period_start_date,
        metric_period_end_date_exclusive,
        period,
        COUNT(DISTINCT IF(
            (TRUE),
            CONCAT(
                observations_by_assignments.person_id, observations_by_assignments.state_code, 
                observations_by_assignments.event_date
            ), NULL
        )) AS my_drug_screens
    FROM observations_by_assignments
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
    ),
    observations_by_assignments AS (
        SELECT
            person_assignments_by_time_period.person_id,
            person_assignments_by_time_period.state_code,
            person_assignments_by_time_period.officer_id,
            person_assignments_by_time_period.metric_period_start_date,
            person_assignments_by_time_period.metric_period_end_date_exclusive,
            person_assignments_by_time_period.period,
            person_assignments_by_time_period.assignment_start_date,
            person_assignments_by_time_period.assignment_end_date_exclusive_nonnull,
            person_assignments_by_time_period.intersection_event_attribution_start_date,
            person_assignments_by_time_period.intersection_event_attribution_end_date_exclusive_nonnull,
            person_assignments_by_time_period.assignment_is_first_day_in_population,
            observations.event_date,
            observations.status
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
            AND observations.event_date >= person_assignments_by_time_period.intersection_event_attribution_start_date
            AND observations.event_date <  person_assignments_by_time_period.intersection_event_attribution_end_date_exclusive_nonnull
    )
    SELECT
        state_code,
        officer_id,
        metric_period_start_date,
        metric_period_end_date_exclusive,
        period,
        COUNT(DISTINCT IF(
            (status IN ("COMPLETED")),
            CONCAT(
                observations_by_assignments.person_id, observations_by_assignments.state_code, 
                observations_by_assignments.event_date
            ), NULL
        )) AS my_contacts_completed
    FROM observations_by_assignments
    GROUP BY state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period
),
output_row_keys AS (
    SELECT DISTINCT state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period
    FROM person_assignments_by_time_period
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

    def test_build__period_event__multiple_units_of_observation(self) -> None:
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
            disaggregate_by_observation_attributes=None,
        )

        expected_result = """
WITH 
all_metrics__person_unit_of_observation AS (
    WITH 
    person_assignments_by_time_period AS (
        SELECT * FROM `{project_id}.unit_of_analysis_assignments_by_time_period.supervision__person_to_district__by_intersection_event_attribution__last_12_months_materialized`
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
        ),
        observations_by_assignments AS (
            SELECT
                person_assignments_by_time_period.person_id,
                person_assignments_by_time_period.state_code,
                person_assignments_by_time_period.district,
                person_assignments_by_time_period.metric_period_start_date,
                person_assignments_by_time_period.metric_period_end_date_exclusive,
                person_assignments_by_time_period.period,
                person_assignments_by_time_period.assignment_start_date,
                person_assignments_by_time_period.assignment_end_date_exclusive_nonnull,
                person_assignments_by_time_period.intersection_event_attribution_start_date,
                person_assignments_by_time_period.intersection_event_attribution_end_date_exclusive_nonnull,
                person_assignments_by_time_period.assignment_is_first_day_in_population,
                observations.event_date
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
                AND observations.event_date >= person_assignments_by_time_period.intersection_event_attribution_start_date
                AND observations.event_date <  person_assignments_by_time_period.intersection_event_attribution_end_date_exclusive_nonnull
        )
        SELECT
            state_code,
            district,
            metric_period_start_date,
            metric_period_end_date_exclusive,
            period,
            COUNT(DISTINCT IF(
                (TRUE),
                CONCAT(
                    observations_by_assignments.person_id, observations_by_assignments.state_code, 
                    observations_by_assignments.event_date
                ), NULL
            )) AS my_drug_screens
        FROM observations_by_assignments
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
        ),
        observations_by_assignments AS (
            SELECT
                person_assignments_by_time_period.person_id,
                person_assignments_by_time_period.state_code,
                person_assignments_by_time_period.district,
                person_assignments_by_time_period.metric_period_start_date,
                person_assignments_by_time_period.metric_period_end_date_exclusive,
                person_assignments_by_time_period.period,
                person_assignments_by_time_period.assignment_start_date,
                person_assignments_by_time_period.assignment_end_date_exclusive_nonnull,
                person_assignments_by_time_period.intersection_event_attribution_start_date,
                person_assignments_by_time_period.intersection_event_attribution_end_date_exclusive_nonnull,
                person_assignments_by_time_period.assignment_is_first_day_in_population,
                observations.event_date,
                observations.status
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
                AND observations.event_date >= person_assignments_by_time_period.intersection_event_attribution_start_date
                AND observations.event_date <  person_assignments_by_time_period.intersection_event_attribution_end_date_exclusive_nonnull
        )
        SELECT
            state_code,
            district,
            metric_period_start_date,
            metric_period_end_date_exclusive,
            period,
            COUNT(DISTINCT IF(
                (status IN ("COMPLETED")),
                CONCAT(
                    observations_by_assignments.person_id, observations_by_assignments.state_code, 
                    observations_by_assignments.event_date
                ), NULL
            )) AS my_contacts_completed
        FROM observations_by_assignments
        GROUP BY state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period
    ),
    output_row_keys AS (
        SELECT DISTINCT state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period
        FROM person_assignments_by_time_period
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
        SELECT * FROM `{project_id}.unit_of_analysis_assignments_by_time_period.supervision__workflows_primary_user_to_district__by_intersection_event_attribution__last_12_months_materialized`
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
        ),
        observations_by_assignments AS (
            SELECT
                workflows_primary_user_assignments_by_time_period.email_address,
                workflows_primary_user_assignments_by_time_period.state_code,
                workflows_primary_user_assignments_by_time_period.district,
                workflows_primary_user_assignments_by_time_period.metric_period_start_date,
                workflows_primary_user_assignments_by_time_period.metric_period_end_date_exclusive,
                workflows_primary_user_assignments_by_time_period.period,
                workflows_primary_user_assignments_by_time_period.assignment_start_date,
                workflows_primary_user_assignments_by_time_period.assignment_end_date_exclusive_nonnull,
                workflows_primary_user_assignments_by_time_period.intersection_event_attribution_start_date,
                workflows_primary_user_assignments_by_time_period.intersection_event_attribution_end_date_exclusive_nonnull,
                workflows_primary_user_assignments_by_time_period.assignment_is_first_day_in_population,
                observations.event_date
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
                AND observations.event_date >= workflows_primary_user_assignments_by_time_period.intersection_event_attribution_start_date
                AND observations.event_date <  workflows_primary_user_assignments_by_time_period.intersection_event_attribution_end_date_exclusive_nonnull
        )
        SELECT
            state_code,
            district,
            metric_period_start_date,
            metric_period_end_date_exclusive,
            period,
            COUNT(DISTINCT IF(
                (TRUE),
                CONCAT(
                    observations_by_assignments.email_address, observations_by_assignments.state_code, 
                    observations_by_assignments.event_date
                ), NULL
            )) AS my_logins_primary_workflows_user
        FROM observations_by_assignments
        GROUP BY state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period
    ),
    output_row_keys AS (
        SELECT DISTINCT state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period
        FROM workflows_primary_user_assignments_by_time_period
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

    def test_build__period_event__multiple_units_of_observation__with_observation_attributes(
        self,
    ) -> None:
        result = build_aggregated_metric_query_template(
            population_type=MetricPopulationType.SUPERVISION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
            metric_class=PeriodEventAggregatedMetric,
            metrics=[
                MY_TASK_COMPLETIONS,
                MY_LOGINS_BY_PRIMARY_WORKFLOWS,
            ],
            time_period=MetricTimePeriodConfig.month_periods(lookback_months=12),
            disaggregate_by_observation_attributes=["system_type"],
        )

        expected_result = """
WITH 
all_metrics__person_unit_of_observation AS (
    WITH 
    person_assignments_by_time_period AS (
        SELECT * FROM `{project_id}.unit_of_analysis_assignments_by_time_period.supervision__person_to_district__by_intersection_event_attribution__last_12_months_materialized`
    ),
    task_completed_metrics AS (
        WITH
        observations AS (
            SELECT
                person_id,
                state_code,
                event_date,
                system_type
            FROM 
                `{project_id}.observations__person_event.task_completed_materialized`
            WHERE
                TRUE
        ),
        observations_by_assignments AS (
            SELECT
                person_assignments_by_time_period.person_id,
                person_assignments_by_time_period.state_code,
                person_assignments_by_time_period.district,
                person_assignments_by_time_period.metric_period_start_date,
                person_assignments_by_time_period.metric_period_end_date_exclusive,
                person_assignments_by_time_period.period,
                person_assignments_by_time_period.assignment_start_date,
                person_assignments_by_time_period.assignment_end_date_exclusive_nonnull,
                person_assignments_by_time_period.intersection_event_attribution_start_date,
                person_assignments_by_time_period.intersection_event_attribution_end_date_exclusive_nonnull,
                person_assignments_by_time_period.assignment_is_first_day_in_population,
                observations.event_date,
                observations.system_type
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
                AND observations.event_date >= person_assignments_by_time_period.intersection_event_attribution_start_date
                AND observations.event_date <  person_assignments_by_time_period.intersection_event_attribution_end_date_exclusive_nonnull
        )
        SELECT
            state_code,
            district,
            metric_period_start_date,
            metric_period_end_date_exclusive,
            period,
            system_type,
            COUNT(DISTINCT IF(
                (TRUE),
                CONCAT(
                    observations_by_assignments.person_id, observations_by_assignments.state_code, 
                    observations_by_assignments.event_date
                ), NULL
            )) AS my_task_completions
        FROM observations_by_assignments
        GROUP BY state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period, system_type
    ),
    output_row_keys AS (
        SELECT DISTINCT state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period, system_type
        FROM person_assignments_by_time_period
        LEFT JOIN (
            SELECT DISTINCT
                district, state_code,
                system_type
            FROM
                task_completed_metrics
        )
        USING (district, state_code)
    )
    SELECT
        state_code,
        district,
        metric_period_start_date,
        metric_period_end_date_exclusive,
        period,
        system_type,
        IFNULL(my_task_completions, 0) AS my_task_completions
    FROM output_row_keys
    LEFT OUTER JOIN
        task_completed_metrics
    USING (state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period, system_type)
),
all_metrics__workflows_primary_user_unit_of_observation AS (
    WITH 
    workflows_primary_user_assignments_by_time_period AS (
        SELECT * FROM `{project_id}.unit_of_analysis_assignments_by_time_period.supervision__workflows_primary_user_to_district__by_intersection_event_attribution__last_12_months_materialized`
    ),
    workflows_user_login_metrics AS (
        WITH
        observations AS (
            SELECT
                email_address,
                state_code,
                event_date,
                system_type
            FROM 
                `{project_id}.observations__workflows_primary_user_event.workflows_user_login_materialized`
            WHERE
                TRUE
        ),
        observations_by_assignments AS (
            SELECT
                workflows_primary_user_assignments_by_time_period.email_address,
                workflows_primary_user_assignments_by_time_period.state_code,
                workflows_primary_user_assignments_by_time_period.district,
                workflows_primary_user_assignments_by_time_period.metric_period_start_date,
                workflows_primary_user_assignments_by_time_period.metric_period_end_date_exclusive,
                workflows_primary_user_assignments_by_time_period.period,
                workflows_primary_user_assignments_by_time_period.assignment_start_date,
                workflows_primary_user_assignments_by_time_period.assignment_end_date_exclusive_nonnull,
                workflows_primary_user_assignments_by_time_period.intersection_event_attribution_start_date,
                workflows_primary_user_assignments_by_time_period.intersection_event_attribution_end_date_exclusive_nonnull,
                workflows_primary_user_assignments_by_time_period.assignment_is_first_day_in_population,
                observations.event_date,
                observations.system_type
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
                AND observations.event_date >= workflows_primary_user_assignments_by_time_period.intersection_event_attribution_start_date
                AND observations.event_date <  workflows_primary_user_assignments_by_time_period.intersection_event_attribution_end_date_exclusive_nonnull
        )
        SELECT
            state_code,
            district,
            metric_period_start_date,
            metric_period_end_date_exclusive,
            period,
            system_type,
            COUNT(DISTINCT IF(
                (TRUE),
                CONCAT(
                    observations_by_assignments.email_address, observations_by_assignments.state_code, 
                    observations_by_assignments.event_date
                ), NULL
            )) AS my_logins_primary_workflows_user
        FROM observations_by_assignments
        GROUP BY state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period, system_type
    ),
    output_row_keys AS (
        SELECT DISTINCT state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period, system_type
        FROM workflows_primary_user_assignments_by_time_period
        LEFT JOIN (
            SELECT DISTINCT
                district, state_code,
                system_type
            FROM
                workflows_user_login_metrics
        )
        USING (district, state_code)
    )
    SELECT
        state_code,
        district,
        metric_period_start_date,
        metric_period_end_date_exclusive,
        period,
        system_type,
        IFNULL(my_logins_primary_workflows_user, 0) AS my_logins_primary_workflows_user
    FROM output_row_keys
    LEFT OUTER JOIN
        workflows_user_login_metrics
    USING (state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period, system_type)
),
output_row_keys AS (
    SELECT DISTINCT state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period, system_type
    FROM all_metrics__person_unit_of_observation
    UNION DISTINCT
    SELECT DISTINCT state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period, system_type
    FROM all_metrics__workflows_primary_user_unit_of_observation
)
SELECT
    state_code,
    district,
    metric_period_start_date AS start_date,
    metric_period_end_date_exclusive AS end_date,
    period,
    system_type,
    my_logins_primary_workflows_user,
    my_task_completions
FROM output_row_keys
LEFT OUTER JOIN
    all_metrics__person_unit_of_observation
USING (state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period, system_type)
LEFT OUTER JOIN
    all_metrics__workflows_primary_user_unit_of_observation
USING (state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period, system_type)
"""

        self.assertEqual(expected_result, result)

    def test_build__period_span__single_unit_of_observation(self) -> None:
        result = build_aggregated_metric_query_template(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            metric_class=PeriodSpanAggregatedMetric,
            metrics=[
                MY_AVG_DAILY_POPULATION,
                MY_AVG_LSIR_SCORE,
            ],
            time_period=MetricTimePeriodConfig.month_periods(lookback_months=12),
            disaggregate_by_observation_attributes=None,
        )
        expected_result = """
WITH 
person_assignments_by_time_period AS (
    SELECT * FROM `{project_id}.unit_of_analysis_assignments_by_time_period.incarceration__person_to_facility__by_intersection__last_12_months_materialized`
),
assessment_score_session_metrics AS (
    WITH
    observations AS (
        SELECT
            person_id,
            state_code,
            start_date,
            end_date,
            assessment_score,
            assessment_type
        FROM 
            `{project_id}.observations__person_span.assessment_score_session_materialized`
        WHERE
            assessment_type IN ("LSIR")
    ),
    observations_by_assignments AS (
        SELECT
            person_assignments_by_time_period.person_id,
            person_assignments_by_time_period.state_code,
            person_assignments_by_time_period.facility,
            person_assignments_by_time_period.metric_period_start_date,
            person_assignments_by_time_period.metric_period_end_date_exclusive,
            person_assignments_by_time_period.period,
            person_assignments_by_time_period.assignment_start_date,
            person_assignments_by_time_period.assignment_end_date_exclusive_nonnull,
            person_assignments_by_time_period.intersection_start_date,
            person_assignments_by_time_period.intersection_end_date_exclusive_nonnull,
            observations.start_date,
            observations.end_date,
            observations.assessment_score,
            observations.assessment_type
        FROM 
            person_assignments_by_time_period
        JOIN 
            observations
        ON
            observations.person_id = person_assignments_by_time_period.person_id
            AND observations.state_code = person_assignments_by_time_period.state_code
            AND observations.start_date <= person_assignments_by_time_period.intersection_end_date_exclusive_nonnull
            AND (
                observations.end_date IS NULL OR
                observations.end_date > person_assignments_by_time_period.intersection_start_date
            )
    )
    SELECT
        state_code,
        facility,
        metric_period_start_date,
        metric_period_end_date_exclusive,
        period,
        SAFE_DIVIDE(
                SUM(
                    DATE_DIFF(
                        LEAST(metric_period_end_date_exclusive, COALESCE(LEAST(
            IFNULL(observations_by_assignments.end_date, "9999-12-31"),
            assignment_end_date_exclusive_nonnull
        ), DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY))),
                        GREATEST(metric_period_start_date, GREATEST(
            observations_by_assignments.start_date,
            assignment_start_date
        )),
                        DAY
                    ) * IF(
                        (assessment_type IN ("LSIR")),
                        CAST(assessment_score AS FLOAT64),
                        0
                    )
                ),
                SUM(
                    DATE_DIFF(
                        LEAST(metric_period_end_date_exclusive, COALESCE(LEAST(
            IFNULL(observations_by_assignments.end_date, "9999-12-31"),
            assignment_end_date_exclusive_nonnull
        ), DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY))),
                        GREATEST(metric_period_start_date, GREATEST(
            observations_by_assignments.start_date,
            assignment_start_date
        )),
                        DAY
                    ) * IF((assessment_type IN ("LSIR")), 1, 0)
                )
            ) AS my_avg_lsir_score
    FROM observations_by_assignments
    GROUP BY state_code, facility, metric_period_start_date, metric_period_end_date_exclusive, period
),
compartment_session_metrics AS (
    WITH
    observations AS (
        SELECT
            person_id,
            state_code,
            start_date,
            end_date
        FROM 
            `{project_id}.observations__person_span.compartment_session_materialized`
        WHERE
            TRUE
    ),
    observations_by_assignments AS (
        SELECT
            person_assignments_by_time_period.person_id,
            person_assignments_by_time_period.state_code,
            person_assignments_by_time_period.facility,
            person_assignments_by_time_period.metric_period_start_date,
            person_assignments_by_time_period.metric_period_end_date_exclusive,
            person_assignments_by_time_period.period,
            person_assignments_by_time_period.assignment_start_date,
            person_assignments_by_time_period.assignment_end_date_exclusive_nonnull,
            person_assignments_by_time_period.intersection_start_date,
            person_assignments_by_time_period.intersection_end_date_exclusive_nonnull,
            observations.start_date,
            observations.end_date
        FROM 
            person_assignments_by_time_period
        JOIN 
            observations
        ON
            observations.person_id = person_assignments_by_time_period.person_id
            AND observations.state_code = person_assignments_by_time_period.state_code
            AND observations.start_date <= person_assignments_by_time_period.intersection_end_date_exclusive_nonnull
            AND (
                observations.end_date IS NULL OR
                observations.end_date > person_assignments_by_time_period.intersection_start_date
            )
    )
    SELECT
        state_code,
        facility,
        metric_period_start_date,
        metric_period_end_date_exclusive,
        period,
        SUM(
            (
                DATE_DIFF(
                    LEAST(metric_period_end_date_exclusive, COALESCE(LEAST(
            IFNULL(observations_by_assignments.end_date, "9999-12-31"),
            assignment_end_date_exclusive_nonnull
        ), DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY))),
                    GREATEST(metric_period_start_date, GREATEST(
            observations_by_assignments.start_date,
            assignment_start_date
        )),
                    DAY)
                ) * (IF((TRUE), 1, 0))
            ) / DATE_DIFF(
                    metric_period_end_date_exclusive,
                    metric_period_start_date,
                    DAY
            ) AS my_avg_daily_population
    FROM observations_by_assignments
    GROUP BY state_code, facility, metric_period_start_date, metric_period_end_date_exclusive, period
),
output_row_keys AS (
    SELECT DISTINCT state_code, facility, metric_period_start_date, metric_period_end_date_exclusive, period
    FROM person_assignments_by_time_period
)
SELECT
    state_code,
    facility,
    metric_period_start_date AS start_date,
    metric_period_end_date_exclusive AS end_date,
    period,
    IFNULL(my_avg_daily_population, 0) AS my_avg_daily_population,
    my_avg_lsir_score
FROM output_row_keys
LEFT OUTER JOIN
    assessment_score_session_metrics
USING (state_code, facility, metric_period_start_date, metric_period_end_date_exclusive, period)
LEFT OUTER JOIN
    compartment_session_metrics
USING (state_code, facility, metric_period_start_date, metric_period_end_date_exclusive, period)
"""
        self.assertEqual(expected_result, result)

    def test_build__assignment_event__single_unit_of_observation(self) -> None:
        result = build_aggregated_metric_query_template(
            population_type=MetricPopulationType.SUPERVISION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_DISTRICT,
            metric_class=AssignmentEventAggregatedMetric,
            metrics=[
                MY_ANY_INCARCERATION_365,
                MY_DAYS_TO_FIRST_INCARCERATION_100,
            ],
            time_period=MetricTimePeriodConfig.quarter_periods_rolling_monthly(
                lookback_months=12
            ),
            disaggregate_by_observation_attributes=None,
        )
        expected_result = """
WITH 
person_assignments_by_time_period AS (
    SELECT * FROM `{project_id}.unit_of_analysis_assignments_by_time_period.supervision__person_to_district__by_assignment__quarters_rolling_last_12_months_materialized`
),
incarceration_start_metrics AS (
    WITH
    observations AS (
        SELECT
            person_id,
            state_code,
            event_date,
            inflow_from_level_1
        FROM 
            `{project_id}.observations__person_event.incarceration_start_materialized`
        WHERE
            ( TRUE )
            OR ( inflow_from_level_1 IN ("SUPERVISION") )
    ),
    observations_by_assignments AS (
        SELECT
            person_assignments_by_time_period.person_id,
            person_assignments_by_time_period.state_code,
            person_assignments_by_time_period.district,
            person_assignments_by_time_period.metric_period_start_date,
            person_assignments_by_time_period.metric_period_end_date_exclusive,
            person_assignments_by_time_period.period,
            person_assignments_by_time_period.assignment_start_date,
            person_assignments_by_time_period.assignment_end_date_exclusive_nonnull,
            observations.event_date,
            observations.inflow_from_level_1,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    person_assignments_by_time_period.person_id,
                    person_assignments_by_time_period.state_code,
                    person_assignments_by_time_period.district,
                    person_assignments_by_time_period.metric_period_start_date,
                    person_assignments_by_time_period.metric_period_end_date_exclusive,
                    person_assignments_by_time_period.period,
                    person_assignments_by_time_period.assignment_start_date,
                    person_assignments_by_time_period.assignment_end_date_exclusive_nonnull,
                    -- Partition by observations filter so we only order among relevant
                    -- events.
                    event_date IS NOT NULL AND (inflow_from_level_1 IN ("SUPERVISION"))
                ORDER BY
                    event_date
            ) AS event_seq_num__my_days_to_first_incarceration_100,
            COUNTIF(event_date IS NOT NULL AND (inflow_from_level_1 IN ("SUPERVISION"))) OVER (
                PARTITION BY 
                    person_assignments_by_time_period.person_id,
                    person_assignments_by_time_period.state_code,
                    person_assignments_by_time_period.district,
                    person_assignments_by_time_period.metric_period_start_date,
                    person_assignments_by_time_period.metric_period_end_date_exclusive,
                    person_assignments_by_time_period.period,
                    person_assignments_by_time_period.assignment_start_date,
                    person_assignments_by_time_period.assignment_end_date_exclusive_nonnull
            ) AS num_matching_events__my_days_to_first_incarceration_100
        FROM 
            person_assignments_by_time_period
        LEFT OUTER JOIN 
            observations
        ON
            observations.person_id = person_assignments_by_time_period.person_id
            AND observations.state_code = person_assignments_by_time_period.state_code
            AND observations.event_date >= person_assignments_by_time_period.assignment_start_date
    )
    SELECT
        state_code,
        district,
        metric_period_start_date,
        metric_period_end_date_exclusive,
        period,
        COUNT(
            DISTINCT IF(
                (TRUE) AND observations_by_assignments.event_date <= DATE_ADD(assignment_start_date, INTERVAL 365 DAY),
                CONCAT(observations_by_assignments.person_id, observations_by_assignments.state_code, "#", assignment_start_date),
                NULL
            )
        ) AS my_any_incarceration_365,
        SUM(
            IF(
                observations_by_assignments.num_matching_events__my_days_to_first_incarceration_100 = 0 AND observations_by_assignments.event_seq_num__my_days_to_first_incarceration_100 = 1,
                -- There were no events associated with this assignment - return full 
                -- window length.
                100,
                -- Otherwise, if this is a valid first event, get the time since 
                -- assignment or window length, whichever is less
                IF(
                    observations_by_assignments.event_seq_num__my_days_to_first_incarceration_100 = 1 AND (inflow_from_level_1 IN ("SUPERVISION")),
                    DATE_DIFF(
                        LEAST(
                            observations_by_assignments.event_date, 
                            DATE_ADD(assignment_start_date, INTERVAL 100 DAY)
                        ),
                        assignment_start_date,
                        DAY
                    ),
                    0
                )
            )
        ) AS my_days_to_first_incarceration_100
    FROM observations_by_assignments
    GROUP BY state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period
),
output_row_keys AS (
    SELECT DISTINCT state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period
    FROM person_assignments_by_time_period
)
SELECT
    state_code,
    district,
    metric_period_start_date AS start_date,
    metric_period_end_date_exclusive AS end_date,
    period,
    IFNULL(my_any_incarceration_365, 0) AS my_any_incarceration_365,
    my_days_to_first_incarceration_100
FROM output_row_keys
LEFT OUTER JOIN
    incarceration_start_metrics
USING (state_code, district, metric_period_start_date, metric_period_end_date_exclusive, period)
"""
        self.assertEqual(expected_result, result)

    def test_build__assignment_span__single_unit_of_observation(self) -> None:
        result = build_aggregated_metric_query_template(
            population_type=MetricPopulationType.SUPERVISION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
            metric_class=AssignmentSpanAggregatedMetric,
            metrics=[
                MY_DAYS_SUPERVISED_365,
                MY_MAX_DAYS_STABLE_EMPLOYMENT_365,
            ],
            time_period=MetricTimePeriodConfig.quarter_periods_rolling_monthly(
                lookback_months=12
            ),
            disaggregate_by_observation_attributes=None,
        )
        expected_result = """
WITH 
person_assignments_by_time_period AS (
    SELECT * FROM `{project_id}.unit_of_analysis_assignments_by_time_period.supervision__person_to_officer_or_previous_if_transitional__by_assignment__quarters_rolling_last_12_months_materialized`
),
compartment_session_metrics AS (
    WITH
    observations AS (
        SELECT
            person_id,
            state_code,
            start_date,
            end_date,
            compartment_level_1
        FROM 
            `{project_id}.observations__person_span.compartment_session_materialized`
        WHERE
            compartment_level_1 IN ("SUPERVISION")
    ),
    observations_by_assignments AS (
        SELECT
            person_assignments_by_time_period.person_id,
            person_assignments_by_time_period.state_code,
            person_assignments_by_time_period.officer_id,
            person_assignments_by_time_period.metric_period_start_date,
            person_assignments_by_time_period.metric_period_end_date_exclusive,
            person_assignments_by_time_period.period,
            person_assignments_by_time_period.assignment_start_date,
            person_assignments_by_time_period.assignment_end_date_exclusive_nonnull,
            observations.start_date,
            observations.end_date,
            observations.compartment_level_1
        FROM 
            person_assignments_by_time_period
        LEFT OUTER JOIN 
            observations
        ON
            observations.person_id = person_assignments_by_time_period.person_id
            AND observations.state_code = person_assignments_by_time_period.state_code
            AND (
                -- Span observation overlaps with any time period after the assignment start
                observations.end_date IS NULL OR
                observations.end_date > person_assignments_by_time_period.assignment_start_date
            )
    )
    SELECT
        state_code,
        officer_id,
        metric_period_start_date,
        metric_period_end_date_exclusive,
        period,
        SUM(
            IF((compartment_level_1 IN ("SUPERVISION")), DATE_DIFF(
                LEAST(
                    DATE_ADD(assignment_start_date, INTERVAL 365 DAY),
                    COALESCE(observations_by_assignments.end_date, DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY))
                ),
                GREATEST(
                    assignment_start_date,
                    IF(observations_by_assignments.start_date <= DATE_ADD(assignment_start_date, INTERVAL 365 DAY), observations_by_assignments.start_date, NULL)
                ),
                DAY
            ), 0)
        ) AS my_days_supervised_365
    FROM observations_by_assignments
    GROUP BY state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period
),
employment_period_metrics AS (
    WITH
    observations AS (
        SELECT
            person_id,
            state_code,
            start_date,
            end_date
        FROM 
            `{project_id}.observations__person_span.employment_period_materialized`
        WHERE
            TRUE
    ),
    observations_by_assignments AS (
        SELECT
            person_assignments_by_time_period.person_id,
            person_assignments_by_time_period.state_code,
            person_assignments_by_time_period.officer_id,
            person_assignments_by_time_period.metric_period_start_date,
            person_assignments_by_time_period.metric_period_end_date_exclusive,
            person_assignments_by_time_period.period,
            person_assignments_by_time_period.assignment_start_date,
            person_assignments_by_time_period.assignment_end_date_exclusive_nonnull,
            observations.start_date,
            observations.end_date,
            ROW_NUMBER() OVER (
                PARTITION BY 
                    person_assignments_by_time_period.person_id,
                    person_assignments_by_time_period.state_code,
                    person_assignments_by_time_period.officer_id,
                    person_assignments_by_time_period.metric_period_start_date,
                    person_assignments_by_time_period.metric_period_end_date_exclusive,
                    person_assignments_by_time_period.period,
                    person_assignments_by_time_period.assignment_start_date,
                    person_assignments_by_time_period.assignment_end_date_exclusive_nonnull,
                    -- Partition by observations filter so we only order among relevant
                    -- events.
                    (TRUE)
                ORDER BY
                    GREATEST (
                        DATE_DIFF(
                            LEAST(
                                DATE_ADD(assignment_start_date, INTERVAL 365 DAY),
                                COALESCE(end_date, DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY))
                            ),
                            GREATEST(assignment_start_date, start_date),
                            DAY
                        ),
                        0
                    ) DESC
            ) = 1 AS is_max_days_overlap_in_window__my_max_days_stable_employment_365
        FROM 
            person_assignments_by_time_period
        LEFT OUTER JOIN 
            observations
        ON
            observations.person_id = person_assignments_by_time_period.person_id
            AND observations.state_code = person_assignments_by_time_period.state_code
            AND (
                -- Span observation overlaps with any time period after the assignment start
                observations.end_date IS NULL OR
                observations.end_date > person_assignments_by_time_period.assignment_start_date
            )
    )
    SELECT
        state_code,
        officer_id,
        metric_period_start_date,
        metric_period_end_date_exclusive,
        period,
        SUM(
            IF(
                (TRUE) AND is_max_days_overlap_in_window__my_max_days_stable_employment_365,
                GREATEST (
                    DATE_DIFF(
                        LEAST(
                            DATE_ADD(assignment_start_date, INTERVAL 365 DAY),
                            COALESCE(observations_by_assignments.end_date, DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY))
                        ),
                        GREATEST(assignment_start_date, observations_by_assignments.start_date),
                        DAY
                    ),
                    0
                ), 
                0
            )
        ) AS my_max_days_stable_employment_365
    FROM observations_by_assignments
    GROUP BY state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period
),
output_row_keys AS (
    SELECT DISTINCT state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period
    FROM person_assignments_by_time_period
)
SELECT
    state_code,
    officer_id,
    metric_period_start_date AS start_date,
    metric_period_end_date_exclusive AS end_date,
    period,
    IFNULL(my_days_supervised_365, 0) AS my_days_supervised_365,
    my_max_days_stable_employment_365
FROM output_row_keys
LEFT OUTER JOIN
    compartment_session_metrics
USING (state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period)
LEFT OUTER JOIN
    employment_period_metrics
USING (state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period)
"""
        self.assertEqual(expected_result, result)
