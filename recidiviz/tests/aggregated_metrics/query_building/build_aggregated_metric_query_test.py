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
    MY_AVG_DAILY_POPULATION,
    MY_AVG_LSIR_SCORE,
    MY_CONTACTS_COMPLETED_METRIC,
    MY_DRUG_SCREENS_METRIC,
    MY_LOGINS_BY_PRIMARY_WORKFLOWS,
)


class TestBuildSingleObservationTypeAggregatedMetricQueryTemplate(unittest.TestCase):
    """Tests for build_aggregated_metric_query_template()"""

    def setUp(self) -> None:
        self.maxDiff = None

    def test_build__period_event__single_unit_of_observation(self) -> None:

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
    SELECT * FROM `{project_id}.unit_of_analysis_assignments_by_time_period.supervision__person_to_officer__by_intersection_extended__last_12_months_materialized`
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
        AND observations.event_date >= person_assignments_by_time_period.intersection_start_date
        AND observations.event_date <  person_assignments_by_time_period.intersection_extended_end_date_exclusive_nonnull
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
        AND observations.event_date >= person_assignments_by_time_period.intersection_start_date
        AND observations.event_date <  person_assignments_by_time_period.intersection_extended_end_date_exclusive_nonnull
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
        )

        expected_result = """
WITH 
all_metrics__person_unit_of_observation AS (
    WITH 
    person_assignments_by_time_period AS (
        SELECT * FROM `{project_id}.unit_of_analysis_assignments_by_time_period.supervision__person_to_district__by_intersection_extended__last_12_months_materialized`
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
            AND observations.event_date >= person_assignments_by_time_period.intersection_start_date
            AND observations.event_date <  person_assignments_by_time_period.intersection_extended_end_date_exclusive_nonnull
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
            AND observations.event_date >= person_assignments_by_time_period.intersection_start_date
            AND observations.event_date <  person_assignments_by_time_period.intersection_extended_end_date_exclusive_nonnull
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
        SELECT * FROM `{project_id}.unit_of_analysis_assignments_by_time_period.supervision__workflows_primary_user_to_district__by_intersection_extended__last_12_months_materialized`
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
            AND observations.event_date >= workflows_primary_user_assignments_by_time_period.intersection_start_date
            AND observations.event_date <  workflows_primary_user_assignments_by_time_period.intersection_extended_end_date_exclusive_nonnull
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
        )
        expected_result = """
WITH 
person_assignments_by_time_period AS (
    SELECT * FROM `{project_id}.unit_of_analysis_assignments_by_time_period.incarceration__person_to_facility__by_intersection__last_12_months_materialized`
),
output_row_keys AS (
    SELECT DISTINCT state_code, facility, metric_period_start_date, metric_period_end_date_exclusive, period
    FROM person_assignments_by_time_period
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
    )
    SELECT
        person_assignments_by_time_period.state_code,
        person_assignments_by_time_period.facility,
        person_assignments_by_time_period.metric_period_start_date,
        person_assignments_by_time_period.metric_period_end_date_exclusive,
        person_assignments_by_time_period.period,
        SAFE_DIVIDE(
                SUM(
                    DATE_DIFF(
                        LEAST(metric_period_end_date_exclusive, COALESCE(LEAST(
            IFNULL(observations.end_date, "9999-12-31"),
            assignment_end_date_exclusive_nonnull
        ), DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY))),
                        GREATEST(metric_period_start_date, GREATEST(
            observations.start_date,
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
            IFNULL(observations.end_date, "9999-12-31"),
            assignment_end_date_exclusive_nonnull
        ), DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY))),
                        GREATEST(metric_period_start_date, GREATEST(
            observations.start_date,
            assignment_start_date
        )),
                        DAY
                    ) * IF((assessment_type IN ("LSIR")), 1, 0)
                )
            ) AS my_avg_lsir_score
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
    )
    SELECT
        person_assignments_by_time_period.state_code,
        person_assignments_by_time_period.facility,
        person_assignments_by_time_period.metric_period_start_date,
        person_assignments_by_time_period.metric_period_end_date_exclusive,
        person_assignments_by_time_period.period,
        SUM(
            (
                DATE_DIFF(
                    LEAST(metric_period_end_date_exclusive, COALESCE(LEAST(
            IFNULL(observations.end_date, "9999-12-31"),
            assignment_end_date_exclusive_nonnull
        ), DATE_ADD(CURRENT_DATE("US/Eastern"), INTERVAL 1 DAY))),
                    GREATEST(metric_period_start_date, GREATEST(
            observations.start_date,
            assignment_start_date
        )),
                    DAY)
                ) * (IF((TRUE), 1, 0)) / DATE_DIFF(metric_period_end_date_exclusive, metric_period_start_date, DAY)
            ) AS my_avg_daily_population
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
    GROUP BY state_code, facility, metric_period_start_date, metric_period_end_date_exclusive, period
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

    # TODO(#35897): Add tests for AssignmentEventAggregatedMetric
    # TODO(#35898): Add tests for AssignmentSpanAggregatedMetric
