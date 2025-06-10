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
"""Tests for build_single_observation_type_aggregated_metric_query.py"""
import unittest

from recidiviz.aggregated_metrics.models.aggregated_metric import (
    AssignmentEventAggregatedMetric,
    AssignmentSpanAggregatedMetric,
    PeriodEventAggregatedMetric,
    PeriodSpanAggregatedMetric,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.aggregated_metrics.query_building.build_single_observation_type_aggregated_metric_query import (
    build_single_observation_type_aggregated_metric_query_template,
)
from recidiviz.observations.event_type import EventType
from recidiviz.observations.span_type import SpanType
from recidiviz.tests.aggregated_metrics.fixture_aggregated_metrics import (
    MY_ANY_INCARCERATION_365,
    MY_AVG_DAILY_POPULATION,
    MY_AVG_DAILY_POPULATION_GENERAL_INCARCERATION,
    MY_AVG_LSIR_SCORE,
    MY_CONTACTS_ATTEMPTED_METRIC,
    MY_CONTACTS_COMPLETED_METRIC,
    MY_DAYS_AT_LIBERTY_365,
    MY_DAYS_SUPERVISED_365,
    MY_DAYS_TO_FIRST_INCARCERATION_100,
    MY_DRUG_SCREENS_METRIC,
    MY_MAX_DAYS_STABLE_EMPLOYMENT_365,
)


class TestBuildSingleObservationTypeAggregatedMetricQueryTemplate(unittest.TestCase):
    """Tests for build_single_observation_type_aggregated_metric_query_template()"""

    def test_build_period_event_metric__single(self) -> None:
        result = build_single_observation_type_aggregated_metric_query_template(
            observation_type=EventType.DRUG_SCREEN,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            metric_class=PeriodEventAggregatedMetric,
            single_observation_type_metrics=[
                MY_DRUG_SCREENS_METRIC,
            ],
            assignments_by_time_period_cte_name="person_assignments_by_time_period",
            disaggregate_by_observation_attributes=None,
        )

        expected_result = """
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
        person_assignments_by_time_period.facility,
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
    facility,
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
GROUP BY state_code, facility, metric_period_start_date, metric_period_end_date_exclusive, period
"""

        self.assertEqual(expected_result, result)

    def test_build_period_event_metric__multiple(self) -> None:
        result = build_single_observation_type_aggregated_metric_query_template(
            observation_type=EventType.SUPERVISION_CONTACT,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_UNIT,
            metric_class=PeriodEventAggregatedMetric,
            single_observation_type_metrics=[
                MY_CONTACTS_ATTEMPTED_METRIC,
                MY_CONTACTS_COMPLETED_METRIC,
            ],
            assignments_by_time_period_cte_name="person_assignments_by_time_period",
            disaggregate_by_observation_attributes=None,
        )

        expected_result = """
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
        ( status IN ("ATTEMPTED") )
        OR ( status IN ("COMPLETED") )
),
observations_by_assignments AS (
    SELECT
        person_assignments_by_time_period.person_id,
        person_assignments_by_time_period.state_code,
        person_assignments_by_time_period.unit_supervisor,
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
    unit_supervisor,
    metric_period_start_date,
    metric_period_end_date_exclusive,
    period,
    COUNT(DISTINCT IF(
        (status IN ("ATTEMPTED")),
        CONCAT(
            observations_by_assignments.person_id, observations_by_assignments.state_code, 
            observations_by_assignments.event_date
        ), NULL
    )) AS my_contacts_attempted,
    COUNT(DISTINCT IF(
        (status IN ("COMPLETED")),
        CONCAT(
            observations_by_assignments.person_id, observations_by_assignments.state_code, 
            observations_by_assignments.event_date
        ), NULL
    )) AS my_contacts_completed
FROM observations_by_assignments
GROUP BY state_code, unit_supervisor, metric_period_start_date, metric_period_end_date_exclusive, period
"""

        self.assertEqual(expected_result, result)

    def test_build_period_event_metric__overlapping_attributes_error(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Found attributes \['status'\] in `disaggregate_by_observation_attributes` "
            r"that are referenced by metric conditions for metric `my_contacts_attempted`. "
            r"If metric is already filtering on an attribute, disaggregation is unnecessary/redundant.",
        ):
            _ = build_single_observation_type_aggregated_metric_query_template(
                observation_type=EventType.SUPERVISION_CONTACT,
                unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_UNIT,
                metric_class=PeriodEventAggregatedMetric,
                single_observation_type_metrics=[
                    MY_CONTACTS_ATTEMPTED_METRIC,
                    MY_CONTACTS_COMPLETED_METRIC,
                ],
                assignments_by_time_period_cte_name="person_assignments_by_time_period",
                disaggregate_by_observation_attributes=["status"],
            )

    def test_build_period_span_metric__single(self) -> None:
        result = build_single_observation_type_aggregated_metric_query_template(
            observation_type=SpanType.ASSESSMENT_SCORE_SESSION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
            metric_class=PeriodSpanAggregatedMetric,
            single_observation_type_metrics=[
                MY_AVG_LSIR_SCORE,
            ],
            assignments_by_time_period_cte_name="person_assignments_by_time_period",
            disaggregate_by_observation_attributes=None,
        )

        expected_result = """
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
        person_assignments_by_time_period.officer_id,
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
    officer_id,
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
GROUP BY state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period
"""

        self.assertEqual(expected_result, result)

    def test_build_period_span_metric__with_disaggregate_by_observation_attributes(
        self,
    ) -> None:
        result = build_single_observation_type_aggregated_metric_query_template(
            observation_type=SpanType.COMPARTMENT_SESSION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
            metric_class=PeriodSpanAggregatedMetric,
            single_observation_type_metrics=[
                MY_AVG_DAILY_POPULATION,
            ],
            assignments_by_time_period_cte_name="person_assignments_by_time_period",
            disaggregate_by_observation_attributes=[
                "compartment_level_1",
                "compartment_level_2",
            ],
        )

        expected_result = """
WITH
observations AS (
    SELECT
        person_id,
        state_code,
        start_date,
        end_date,
        compartment_level_1,
        compartment_level_2
    FROM 
        `{project_id}.observations__person_span.compartment_session_materialized`
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
        person_assignments_by_time_period.intersection_start_date,
        person_assignments_by_time_period.intersection_end_date_exclusive_nonnull,
        observations.start_date,
        observations.end_date,
        observations.compartment_level_1,
        observations.compartment_level_2
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
    officer_id,
    metric_period_start_date,
    metric_period_end_date_exclusive,
    period,
    compartment_level_1,
    compartment_level_2,
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
GROUP BY state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period, compartment_level_1, compartment_level_2
"""

        self.maxDiff = None
        self.assertEqual(expected_result, result)

    def test_build_period_span_metric__multiple(self) -> None:
        result = build_single_observation_type_aggregated_metric_query_template(
            observation_type=SpanType.COMPARTMENT_SESSION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
            metric_class=PeriodSpanAggregatedMetric,
            single_observation_type_metrics=[
                MY_AVG_DAILY_POPULATION,
                MY_AVG_DAILY_POPULATION_GENERAL_INCARCERATION,
            ],
            assignments_by_time_period_cte_name="person_assignments_by_time_period",
            disaggregate_by_observation_attributes=None,
        )

        expected_result = """
WITH
observations AS (
    SELECT
        person_id,
        state_code,
        start_date,
        end_date,
        compartment_level_1,
        compartment_level_2
    FROM 
        `{project_id}.observations__person_span.compartment_session_materialized`
    WHERE
        ( TRUE )
        OR ( compartment_level_1 IN ("INCARCERATION") AND compartment_level_2 IN ("GENERAL") )
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
        person_assignments_by_time_period.intersection_start_date,
        person_assignments_by_time_period.intersection_end_date_exclusive_nonnull,
        observations.start_date,
        observations.end_date,
        observations.compartment_level_1,
        observations.compartment_level_2
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
    officer_id,
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
        ) AS my_avg_daily_population,
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
            ) * (IF((compartment_level_1 IN ("INCARCERATION")
    AND compartment_level_2 IN ("GENERAL")), 1, 0))
        ) / DATE_DIFF(
                metric_period_end_date_exclusive,
                metric_period_start_date,
                DAY
        ) AS my_avg_population_general_incarceration
FROM observations_by_assignments
GROUP BY state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period
"""

        self.assertEqual(expected_result, result)

    def test_build_assignment_event_metric__single(self) -> None:
        result = build_single_observation_type_aggregated_metric_query_template(
            observation_type=EventType.INCARCERATION_START,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
            metric_class=AssignmentEventAggregatedMetric,
            single_observation_type_metrics=[
                MY_ANY_INCARCERATION_365,
            ],
            assignments_by_time_period_cte_name="person_assignments_by_time_period",
            disaggregate_by_observation_attributes=None,
        )

        expected_result = """
WITH
observations AS (
    SELECT
        person_id,
        state_code,
        event_date
    FROM 
        `{project_id}.observations__person_event.incarceration_start_materialized`
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
        observations.event_date
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
    officer_id,
    metric_period_start_date,
    metric_period_end_date_exclusive,
    period,
    COUNT(
        DISTINCT IF(
            (TRUE) AND observations_by_assignments.event_date <= DATE_ADD(assignment_start_date, INTERVAL 365 DAY),
            CONCAT(observations_by_assignments.person_id, observations_by_assignments.state_code, "#", assignment_start_date),
            NULL
        )
    ) AS my_any_incarceration_365
FROM observations_by_assignments
GROUP BY state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period
"""

        self.assertEqual(expected_result, result)

    def test_build_assignment_event_metric__multiple(self) -> None:
        result = build_single_observation_type_aggregated_metric_query_template(
            observation_type=EventType.INCARCERATION_START,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
            metric_class=AssignmentEventAggregatedMetric,
            single_observation_type_metrics=[
                MY_ANY_INCARCERATION_365,
                MY_DAYS_TO_FIRST_INCARCERATION_100,
            ],
            assignments_by_time_period_cte_name="person_assignments_by_time_period",
            disaggregate_by_observation_attributes=None,
        )

        expected_result = """
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
        person_assignments_by_time_period.office,
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
                person_assignments_by_time_period.office,
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
                person_assignments_by_time_period.office,
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
    office,
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
GROUP BY state_code, district, office, metric_period_start_date, metric_period_end_date_exclusive, period
"""

        self.assertEqual(expected_result, result)

    def test_build_assignment_span_metric__single(self) -> None:
        result = build_single_observation_type_aggregated_metric_query_template(
            observation_type=SpanType.EMPLOYMENT_PERIOD,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICE,
            metric_class=AssignmentSpanAggregatedMetric,
            single_observation_type_metrics=[MY_MAX_DAYS_STABLE_EMPLOYMENT_365],
            assignments_by_time_period_cte_name="person_assignments_by_time_period",
            disaggregate_by_observation_attributes=None,
        )

        expected_result = """
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
        person_assignments_by_time_period.district,
        person_assignments_by_time_period.office,
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
                person_assignments_by_time_period.district,
                person_assignments_by_time_period.office,
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
    district,
    office,
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
GROUP BY state_code, district, office, metric_period_start_date, metric_period_end_date_exclusive, period
"""

        self.assertEqual(expected_result, result)

    def test_build_assignment_span_metric__multiple(self) -> None:
        result = build_single_observation_type_aggregated_metric_query_template(
            observation_type=SpanType.COMPARTMENT_SESSION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.SUPERVISION_OFFICER_OR_PREVIOUS_IF_TRANSITIONAL,
            metric_class=AssignmentSpanAggregatedMetric,
            single_observation_type_metrics=[
                MY_DAYS_SUPERVISED_365,
                MY_DAYS_AT_LIBERTY_365,
            ],
            assignments_by_time_period_cte_name="person_assignments_by_time_period",
            disaggregate_by_observation_attributes=None,
        )

        expected_result = """
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
        ( compartment_level_1 IN ("SUPERVISION") )
        OR ( compartment_level_1 IN ("LIBERTY") )
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
    ) AS my_days_supervised_365,
    SUM(
        IF((compartment_level_1 IN ("LIBERTY")), DATE_DIFF(
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
    ) AS my_days_at_liberty_365
FROM observations_by_assignments
GROUP BY state_code, officer_id, metric_period_start_date, metric_period_end_date_exclusive, period
"""

        self.assertEqual(expected_result, result)
