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
"""Tests for AssignmentsByTimePeriodViewBuilder"""

import datetime
from typing import Any

from freezegun import freeze_time

from recidiviz.aggregated_metrics.assignment_sessions_view_builder import (
    get_metric_assignment_sessions_materialized_table_address,
)
from recidiviz.aggregated_metrics.assignments_by_time_period_view_builder import (
    AssignmentsByTimePeriodViewBuilder,
)
from recidiviz.aggregated_metrics.metric_time_period_config import (
    MetricTimePeriodConfig,
)
from recidiviz.aggregated_metrics.models.aggregated_metric import (
    MetricTimePeriodToAssignmentJoinType,
)
from recidiviz.aggregated_metrics.models.metric_population_type import (
    MetricPopulationType,
)
from recidiviz.aggregated_metrics.models.metric_unit_of_analysis_type import (
    MetricUnitOfAnalysisType,
)
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import schema_field_for_type
from recidiviz.common.date import current_date_us_eastern
from recidiviz.observations.metric_unit_of_observation_type import (
    MetricUnitOfObservationType,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)


class TestAssignmentsByTimePeriodViewBuilder(BigQueryEmulatorTestCase):
    """Tests for AssignmentsByTimePeriodViewBuilder"""

    def setUp(self) -> None:
        super().setUp()
        # Tuesday, Nov 12, 2024 (US/Eastern time)
        with freeze_time("2024-11-12 00:00:00-05:00"):
            self.assertEqual("2024-11-12", current_date_us_eastern().isoformat())

            self.two_months_time_period_config = MetricTimePeriodConfig.month_periods(
                lookback_months=2
            )

    def _create_assignments_table(
        self,
        *,
        unit_of_analysis_type: MetricUnitOfAnalysisType,
        unit_of_observation_type: MetricUnitOfObservationType,
        population_type: MetricPopulationType,
        data: list[dict[str, Any]],
    ) -> None:
        address = get_metric_assignment_sessions_materialized_table_address(
            population_type=population_type,
            unit_of_analysis_type=unit_of_analysis_type,
            unit_of_observation_type=unit_of_observation_type,
        )
        self.create_mock_table(
            address=address,
            schema=[
                schema_field_for_type("person_id", int),
                schema_field_for_type("facility", str),
                schema_field_for_type("state_code", str),
                schema_field_for_type("assignment_date", datetime.date),
                schema_field_for_type("end_date", datetime.date),
                schema_field_for_type("end_date_exclusive", datetime.date),
                schema_field_for_type("assignment_is_first_day_in_population", bool),
            ],
        )
        self.load_rows_into_table(address=address, data=data)

    def test_simple_query_empty(self) -> None:

        builder = AssignmentsByTimePeriodViewBuilder(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            unit_of_observation_type=MetricUnitOfObservationType.PERSON_ID,
            time_period=self.two_months_time_period_config,
            metric_time_period_to_assignment_join_type=MetricTimePeriodToAssignmentJoinType.INTERSECTION_EVENT_ATTRIBUTION,
        )

        self._create_assignments_table(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            unit_of_observation_type=MetricUnitOfObservationType.PERSON_ID,
            data=[],
        )

        self.run_query_test(
            builder.build(sandbox_context=None).view_query, expected_result=[]
        )

    def test_simple_query__intersection_event_attribution(self) -> None:
        builder = AssignmentsByTimePeriodViewBuilder(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            unit_of_observation_type=MetricUnitOfObservationType.PERSON_ID,
            time_period=self.two_months_time_period_config,
            metric_time_period_to_assignment_join_type=MetricTimePeriodToAssignmentJoinType.INTERSECTION_EVENT_ATTRIBUTION,
        )

        input_assignments: list[dict[str, Any]] = [
            {
                "person_id": "1234",
                "facility": "FACILITY_1",
                "state_code": "US_XX",
                "assignment_date": "2010-01-01",
                "end_date": "2024-09-15",
                "end_date_exclusive": "2024-09-16",
                "assignment_is_first_day_in_population": True,
            },
            {
                "person_id": "4567",
                "facility": "FACILITY_2",
                "state_code": "US_XX",
                "assignment_date": "2024-09-15",
                "end_date": None,
                "end_date_exclusive": None,
                "assignment_is_first_day_in_population": False,
            },
        ]
        self._create_assignments_table(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            unit_of_observation_type=MetricUnitOfObservationType.PERSON_ID,
            data=input_assignments,
        )

        expected_result = [
            # For person_id=1234, the assignment applied to Sept 2024, but not Oct 2024
            {
                "person_id": 1234,
                "facility": "FACILITY_1",
                "state_code": "US_XX",
                "assignment_start_date": datetime.date(2010, 1, 1),
                "assignment_end_date_exclusive_nonnull": datetime.date(2024, 9, 16),
                "metric_period_start_date": datetime.date(2024, 9, 1),
                "metric_period_end_date_exclusive": datetime.date(2024, 10, 1),
                "period": "MONTH",
                "intersection_event_attribution_start_date": datetime.date(2024, 9, 1),
                "intersection_event_attribution_end_date_exclusive_nonnull": datetime.date(
                    2024, 9, 17
                ),
                "assignment_is_first_day_in_population": True,
            },
            # For person_id=4567, the assignment applied to both months
            {
                "person_id": 4567,
                "facility": "FACILITY_2",
                "state_code": "US_XX",
                "assignment_start_date": datetime.date(2024, 9, 15),
                "assignment_end_date_exclusive_nonnull": datetime.date(9999, 12, 31),
                "metric_period_start_date": datetime.date(2024, 9, 1),
                "metric_period_end_date_exclusive": datetime.date(2024, 10, 1),
                "period": "MONTH",
                "intersection_event_attribution_start_date": datetime.date(2024, 9, 16),
                "intersection_event_attribution_end_date_exclusive_nonnull": datetime.date(
                    2024, 10, 1
                ),
                "assignment_is_first_day_in_population": False,
            },
            {
                "person_id": 4567,
                "facility": "FACILITY_2",
                "state_code": "US_XX",
                "assignment_start_date": datetime.date(2024, 9, 15),
                "assignment_end_date_exclusive_nonnull": datetime.date(9999, 12, 31),
                "metric_period_start_date": datetime.date(2024, 10, 1),
                "metric_period_end_date_exclusive": datetime.date(2024, 11, 1),
                "period": "MONTH",
                "intersection_event_attribution_start_date": datetime.date(2024, 10, 1),
                "intersection_event_attribution_end_date_exclusive_nonnull": datetime.date(
                    2024, 11, 1
                ),
                "assignment_is_first_day_in_population": False,
            },
        ]
        self.run_query_test(
            builder.build(sandbox_context=None).view_query, expected_result
        )

    def test_simple_query__intersection(self) -> None:
        builder = AssignmentsByTimePeriodViewBuilder(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            unit_of_observation_type=MetricUnitOfObservationType.PERSON_ID,
            time_period=self.two_months_time_period_config,
            metric_time_period_to_assignment_join_type=MetricTimePeriodToAssignmentJoinType.INTERSECTION,
        )

        input_assignments: list[dict[str, Any]] = [
            {
                "person_id": "1234",
                "facility": "FACILITY_1",
                "state_code": "US_XX",
                "assignment_date": "2010-01-01",
                "end_date": "2024-09-15",
                "end_date_exclusive": "2024-09-16",
            },
            {
                "person_id": "4567",
                "facility": "FACILITY_2",
                "state_code": "US_XX",
                "assignment_date": "2024-09-15",
                "end_date": None,
                "end_date_exclusive": None,
            },
        ]
        self._create_assignments_table(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            unit_of_observation_type=MetricUnitOfObservationType.PERSON_ID,
            data=input_assignments,
        )

        expected_result = [
            # For person_id=1234, the assignment applied to Sept 2024, but not Oct 2024
            {
                "person_id": 1234,
                "facility": "FACILITY_1",
                "state_code": "US_XX",
                "assignment_start_date": datetime.date(2010, 1, 1),
                "assignment_end_date_exclusive_nonnull": datetime.date(2024, 9, 16),
                "metric_period_start_date": datetime.date(2024, 9, 1),
                "metric_period_end_date_exclusive": datetime.date(2024, 10, 1),
                "period": "MONTH",
                "intersection_start_date": datetime.date(2024, 9, 1),
                "intersection_end_date_exclusive_nonnull": datetime.date(2024, 9, 16),
            },
            # For person_id=4567, the assignment applied to both months
            {
                "person_id": 4567,
                "facility": "FACILITY_2",
                "state_code": "US_XX",
                "assignment_start_date": datetime.date(2024, 9, 15),
                "assignment_end_date_exclusive_nonnull": datetime.date(9999, 12, 31),
                "metric_period_start_date": datetime.date(2024, 9, 1),
                "metric_period_end_date_exclusive": datetime.date(2024, 10, 1),
                "period": "MONTH",
                "intersection_start_date": datetime.date(2024, 9, 15),
                "intersection_end_date_exclusive_nonnull": datetime.date(2024, 10, 1),
            },
            {
                "person_id": 4567,
                "facility": "FACILITY_2",
                "state_code": "US_XX",
                "assignment_start_date": datetime.date(2024, 9, 15),
                "assignment_end_date_exclusive_nonnull": datetime.date(9999, 12, 31),
                "metric_period_start_date": datetime.date(2024, 10, 1),
                "metric_period_end_date_exclusive": datetime.date(2024, 11, 1),
                "period": "MONTH",
                "intersection_start_date": datetime.date(2024, 10, 1),
                "intersection_end_date_exclusive_nonnull": datetime.date(2024, 11, 1),
            },
        ]
        self.run_query_test(
            builder.build(sandbox_context=None).view_query, expected_result
        )

    def test_view_addresses(self) -> None:
        builder = AssignmentsByTimePeriodViewBuilder(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            unit_of_observation_type=MetricUnitOfObservationType.PERSON_ID,
            time_period=self.two_months_time_period_config,
            metric_time_period_to_assignment_join_type=MetricTimePeriodToAssignmentJoinType.INTERSECTION_EVENT_ATTRIBUTION,
        )

        self.assertEqual(
            BigQueryAddress(
                dataset_id="unit_of_analysis_assignments_by_time_period",
                table_id="incarceration__person_to_facility__by_intersection_event_attribution__last_2_months",
            ),
            builder.address,
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id="unit_of_analysis_assignments_by_time_period",
                table_id="incarceration__person_to_facility__by_intersection_event_attribution__last_2_months_materialized",
            ),
            builder.materialized_address,
        )

        builder = AssignmentsByTimePeriodViewBuilder(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            unit_of_observation_type=MetricUnitOfObservationType.PERSON_ID,
            time_period=self.two_months_time_period_config,
            metric_time_period_to_assignment_join_type=MetricTimePeriodToAssignmentJoinType.INTERSECTION,
        )

        self.assertEqual(
            BigQueryAddress(
                dataset_id="unit_of_analysis_assignments_by_time_period",
                table_id="incarceration__person_to_facility__by_intersection__last_2_months",
            ),
            builder.address,
        )
        self.assertEqual(
            BigQueryAddress(
                dataset_id="unit_of_analysis_assignments_by_time_period",
                table_id="incarceration__person_to_facility__by_intersection__last_2_months_materialized",
            ),
            builder.materialized_address,
        )

    def test_view_description__intersection_event_attribution(self) -> None:
        builder = AssignmentsByTimePeriodViewBuilder(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            unit_of_observation_type=MetricUnitOfObservationType.PERSON_ID,
            time_period=self.two_months_time_period_config,
            metric_time_period_to_assignment_join_type=MetricTimePeriodToAssignmentJoinType.INTERSECTION_EVENT_ATTRIBUTION,
        )

        expected_description = """Joins a collection of metric time periods with assignment periods that associate [FACILITY] unit of
analysis to [PERSON] unit of observation assignment spans, returning one result row for every metric
time period where there is some overlap with an assignment span (treating the end date of the
assignment span as *inclusive*, not *exclusive*, and treating the start date as *exclusive* in cases
where the unit of observation was not newly entering the population). If there are multiple
assignments associated with a metric period, multiple rows will be returned.

Key column descriptions:
| Column                                                    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
|-----------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| metric_period_start_date                                  | The start date (inclusive) for the metric time period. May come before the assignment_start_date if the assignment starts in the middle of this metric period, or after if the assignment spans multiple metric periods.                                                                                                                                                                                                                                                             |
| metric_period_end_date_exclusive                          | The end date (exclusive) for the metric time period. May come after the assignment_end_date_exclusive_nonnull if the assignment ends in the middle of this metric period, or before if the assignment spans multiple metric periods.                                                                                                                                                                                                                                                 |
| assignment_start_date                                     | The start date (inclusive) of the full unit of observation to unit of analysis assignment period being associated with this metric time period.                                                                                                                                                                                                                                                                                                                                      |
| assignment_end_date_exclusive_nonnull                     | The end date (exclusive) of the full unit of observation to unit of analysis assignment period being associated with this metric time period. This field is always non-null. If the assignment is currently valid (no end date), the end date has value 9999-12-31.                                                                                                                                                                                                                  |
| intersection_event_attribution_start_date                 | This column is pre-computed for use later in aggregated metrics queries. This is the start date (inclusive) of the period of time where the assignment and metric periods overlap, with one day added past the assignment start if the assignment start date does not fall on the unit of observation's first day in the population. This is first date (inclusive) when an event observation would count towards this metric period when calculating a PeriodEventAggregatedMetric. |
| intersection_event_attribution_end_date_exclusive_nonnull | This column is pre-computed for use later in aggregated metrics queries. This is the end date (exclusive) of the period of time where the assignment and metric periods overlap, with one day added past the assignment end date (if that date still falls within the metric period). This is the day after the last date when an event observation would count towards this metric period when calculating a PeriodEventAggregatedMetric. This field is always non-null.            |
| assignment_is_first_day_in_population                     | A boolean column indicating whether `assignment_start_date` is the first day in the unit of observation's population. This is used to determine whether the assignment should be counted on the start date or the day after the start date when calculating aggregated metrics.                                                                                                                                                                                                      |
"""
        self.assertEqual(expected_description, builder.description)

    def test_view_description__intersection(self) -> None:
        builder = AssignmentsByTimePeriodViewBuilder(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            unit_of_observation_type=MetricUnitOfObservationType.PERSON_ID,
            time_period=self.two_months_time_period_config,
            metric_time_period_to_assignment_join_type=MetricTimePeriodToAssignmentJoinType.INTERSECTION,
        )

        expected_description = """Joins a collection of metric time periods with assignment periods that associate [FACILITY] unit of
analysis to [PERSON] unit of observation assignment spans, returning one result row for every metric
time period where there is some overlap with an assignment span. If there are multiple assignments
associated with a metric period, multiple rows will be returned.

Key column descriptions:
| Column                                  | Description                                                                                                                                                                                                                                                                                                                                                                           |
|-----------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| metric_period_start_date                | The start date (inclusive) for the metric time period. May come before the assignment_start_date if the assignment starts in the middle of this metric period, or after if the assignment spans multiple metric periods.                                                                                                                                                              |
| metric_period_end_date_exclusive        | The end date (exclusive) for the metric time period. May come after the assignment_end_date_exclusive_nonnull if the assignment ends in the middle of this metric period, or before if the assignment spans multiple metric periods.                                                                                                                                                  |
| assignment_start_date                   | The start date (inclusive) of the full unit of observation to unit of analysis assignment period being associated with this metric time period.                                                                                                                                                                                                                                       |
| assignment_end_date_exclusive_nonnull   | The end date (exclusive) of the full unit of observation to unit of analysis assignment period being associated with this metric time period. This field is always non-null. If the assignment is currently valid (no end date), the end date has value 9999-12-31.                                                                                                                   |
| intersection_start_date                 | This column is pre-computed for use later in aggregated metrics queries. This is the start date (inclusive) of the period of time where the assignment and metric periods overlap. This is the start date (inclusive) that should be used to determine if a span observation overlaps and should be counted when calculating a PeriodSpanAggregatedMetric.                            |
| intersection_end_date_exclusive_nonnull | This column is pre-computed for use later in aggregated metrics queries. This is the end date (exclusive) of the period of time where the assignment and metric periods overlap. This is the end date (exclusive) that should be used to determine if a span observation overlaps and should be counted when calculating a PeriodSpanAggregatedMetric. This field is always non-null. |
"""
        self.maxDiff = None
        self.assertEqual(expected_description, builder.description)

    def test_query_building__intersection_event_attribution(self) -> None:
        builder = AssignmentsByTimePeriodViewBuilder(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            unit_of_observation_type=MetricUnitOfObservationType.PERSON_ID,
            time_period=self.two_months_time_period_config,
            metric_time_period_to_assignment_join_type=MetricTimePeriodToAssignmentJoinType.INTERSECTION_EVENT_ATTRIBUTION,
        )

        expected_query = """WITH
time_periods AS (
    SELECT
        DATE_SUB(
            metric_period_end_date_exclusive, INTERVAL 1 MONTH
        ) AS metric_period_start_date,
        metric_period_end_date_exclusive,
        "MONTH" as period,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            "2024-10-01",
            "2024-11-01",
            INTERVAL 1 MONTH
        )) AS metric_period_end_date_exclusive
),
assignment_sessions AS (
    SELECT
        * EXCEPT(assignment_date, end_date_exclusive),
        assignment_date AS assignment_start_date,
        IFNULL(end_date_exclusive, "9999-12-31") AS assignment_end_date_exclusive_nonnull
    FROM
        `recidiviz-bq-emulator-project.aggregated_metrics.incarceration_facility_metrics_person_assignment_sessions_materialized`
)
SELECT
    person_id,
    state_code,
    facility,
    metric_period_start_date,
    metric_period_end_date_exclusive,
    period,
    assignment_start_date,
    assignment_end_date_exclusive_nonnull,
    GREATEST(
        IF(
            assignment_is_first_day_in_population,
            assignment_start_date,
            DATE_ADD(
                assignment_start_date,
                INTERVAL 1 DAY
            )
        ),
        metric_period_start_date
    ) AS intersection_event_attribution_start_date,
    LEAST(
        -- If an event observation occurs on the exclusive end date of an 
        -- assignment period we still want to count it. However, if the event 
        -- occurs on the exclusive end date of the metric period, we don't count
        -- it.
        metric_period_end_date_exclusive,
        IF(
            assignment_end_date_exclusive_nonnull = '9999-12-31',
            assignment_end_date_exclusive_nonnull,
            DATE_ADD(
                assignment_end_date_exclusive_nonnull,
                INTERVAL 1 DAY
            )
        )
    ) AS intersection_event_attribution_end_date_exclusive_nonnull,
    assignment_is_first_day_in_population
FROM
    time_periods
JOIN
    assignment_sessions
ON
    GREATEST(
        IF(
            assignment_is_first_day_in_population,
            assignment_start_date,
            DATE_ADD(
                assignment_start_date,
                INTERVAL 1 DAY
            )
        ),
        metric_period_start_date
    ) < LEAST(
        -- If an event observation occurs on the exclusive end date of an 
        -- assignment period we still want to count it. However, if the event 
        -- occurs on the exclusive end date of the metric period, we don't count
        -- it.
        metric_period_end_date_exclusive,
        IF(
            assignment_end_date_exclusive_nonnull = '9999-12-31',
            assignment_end_date_exclusive_nonnull,
            DATE_ADD(
                assignment_end_date_exclusive_nonnull,
                INTERVAL 1 DAY
            )
        )
    )"""
        self.assertEqual(expected_query, builder.build(sandbox_context=None).view_query)

    def test_query_building__intersection(self) -> None:
        builder = AssignmentsByTimePeriodViewBuilder(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            unit_of_observation_type=MetricUnitOfObservationType.PERSON_ID,
            time_period=self.two_months_time_period_config,
            metric_time_period_to_assignment_join_type=MetricTimePeriodToAssignmentJoinType.INTERSECTION,
        )

        expected_query = """WITH
time_periods AS (
    SELECT
        DATE_SUB(
            metric_period_end_date_exclusive, INTERVAL 1 MONTH
        ) AS metric_period_start_date,
        metric_period_end_date_exclusive,
        "MONTH" as period,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            "2024-10-01",
            "2024-11-01",
            INTERVAL 1 MONTH
        )) AS metric_period_end_date_exclusive
),
assignment_sessions AS (
    SELECT
        * EXCEPT(assignment_date, end_date_exclusive),
        assignment_date AS assignment_start_date,
        IFNULL(end_date_exclusive, "9999-12-31") AS assignment_end_date_exclusive_nonnull
    FROM
        `recidiviz-bq-emulator-project.aggregated_metrics.incarceration_facility_metrics_person_assignment_sessions_materialized`
)
SELECT
    person_id,
    state_code,
    facility,
    metric_period_start_date,
    metric_period_end_date_exclusive,
    period,
    assignment_start_date,
    assignment_end_date_exclusive_nonnull,
    GREATEST(
        assignment_start_date,
        metric_period_start_date
    ) AS intersection_start_date,
    LEAST(
        assignment_end_date_exclusive_nonnull,
        metric_period_end_date_exclusive
    ) AS intersection_end_date_exclusive_nonnull
FROM
    time_periods
JOIN
    assignment_sessions
ON
    GREATEST(
        assignment_start_date,
        metric_period_start_date
    ) < LEAST(
        assignment_end_date_exclusive_nonnull,
        metric_period_end_date_exclusive
    )"""

        self.assertEqual(expected_query, builder.build(sandbox_context=None).view_query)

    def test_query_building__assignment(self) -> None:
        builder = AssignmentsByTimePeriodViewBuilder(
            population_type=MetricPopulationType.INCARCERATION,
            unit_of_analysis_type=MetricUnitOfAnalysisType.FACILITY,
            unit_of_observation_type=MetricUnitOfObservationType.PERSON_ID,
            time_period=self.two_months_time_period_config,
            metric_time_period_to_assignment_join_type=MetricTimePeriodToAssignmentJoinType.ASSIGNMENT,
        )

        expected_query = """WITH
time_periods AS (
    SELECT
        DATE_SUB(
            metric_period_end_date_exclusive, INTERVAL 1 MONTH
        ) AS metric_period_start_date,
        metric_period_end_date_exclusive,
        "MONTH" as period,
    FROM
        UNNEST(GENERATE_DATE_ARRAY(
            "2024-10-01",
            "2024-11-01",
            INTERVAL 1 MONTH
        )) AS metric_period_end_date_exclusive
),
assignment_sessions AS (
    SELECT
        * EXCEPT(assignment_date, end_date_exclusive),
        assignment_date AS assignment_start_date,
        IFNULL(end_date_exclusive, "9999-12-31") AS assignment_end_date_exclusive_nonnull
    FROM
        `recidiviz-bq-emulator-project.aggregated_metrics.incarceration_facility_metrics_person_assignment_sessions_materialized`
)
SELECT
    person_id,
    state_code,
    facility,
    metric_period_start_date,
    metric_period_end_date_exclusive,
    period,
    assignment_start_date,
    assignment_end_date_exclusive_nonnull
FROM
    time_periods
JOIN
    assignment_sessions
ON
    metric_period_start_date <= assignment_start_date AND metric_period_end_date_exclusive > assignment_start_date"""

        self.assertEqual(expected_query, builder.build(sandbox_context=None).view_query)
