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
"""Tests for the SingleTaskEligibilitySpansBigQueryViewBuilder."""
from datetime import date

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.almost_eligible_spans_big_query_view_builder import (
    AlmostEligibleSpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.basic_single_task_eligibility_spans_big_query_view_builder import (
    BasicSingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.criteria_condition import (
    NotEligibleCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateSpecificTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.task_eligibility.task_eligibility_big_query_emulator_utils import (
    TEST_COMPLETION_EVENT_BUILDER,
    TEST_POPULATION_BUILDER,
    load_data_for_almost_eligible_span_view,
    load_data_for_basic_task_eligibility_span_view,
    load_data_for_candidate_population_view,
    load_data_for_task_completion_event_view,
    load_data_for_task_criteria_view,
    load_empty_task_completion_event_view,
)

TEST_CRITERIA_BUILDER_1 = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name="SIMPLE_CRITERIA",
    criteria_spans_query_template="SELECT * FROM `{project_id}.test_dataset.foo`;",
    description="Simple criteria description",
    reasons_fields=[
        ReasonsField(
            name="test_reason",
            type=bigquery.StandardSqlTypeNames.STRING,
            description="Simple reason description",
        ),
    ],
)

TEST_CRITERIA_BUILDER_2 = StateAgnosticTaskCriteriaBigQueryViewBuilder(
    criteria_name="SIMPLE_CRITERIA_2",
    criteria_spans_query_template="SELECT * FROM `{project_id}.test_dataset.state_foo`;",
    description="Simple specific criteria description",
    reasons_fields=[
        ReasonsField(
            name="test_reason_int",
            type=bigquery.StandardSqlTypeNames.INT64,
            description="Simple reason description",
        )
    ],
)

TEST_CRITERIA_BUILDER_3 = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_XX,
    criteria_name="US_XX_SIMPLE_CRITERIA",
    criteria_spans_query_template="SELECT * FROM `{project_id}.test_dataset.state_foo`;",
    description="Simple state specific criteria description",
    reasons_fields=[
        ReasonsField(
            name="test_reason_date",
            type=bigquery.StandardSqlTypeNames.DATE,
            description="Simple reason description",
        )
    ],
)

TEST_CRITERIA_BUILDER_4 = StateSpecificTaskCriteriaBigQueryViewBuilder(
    state_code=StateCode.US_XX,
    criteria_name="US_XX_SIMPLE_CRITERIA_2",
    criteria_spans_query_template="SELECT * FROM `{project_id}.test_dataset.state_foo`;",
    description="Other simple state specific criteria description",
    reasons_fields=[
        ReasonsField(
            name="test_reason_date",
            type=bigquery.StandardSqlTypeNames.DATE,
            description="Simple reason description",
        )
    ],
)

TES_QUERY_BUILDER = SingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_XX,
    task_name="my_task_name",
    description="my_task_description",
    candidate_population_view_builder=TEST_POPULATION_BUILDER,
    criteria_spans_view_builders=[
        TEST_CRITERIA_BUILDER_1,
    ],
    completion_event_builder=TEST_COMPLETION_EVENT_BUILDER,
)


class TestSingleTaskEligibilitySpansBigQueryViewBuilder(BigQueryEmulatorTestCase):
    """Tests for the SingleTaskEligibilitySpansBigQueryViewBuilder."""

    def test_materialized_table_for_task_name(self) -> None:
        self.assertEqual(
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_xx",
                table_id="my_task_name_materialized",
            ),
            SingleTaskEligibilitySpansBigQueryViewBuilder.materialized_table_for_task_name(
                task_name=TES_QUERY_BUILDER.task_name,
                state_code=TES_QUERY_BUILDER.state_code,
            ),
        )

        self.assertEqual(
            TES_QUERY_BUILDER.materialized_address,
            SingleTaskEligibilitySpansBigQueryViewBuilder.materialized_table_for_task_name(
                task_name=TES_QUERY_BUILDER.task_name,
                state_code=TES_QUERY_BUILDER.state_code,
            ),
        )

    def test_mismatched_state_codes(self) -> None:
        """Verify an error is raised if the task state code does not match the completion event"""

        us_yy_completion_event = StateSpecificTaskCompletionEventBigQueryViewBuilder(
            state_code=StateCode.US_YY,
            completion_event_type=TaskCompletionEventType.EARLY_DISCHARGE,
            completion_event_query_template="SELECT * FROM `{project_id}.test_dataset.foo`;",
            description="Simple event description",
        )
        with self.assertRaises(ValueError):
            SingleTaskEligibilitySpansBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                task_name="my_task_name",
                description="my_task_description",
                candidate_population_view_builder=TEST_POPULATION_BUILDER,
                criteria_spans_view_builders=[
                    TEST_CRITERIA_BUILDER_1,
                ],
                completion_event_builder=us_yy_completion_event,
            )

    def test_simple_tes_query(self) -> None:
        """
        Verify that the Task Eligibility Spans view can properly handle a client that goes from ineligible to eligible
        for a single criteria
        """
        load_data_for_task_criteria_view(
            emulator=self,
            criteria_view_builder=TEST_CRITERIA_BUILDER_1,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 6),
                    "meets_criteria": False,
                    "reason": {"test_reason": "reason_text"},
                    "reason_v2": {"test_reason": "reason_text"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 3, 18),
                    "meets_criteria": False,
                    "reason": {"test_reason": "reason_change"},
                    "reason_v2": {"test_reason": "reason_change"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 18),
                    "end_date": date(2024, 4, 8),
                    "meets_criteria": True,
                    "reason": {"test_reason": "eligible_reason"},
                    "reason_v2": {"test_reason": "eligible_reason"},
                },
            ],
        )
        load_data_for_candidate_population_view(
            emulator=self, population_date_spans=[(date(2024, 1, 1), date(2024, 4, 8))]
        )
        load_data_for_task_completion_event_view(
            emulator=self,
            completion_event_view_builder=TEST_COMPLETION_EVENT_BUILDER,
            task_completion_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "completion_event_date": date(2024, 4, 8),
                },
            ],
        )

        self.run_query_test(
            query_str=TES_QUERY_BUILDER.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 1,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 6),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        }
                    ],
                    "ineligible_criteria": [TEST_CRITERIA_BUILDER_1.criteria_name],
                    "end_reason": "INELIGIBLE_REASONS_CHANGED",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 2,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 3, 18),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_change"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_change"},
                        }
                    ],
                    "ineligible_criteria": [TEST_CRITERIA_BUILDER_1.criteria_name],
                    "end_reason": "BECAME_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 3,
                    "start_date": date(2024, 3, 18),
                    "end_date": date(2024, 4, 8),
                    "is_eligible": True,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        }
                    ],
                    "ineligible_criteria": [],
                    "end_reason": "TASK_COMPLETED",
                },
            ],
        )

    def test_tes_query_no_task_completion_event(self) -> None:
        """Test that the TES spans end with 'LEFT_CANDIDATE_POPULATION' when there is no completion event"""

        load_data_for_task_criteria_view(
            emulator=self,
            criteria_view_builder=TEST_CRITERIA_BUILDER_1,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 6),
                    "meets_criteria": False,
                    "reason": {"test_reason": "reason_text"},
                    "reason_v2": {"test_reason": "reason_text"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 3, 18),
                    "meets_criteria": False,
                    "reason": {"test_reason": "reason_change"},
                    "reason_v2": {"test_reason": "reason_change"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 18),
                    "end_date": date(2024, 3, 28),
                    "meets_criteria": True,
                    "reason": {"test_reason": "eligible_reason"},
                    "reason_v2": {"test_reason": "eligible_reason"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 28),
                    "end_date": date(2024, 4, 4),
                    "meets_criteria": True,
                    "reason": {"test_reason": "new_eligible_reason"},
                    "reason_v2": {"test_reason": "new_eligible_reason"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 4),
                    "end_date": date(2024, 4, 8),
                    "meets_criteria": False,
                    "reason": {"test_reason": "new_eligible_reason"},
                    "reason_v2": {"test_reason": "new_eligible_reason"},
                },
            ],
        )
        load_data_for_candidate_population_view(
            emulator=self,
            population_date_spans=[
                (date(2024, 1, 1), date(2024, 4, 8)),
            ],
        )
        load_empty_task_completion_event_view(emulator=self)
        self.run_query_test(
            query_str=TES_QUERY_BUILDER.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 1,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 6),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        }
                    ],
                    "ineligible_criteria": [TEST_CRITERIA_BUILDER_1.criteria_name],
                    "end_reason": "INELIGIBLE_REASONS_CHANGED",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 2,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 3, 18),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_change"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_change"},
                        }
                    ],
                    "ineligible_criteria": [TEST_CRITERIA_BUILDER_1.criteria_name],
                    "end_reason": "BECAME_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 3,
                    "start_date": date(2024, 3, 18),
                    "end_date": date(2024, 3, 28),
                    "is_eligible": True,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        }
                    ],
                    "ineligible_criteria": [],
                    "end_reason": "ELIGIBLE_REASONS_CHANGED",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 4,
                    "start_date": date(2024, 3, 28),
                    "end_date": date(2024, 4, 4),
                    "is_eligible": True,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "new_eligible_reason"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "new_eligible_reason"},
                        }
                    ],
                    "ineligible_criteria": [],
                    "end_reason": "BECAME_INELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 5,
                    "start_date": date(2024, 4, 4),
                    "end_date": date(2024, 4, 8),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "new_eligible_reason"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "new_eligible_reason"},
                        }
                    ],
                    "ineligible_criteria": ["SIMPLE_CRITERIA"],
                    "end_reason": "LEFT_CANDIDATE_POPULATION",
                },
            ],
        )

    def test_tes_query_covers_candidate_population(self) -> None:
        """Test that the TES spans cover the full candidate population time period:
        - The eligibility span does not start before the candidate population start date
        - The criteria default is used for periods during the population span when the criteria span is missing
        - The spans are split when the almost eligible flag changes
        """

        meets_criteria_default_true_view_builder = StateAgnosticTaskCriteriaBigQueryViewBuilder(
            criteria_name="SIMPLE_CRITERIA_MEETS_CRITERIA_DEFAULT_TRUE",
            criteria_spans_query_template="SELECT * FROM `{project_id}.test_dataset.foo`;",
            description="Simple criteria description",
            reasons_fields=[
                ReasonsField(
                    name="test_reason",
                    type=bigquery.StandardSqlTypeNames.STRING,
                    description="Simple reason description",
                )
            ],
            meets_criteria_default=True,
        )
        tes_view_builder = SingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            description="my_task_description",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[
                meets_criteria_default_true_view_builder,
            ],
            completion_event_builder=TEST_COMPLETION_EVENT_BUILDER,
            almost_eligible_condition=NotEligibleCriteriaCondition(
                criteria=meets_criteria_default_true_view_builder,
                description="Not eligible criteria condition",
            ),
        )

        basic_eligibility_address = BasicSingleTaskEligibilitySpansBigQueryViewBuilder.materialized_table_for_task_name(
            state_code=tes_view_builder.state_code,
            task_name=tes_view_builder.task_name,
        )

        load_data_for_basic_task_eligibility_span_view(
            emulator=self,
            basic_eligibility_address=basic_eligibility_address,
            basic_eligibility_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 6),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": meets_criteria_default_true_view_builder.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": meets_criteria_default_true_view_builder.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        }
                    ],
                    "ineligible_criteria": [
                        meets_criteria_default_true_view_builder.criteria_name
                    ],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 3, 1),
                    "is_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": meets_criteria_default_true_view_builder.criteria_name,
                            "reason": None,
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": meets_criteria_default_true_view_builder.criteria_name,
                            "reason": None,
                        }
                    ],
                    "ineligible_criteria": [],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2024, 3, 6),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": meets_criteria_default_true_view_builder.criteria_name,
                            "reason": {"test_reason": "reason_change"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": meets_criteria_default_true_view_builder.criteria_name,
                            "reason": {"test_reason": "reason_change"},
                        }
                    ],
                    "ineligible_criteria": [
                        meets_criteria_default_true_view_builder.criteria_name
                    ],
                },
            ],
        )
        load_data_for_almost_eligible_span_view(
            emulator=self,
            almost_eligible_address=AlmostEligibleSpansBigQueryViewBuilder.materialized_table_for_task_name(
                tes_view_builder.state_code,
                tes_view_builder.task_name,
            ),
            almost_eligible_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 1, 28),
                    "is_almost_eligible": False,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 28),
                    "end_date": date(2024, 2, 6),
                    "is_almost_eligible": True,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 3, 1),
                    "is_almost_eligible": False,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2024, 3, 6),
                    "is_almost_eligible": True,
                },
            ],
        )
        load_empty_task_completion_event_view(emulator=self)
        self.run_query_test(
            query_str=tes_view_builder.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": tes_view_builder.task_name,
                    "task_eligibility_span_id": 1,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 1, 28),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": meets_criteria_default_true_view_builder.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": meets_criteria_default_true_view_builder.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        }
                    ],
                    "ineligible_criteria": [
                        meets_criteria_default_true_view_builder.criteria_name
                    ],
                    "end_reason": "BECAME_ALMOST_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": tes_view_builder.task_name,
                    "task_eligibility_span_id": 2,
                    "start_date": date(2024, 1, 28),
                    "end_date": date(2024, 2, 6),
                    "is_eligible": False,
                    "is_almost_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": meets_criteria_default_true_view_builder.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": meets_criteria_default_true_view_builder.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        }
                    ],
                    "ineligible_criteria": [
                        meets_criteria_default_true_view_builder.criteria_name
                    ],
                    "end_reason": "BECAME_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": tes_view_builder.task_name,
                    "task_eligibility_span_id": 3,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 3, 1),
                    "is_eligible": True,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": meets_criteria_default_true_view_builder.criteria_name,
                            "reason": None,
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": meets_criteria_default_true_view_builder.criteria_name,
                            "reason": None,
                        }
                    ],
                    "ineligible_criteria": [],
                    "end_reason": "BECAME_ALMOST_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": tes_view_builder.task_name,
                    "task_eligibility_span_id": 4,
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2024, 3, 6),
                    "is_eligible": False,
                    "is_almost_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": meets_criteria_default_true_view_builder.criteria_name,
                            "reason": {"test_reason": "reason_change"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": meets_criteria_default_true_view_builder.criteria_name,
                            "reason": {"test_reason": "reason_change"},
                        }
                    ],
                    "ineligible_criteria": [
                        meets_criteria_default_true_view_builder.criteria_name
                    ],
                    "end_reason": "LEFT_CANDIDATE_POPULATION",
                },
            ],
        )

    def test_tes_query_final_open_span(self) -> None:
        """
        Test the Task Eligibility Spans view for a client with an open eligibility span
        (end_date and end_reason are NULL) and two eligibility criteria with one time dependent
        almost eligible criteria condition that splits ineligible spans.
        """
        tes_view_builder = SingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            description="my_task_description",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[
                TEST_CRITERIA_BUILDER_1,
                TEST_CRITERIA_BUILDER_4,
            ],
            completion_event_builder=TEST_COMPLETION_EVENT_BUILDER,
            almost_eligible_condition=TimeDependentCriteriaCondition(
                criteria=TEST_CRITERIA_BUILDER_4,
                reasons_date_field="test_reason_date",
                interval_length=1,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Time dependent criteria condition",
            ),
        )
        load_data_for_basic_task_eligibility_span_view(
            emulator=self,
            basic_eligibility_address=BasicSingleTaskEligibilitySpansBigQueryViewBuilder.materialized_table_for_task_name(
                state_code=tes_view_builder.state_code,
                task_name=tes_view_builder.task_name,
            ),
            basic_eligibility_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 6),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": None},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": None},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_1.criteria_name,
                        TEST_CRITERIA_BUILDER_4.criteria_name,
                    ],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 2, 10),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": None},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": None},
                        },
                    ],
                    "ineligible_criteria": [TEST_CRITERIA_BUILDER_4.criteria_name],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 3,
                    "start_date": date(2024, 2, 10),
                    "end_date": date(2024, 3, 18),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [TEST_CRITERIA_BUILDER_4.criteria_name],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 18),
                    "end_date": None,
                    "is_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [],
                },
            ],
        )
        load_data_for_almost_eligible_span_view(
            emulator=self,
            almost_eligible_address=AlmostEligibleSpansBigQueryViewBuilder.materialized_table_for_task_name(
                tes_view_builder.state_code,
                tes_view_builder.task_name,
            ),
            almost_eligible_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 6),
                    "is_almost_eligible": False,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 2, 10),
                    "is_almost_eligible": False,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 10),
                    "end_date": date(2024, 2, 18),
                    "is_almost_eligible": False,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 18),
                    "end_date": date(2024, 3, 18),
                    "is_almost_eligible": True,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 18),
                    "end_date": None,
                    "is_almost_eligible": False,
                },
            ],
        )

        load_empty_task_completion_event_view(emulator=self)

        self.run_query_test(
            query_str=tes_view_builder.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": tes_view_builder.task_name,
                    "task_eligibility_span_id": 1,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 6),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": None},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": None},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_1.criteria_name,
                        TEST_CRITERIA_BUILDER_4.criteria_name,
                    ],
                    "end_reason": "INELIGIBLE_CRITERIA_CHANGED",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": tes_view_builder.task_name,
                    "task_eligibility_span_id": 2,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 2, 10),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": None},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": None},
                        },
                    ],
                    "ineligible_criteria": [TEST_CRITERIA_BUILDER_4.criteria_name],
                    "end_reason": "INELIGIBLE_REASONS_CHANGED",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": tes_view_builder.task_name,
                    "task_eligibility_span_id": 3,
                    "start_date": date(2024, 2, 10),
                    "end_date": date(2024, 2, 18),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [TEST_CRITERIA_BUILDER_4.criteria_name],
                    "end_reason": "BECAME_ALMOST_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": tes_view_builder.task_name,
                    "task_eligibility_span_id": 4,
                    "start_date": date(2024, 2, 18),
                    "end_date": date(2024, 3, 18),
                    "is_eligible": False,
                    "is_almost_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [TEST_CRITERIA_BUILDER_4.criteria_name],
                    "end_reason": "BECAME_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": tes_view_builder.task_name,
                    "task_eligibility_span_id": 5,
                    "start_date": date(2024, 3, 18),
                    "end_date": None,
                    "is_eligible": True,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [],
                    "end_reason": None,
                },
            ],
        )

    def test_non_adjacent_population_spans_with_two_person_ids(self) -> None:
        """
        Verify eligibility spans are computed per person and cover the entire candidate population span
        """
        load_data_for_task_criteria_view(
            emulator=self,
            criteria_view_builder=TEST_CRITERIA_BUILDER_1,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "end_date": None,
                    "meets_criteria": False,
                    "reason": {"test_reason": "reason_text"},
                    "reason_v2": {"test_reason": "reason_text"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 23456,
                    "start_date": date(2024, 1, 28),
                    "end_date": date(2024, 4, 4),
                    "meets_criteria": False,
                    "reason": {"test_reason": "reason_text_2"},
                    "reason_v2": {"test_reason": "reason_text_2"},
                },
            ],
        )
        load_data_for_candidate_population_view(
            emulator=self,
            population_date_spans=[
                (date(2024, 1, 1), date(2024, 3, 18)),
                (date(2024, 4, 8), date(2024, 5, 15)),
            ],
        )
        load_data_for_candidate_population_view(
            emulator=self,
            population_date_spans=[
                (date(2024, 1, 28), date(2024, 3, 6)),
                (date(2024, 3, 28), None),
            ],
            test_person_id=23456,
            create_table=False,
        )
        load_empty_task_completion_event_view(emulator=self)
        tes_query_builder = SingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            description="my_task_description",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[
                TEST_CRITERIA_BUILDER_1,
            ],
            completion_event_builder=TEST_COMPLETION_EVENT_BUILDER,
        )

        self.run_query_test(
            query_str=tes_query_builder.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 1,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 6),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": None,
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": None,
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_1.criteria_name,
                    ],
                    "end_reason": "INELIGIBLE_REASONS_CHANGED",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 2,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 3, 18),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_1.criteria_name,
                    ],
                    "end_reason": "LEFT_CANDIDATE_POPULATION",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 3,
                    "start_date": date(2024, 4, 8),
                    "end_date": date(2024, 5, 15),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_1.criteria_name,
                    ],
                    "end_reason": "LEFT_CANDIDATE_POPULATION",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 23456,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 1,
                    "start_date": date(2024, 1, 28),
                    "end_date": date(2024, 3, 6),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text_2"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text_2"},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_1.criteria_name,
                    ],
                    "end_reason": "LEFT_CANDIDATE_POPULATION",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 23456,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 2,
                    "start_date": date(2024, 3, 28),
                    "end_date": date(2024, 4, 4),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text_2"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text_2"},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_1.criteria_name,
                    ],
                    "end_reason": "INELIGIBLE_REASONS_CHANGED",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 23456,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 3,
                    "start_date": date(2024, 4, 4),
                    "end_date": None,
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": None,
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": None,
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_1.criteria_name,
                    ],
                    "end_reason": None,
                },
            ],
        )
