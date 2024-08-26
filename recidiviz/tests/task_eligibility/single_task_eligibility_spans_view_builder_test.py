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
from typing import Any, Dict, List, Optional, Tuple, Union

from google.cloud import bigquery

from recidiviz.big_query.big_query_utils import (
    BigQueryDateInterval,
    schema_field_for_type,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.criteria_condition import (
    LessThanOrEqualCriteriaCondition,
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    ReasonDateInCalendarWeekCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.single_task_eligiblity_spans_view_builder import (
    SingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateAgnosticTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_completion_event_big_query_view_builder import (
    StateAgnosticTaskCompletionEventBigQueryViewBuilder,
    TaskCompletionEventType,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    InvertedTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaGroupBigQueryViewBuilder,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.utils import metadata
from recidiviz.utils.string import StrictStringFormatter

TEST_POPULATION_BUILDER = StateAgnosticTaskCandidatePopulationBigQueryViewBuilder(
    population_name="SIMPLE_POPULATION",
    population_spans_query_template="SELECT * FROM `{project_id}.test_dataset.foo`;",
    description="Simple population description",
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

TEST_COMPLETION_EVENT_BUILDER = StateAgnosticTaskCompletionEventBigQueryViewBuilder(
    completion_event_type=TaskCompletionEventType.EARLY_DISCHARGE,
    completion_event_query_template="SELECT * FROM `{project_id}.test_dataset.foo`;",
    description="Simple event description",
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

    def _load_data_for_criteria_view(
        self,
        criteria_view_builder: Union[
            TaskCriteriaBigQueryViewBuilder,
            TaskCriteriaGroupBigQueryViewBuilder,
            InvertedTaskCriteriaBigQueryViewBuilder,
        ],
        criteria_data: List[Dict[str, Any]],
    ) -> None:
        self.create_mock_table(
            address=criteria_view_builder.table_for_query,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("start_date", date),
                schema_field_for_type("end_date", date),
                schema_field_for_type("meets_criteria", bool),
                bigquery.SchemaField(
                    "reason",
                    field_type=bigquery.enums.StandardSqlTypeNames.JSON.value,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    "reason_v2",
                    field_type=bigquery.enums.StandardSqlTypeNames.JSON.value,
                    mode="NULLABLE",
                ),
            ],
        )
        self.load_rows_into_table(
            address=criteria_view_builder.table_for_query,
            data=criteria_data,
        )

    def _load_data_for_candidate_population_view(
        self,
        population_date_spans: List[Tuple[date, Optional[date]]],
        test_person_id: int = 12345,
        create_table: bool = True,
    ) -> None:
        """Helper method for loading the mock candidate population view for the test person from state US_XX"""
        if create_table:
            self.create_mock_table(
                address=TEST_POPULATION_BUILDER.table_for_query,
                schema=[
                    schema_field_for_type("state_code", str),
                    schema_field_for_type("person_id", int),
                    schema_field_for_type("start_date", date),
                    schema_field_for_type("end_date", date),
                ],
            )
        self.load_rows_into_table(
            address=TEST_POPULATION_BUILDER.table_for_query,
            data=[
                {
                    "state_code": "US_XX",
                    "person_id": test_person_id,
                    "start_date": date_span[0],
                    "end_date": date_span[1],
                }
                for date_span in population_date_spans
            ],
        )

    def _load_data_for_completion_event_view(
        self,
        completion_event_view_builder: StateAgnosticTaskCompletionEventBigQueryViewBuilder,
        task_completion_data: List[Dict[str, Any]],
    ) -> None:
        self.create_mock_table(
            address=completion_event_view_builder.table_for_query,
            schema=[
                schema_field_for_type("state_code", str),
                schema_field_for_type("person_id", int),
                schema_field_for_type("completion_event_date", date),
            ],
        )
        self.load_rows_into_table(
            address=completion_event_view_builder.table_for_query,
            data=task_completion_data,
        )

    def _load_empty_completion_event_view(self) -> None:
        """Helper method for loading the mock task completion event view with no rows"""
        self._load_data_for_completion_event_view(
            completion_event_view_builder=TEST_COMPLETION_EVENT_BUILDER,
            task_completion_data=[],
        )

    def test_simple_tes_query(self) -> None:
        """
        Verify that the Task Eligibility Spans view can properly handle a client that goes from ineligible to eligible
        for a single criteria
        """
        self._load_data_for_criteria_view(
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
        self._load_data_for_candidate_population_view(
            [(date(2024, 1, 1), date(2024, 4, 8))]
        )
        self._load_data_for_completion_event_view(
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

        self._load_data_for_criteria_view(
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
                    "end_date": date(2024, 4, 8),
                    "meets_criteria": True,
                    "reason": {"test_reason": "new_eligible_reason"},
                    "reason_v2": {"test_reason": "new_eligible_reason"},
                },
            ],
        )
        self._load_data_for_candidate_population_view(
            [
                (date(2024, 1, 1), date(2024, 4, 8)),
            ]
        )
        self._load_empty_completion_event_view()
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
                    "end_date": date(2024, 4, 8),
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
                    "end_reason": "LEFT_CANDIDATE_POPULATION",
                },
            ],
        )

    def test_tes_query_covers_candidate_population(self) -> None:
        """Test that the TES spans cover the full candidate population time period:
        - The eligibility span does not start before the candidate population start date
        - The criteria default is used for periods during the population span when the criteria span is missing
        - The spans are marked as almost eligible when the criterion is not met (NotEligibleCriteriaCondition)
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

        self._load_data_for_criteria_view(
            criteria_view_builder=meets_criteria_default_true_view_builder,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2023, 12, 1),
                    "end_date": date(2024, 2, 6),
                    "meets_criteria": False,
                    "reason": {"test_reason": "reason_text"},
                    "reason_v2": {"test_reason": "reason_text"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2024, 3, 18),
                    "meets_criteria": False,
                    "reason": {"test_reason": "reason_change"},
                    "reason_v2": {"test_reason": "reason_change"},
                },
            ],
        )
        self._load_data_for_candidate_population_view(
            [
                (date(2024, 1, 1), date(2024, 3, 6)),
            ]
        )
        self._load_empty_completion_event_view()
        self.run_query_test(
            query_str=tes_view_builder.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 1,
                    "start_date": date(2024, 1, 1),
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
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 2,
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
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 3,
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
        (end_date and end_reason are NULL) and two eligibility criteria with one almost eligible criteria condition
        """
        self._load_data_for_criteria_view(
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
                    "end_date": None,
                    "meets_criteria": True,
                    "reason": {"test_reason": "eligible_reason"},
                    "reason_v2": {"test_reason": "eligible_reason"},
                },
            ],
        )
        second_criteria_view_builder = StateSpecificTaskCriteriaBigQueryViewBuilder(
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
        self._load_data_for_criteria_view(
            criteria_view_builder=second_criteria_view_builder,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 10),
                    "meets_criteria": False,
                    "reason": {"test_reason_date": None},
                    "reason_v2": {"test_reason_date": None},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 10),
                    "end_date": date(2024, 3, 18),
                    "meets_criteria": False,
                    "reason": {"test_reason_date": "2024-03-18"},
                    "reason_v2": {"test_reason_date": "2024-03-18"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 18),
                    "end_date": None,
                    "meets_criteria": True,
                    "reason": {"test_reason_date": "2024-03-18"},
                    "reason_v2": {"test_reason_date": "2024-03-18"},
                },
            ],
        )
        self._load_data_for_candidate_population_view(
            [
                (date(2024, 1, 1), None),
            ]
        )
        self._load_empty_completion_event_view()
        tes_query_builder = SingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            description="my_task_description",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[
                TEST_CRITERIA_BUILDER_1,
                second_criteria_view_builder,
            ],
            completion_event_builder=TEST_COMPLETION_EVENT_BUILDER,
            almost_eligible_condition=TimeDependentCriteriaCondition(
                criteria=second_criteria_view_builder,
                reasons_date_field="test_reason_date",
                interval_length=1,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Time dependent criteria condition",
            ),
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
                            "reason": {"test_reason": "reason_text"},
                        },
                        {
                            "criteria_name": second_criteria_view_builder.criteria_name,
                            "reason": {"test_reason_date": None},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                        {
                            "criteria_name": second_criteria_view_builder.criteria_name,
                            "reason": {"test_reason_date": None},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_1.criteria_name,
                        second_criteria_view_builder.criteria_name,
                    ],
                    "end_reason": "INELIGIBLE_CRITERIA_CHANGED",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
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
                            "criteria_name": second_criteria_view_builder.criteria_name,
                            "reason": {"test_reason_date": None},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": second_criteria_view_builder.criteria_name,
                            "reason": {"test_reason_date": None},
                        },
                    ],
                    "ineligible_criteria": [second_criteria_view_builder.criteria_name],
                    "end_reason": "INELIGIBLE_REASONS_CHANGED",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
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
                            "criteria_name": second_criteria_view_builder.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": second_criteria_view_builder.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [second_criteria_view_builder.criteria_name],
                    "end_reason": "BECAME_ALMOST_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
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
                            "criteria_name": second_criteria_view_builder.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": second_criteria_view_builder.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [second_criteria_view_builder.criteria_name],
                    "end_reason": "BECAME_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
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
                            "criteria_name": second_criteria_view_builder.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": second_criteria_view_builder.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [],
                    "end_reason": None,
                },
            ],
        )

    def test_almost_eligible_if_x_xor_y_and_z(self) -> None:
        """
        Test three criteria used in a nested composite criteria condition (Criteria_X XOR (Criteria_Y AND Criteria_Z))
        """
        self._load_data_for_criteria_view(
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
                    "end_date": date(2024, 4, 8),
                    "meets_criteria": True,
                    "reason": {"test_reason": "eligible_reason"},
                    "reason_v2": {"test_reason": "eligible_reason"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 8),
                    "end_date": None,
                    "meets_criteria": False,
                    "reason": {"test_reason": "reason_text"},
                    "reason_v2": {"test_reason": "reason_text"},
                },
            ],
        )
        self._load_data_for_criteria_view(
            criteria_view_builder=TEST_CRITERIA_BUILDER_2,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 15),
                    "meets_criteria": False,
                    "reason": {"test_reason_int": 2},
                    "reason_v2": {"test_reason_int": 2},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 15),
                    "end_date": date(2024, 3, 28),
                    "meets_criteria": False,
                    "reason": {"test_reason_int": 1},
                    "reason_v2": {"test_reason_int": 1},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 28),
                    "end_date": None,
                    "meets_criteria": True,
                    "reason": {"test_reason_int": 0},
                    "reason_v2": {"test_reason_int": 0},
                },
            ],
        )
        self._load_data_for_criteria_view(
            criteria_view_builder=TEST_CRITERIA_BUILDER_3,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 3, 18),
                    "meets_criteria": False,
                    "reason": {"test_reason_date": "2024-03-18"},
                    "reason_v2": {"test_reason_date": "2024-03-18"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 18),
                    "end_date": None,
                    "meets_criteria": True,
                    "reason": {"test_reason_date": "2024-03-18"},
                    "reason_v2": {"test_reason_date": "2024-03-18"},
                },
            ],
        )
        self._load_data_for_candidate_population_view([(date(2024, 1, 1), None)])
        self._load_empty_completion_event_view()
        tes_query_builder = SingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            description="my_task_description",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[
                TEST_CRITERIA_BUILDER_1,
                TEST_CRITERIA_BUILDER_2,
                TEST_CRITERIA_BUILDER_3,
            ],
            completion_event_builder=TEST_COMPLETION_EVENT_BUILDER,
            almost_eligible_condition=PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    # Criteria 1 XOR (Criteria 2 AND 3)
                    NotEligibleCriteriaCondition(
                        criteria=TEST_CRITERIA_BUILDER_1,
                        description="Not eligible criteria condition",
                    ),
                    PickNCompositeCriteriaCondition(
                        sub_conditions_list=[
                            LessThanOrEqualCriteriaCondition(
                                criteria=TEST_CRITERIA_BUILDER_2,
                                reasons_numerical_field="test_reason_int",
                                value=1,
                                description="Less than value criteria condition",
                            ),
                            ReasonDateInCalendarWeekCriteriaCondition(
                                criteria=TEST_CRITERIA_BUILDER_3,
                                reasons_date_field="test_reason_date",
                                description="Reason date in calendar week criteria condition",
                            ),
                        ],
                        at_least_n_conditions_true=2,
                    ),
                ],
                at_most_n_conditions_true=1,
            ),
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
                            "reason": {"test_reason": "reason_text"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 2},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 2},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_1.criteria_name,
                        TEST_CRITERIA_BUILDER_2.criteria_name,
                        TEST_CRITERIA_BUILDER_3.criteria_name,
                    ],
                    "end_reason": "INELIGIBLE_CRITERIA_CHANGED",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 2,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 2, 15),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 2},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 2},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_2.criteria_name,
                        TEST_CRITERIA_BUILDER_3.criteria_name,
                    ],
                    "end_reason": "INELIGIBLE_REASONS_CHANGED",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 3,
                    "start_date": date(2024, 2, 15),
                    "end_date": date(2024, 3, 17),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 1},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 1},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_2.criteria_name,
                        TEST_CRITERIA_BUILDER_3.criteria_name,
                    ],
                    "end_reason": "BECAME_ALMOST_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 4,
                    "start_date": date(2024, 3, 17),
                    "end_date": date(2024, 3, 18),
                    "is_eligible": False,
                    "is_almost_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 1},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 1},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_2.criteria_name,
                        TEST_CRITERIA_BUILDER_3.criteria_name,
                    ],
                    "end_reason": "INELIGIBLE_CRITERIA_CHANGED",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 5,
                    "start_date": date(2024, 3, 18),
                    "end_date": date(2024, 3, 28),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 1},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 1},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [TEST_CRITERIA_BUILDER_2.criteria_name],
                    "end_reason": "BECAME_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 6,
                    "start_date": date(2024, 3, 28),
                    "end_date": date(2024, 4, 8),
                    "is_eligible": True,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 0},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 0},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [],
                    "end_reason": "BECAME_ALMOST_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 7,
                    "start_date": date(2024, 4, 8),
                    "end_date": None,
                    "is_eligible": False,
                    "is_almost_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 0},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 0},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [TEST_CRITERIA_BUILDER_1.criteria_name],
                    "end_reason": None,
                },
            ],
        )

    def test_almost_eligible_if_x_or_y(self) -> None:
        """
        Test the composite criteria condition for inclusive OR logic (Criteria_X OR Criteria_Y)
        """
        self._load_data_for_criteria_view(
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
                    "end_date": date(2024, 4, 8),
                    "meets_criteria": True,
                    "reason": {"test_reason": "eligible_reason"},
                    "reason_v2": {"test_reason": "eligible_reason"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 8),
                    "end_date": None,
                    "meets_criteria": False,
                    "reason": {"test_reason": "reason_text"},
                    "reason_v2": {"test_reason": "reason_text"},
                },
            ],
        )
        self._load_data_for_criteria_view(
            criteria_view_builder=TEST_CRITERIA_BUILDER_2,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 15),
                    "meets_criteria": False,
                    "reason": {"test_reason_int": None},
                    "reason_v2": {"test_reason_int": None},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 15),
                    "end_date": date(2024, 3, 28),
                    "meets_criteria": False,
                    "reason": {"test_reason_int": 1},
                    "reason_v2": {"test_reason_int": 1},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 28),
                    "end_date": None,
                    "meets_criteria": True,
                    "reason": {"test_reason_int": 0},
                    "reason_v2": {"test_reason_int": 0},
                },
            ],
        )
        self._load_data_for_candidate_population_view([(date(2024, 1, 1), None)])
        self._load_empty_completion_event_view()
        tes_query_builder = SingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            description="my_task_description",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[
                TEST_CRITERIA_BUILDER_1,
                TEST_CRITERIA_BUILDER_2,
            ],
            completion_event_builder=TEST_COMPLETION_EVENT_BUILDER,
            almost_eligible_condition=PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    # Criteria 1 OR Criteria 2
                    NotEligibleCriteriaCondition(
                        criteria=TEST_CRITERIA_BUILDER_1,
                        description="Not eligible criteria condition",
                    ),
                    LessThanOrEqualCriteriaCondition(
                        criteria=TEST_CRITERIA_BUILDER_2,
                        reasons_numerical_field="test_reason_int",
                        value=1,
                        description="Less than value criteria condition",
                    ),
                ],
                at_least_n_conditions_true=1,
            ),
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
                    "is_almost_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": None},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": None},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_1.criteria_name,
                        TEST_CRITERIA_BUILDER_2.criteria_name,
                    ],
                    "end_reason": "INELIGIBLE_CRITERIA_CHANGED",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 2,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 2, 15),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": None},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": None},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_2.criteria_name,
                    ],
                    "end_reason": "BECAME_ALMOST_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 3,
                    "start_date": date(2024, 2, 15),
                    "end_date": date(2024, 3, 28),
                    "is_eligible": False,
                    "is_almost_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 1},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 1},
                        },
                    ],
                    "ineligible_criteria": [TEST_CRITERIA_BUILDER_2.criteria_name],
                    "end_reason": "BECAME_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 4,
                    "start_date": date(2024, 3, 28),
                    "end_date": date(2024, 4, 8),
                    "is_eligible": True,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 0},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible_reason"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 0},
                        },
                    ],
                    "ineligible_criteria": [],
                    "end_reason": "BECAME_ALMOST_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 5,
                    "start_date": date(2024, 4, 8),
                    "end_date": None,
                    "is_eligible": False,
                    "is_almost_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 0},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 0},
                        },
                    ],
                    "ineligible_criteria": [TEST_CRITERIA_BUILDER_1.criteria_name],
                    "end_reason": None,
                },
            ],
        )

    def test_non_adjacent_population_spans_with_two_person_ids(self) -> None:
        """
        Verify eligibility spans are computed per person and cover the entire candidate population span
        """
        self._load_data_for_criteria_view(
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
        self._load_data_for_candidate_population_view(
            [
                (date(2024, 1, 1), date(2024, 3, 18)),
                (date(2024, 4, 8), date(2024, 5, 15)),
            ]
        )
        self._load_data_for_candidate_population_view(
            [
                (date(2024, 1, 28), date(2024, 3, 6)),
                (date(2024, 3, 28), None),
            ],
            test_person_id=23456,
            create_table=False,
        )
        self._load_empty_completion_event_view()
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

    def test_tes_query_two_time_dependent_conditions_one_criteria(self) -> None:
        """
        Test the Task Eligibility Spans view for a task with two time dependent criteria conditions applied to
        one eligibility criteria.
        """
        two_date_criteria_view_builder = StateSpecificTaskCriteriaBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            criteria_name="US_XX_SIMPLE_CRITERIA",
            criteria_spans_query_template="SELECT * FROM `{project_id}.test_dataset.state_foo`;",
            description="Simple state specific criteria description",
            reasons_fields=[
                ReasonsField(
                    name="first_reason_date",
                    type=bigquery.StandardSqlTypeNames.DATE,
                    description="Simple reason description",
                ),
                ReasonsField(
                    name="second_reason_date",
                    type=bigquery.StandardSqlTypeNames.DATE,
                    description="Another simple reason description",
                ),
            ],
        )
        self._load_data_for_criteria_view(
            criteria_view_builder=two_date_criteria_view_builder,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 28),
                    "meets_criteria": False,
                    "reason": {
                        "first_reason_date": "2024-03-11",
                        "second_reason_date": "2024-05-15",
                    },
                    "reason_v2": {
                        "first_reason_date": "2024-03-11",
                        "second_reason_date": "2024-05-15",
                    },
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 28),
                    "end_date": date(2024, 3, 27),
                    "meets_criteria": False,
                    "reason": {
                        "first_reason_date": "2024-03-27",
                        "second_reason_date": "2024-07-15",
                    },
                    "reason_v2": {
                        "first_reason_date": "2024-03-27",
                        "second_reason_date": "2024-07-15",
                    },
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 27),
                    "end_date": None,
                    "meets_criteria": False,
                    "reason": {
                        "first_reason_date": None,
                        "second_reason_date": "2024-07-15",
                    },
                    "reason_v2": {
                        "first_reason_date": None,
                        "second_reason_date": "2024-07-15",
                    },
                },
            ],
        )
        self._load_data_for_candidate_population_view(
            [
                (date(2024, 1, 1), None),
            ]
        )
        self._load_empty_completion_event_view()
        tes_query_builder = SingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            description="my_task_description",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[two_date_criteria_view_builder],
            completion_event_builder=TEST_COMPLETION_EVENT_BUILDER,
            almost_eligible_condition=PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    TimeDependentCriteriaCondition(
                        criteria=two_date_criteria_view_builder,
                        reasons_date_field="first_reason_date",
                        interval_length=2,
                        interval_date_part=BigQueryDateInterval.WEEK,
                        description="Within 2 weeks of first reason date",
                    ),
                    TimeDependentCriteriaCondition(
                        criteria=two_date_criteria_view_builder,
                        reasons_date_field="second_reason_date",
                        interval_length=3,
                        interval_date_part=BigQueryDateInterval.MONTH,
                        description="Within 3 months of second reason date",
                    ),
                ],
                at_least_n_conditions_true=1,
            ),
        )
        # Test the criteria spans are split on the critical date boundaries correctly
        if tes_query_builder.almost_eligible_condition is None:
            raise ValueError(
                "SingleTaskEligibilitySpansBigQueryViewBuilder must have the `almost_eligible_condition` attribute"
                " populated when almost eligible conditions are supplied."
            )
        critical_date_list = (
            tes_query_builder.almost_eligible_condition.get_critical_date_parsing_fragments_by_criteria()
        )
        if critical_date_list is None:
            raise ValueError(
                "Eligibility span must have critical dates populated when TimeDependentCriteriaCondition are"
                " included in the almost eligible conditions."
            )
        if two_date_criteria_view_builder.materialized_address is None:
            raise ValueError(
                "Expected the criteria view builder to have a materialized address since all criteria"
                " span views are materialized."
            )
        critical_date_split_query = StrictStringFormatter().format(
            StrictStringFormatter().format(
                tes_query_builder.split_criteria_by_critical_date_query_fragment(
                    critical_date_list[two_date_criteria_view_builder.criteria_name]
                ),
                criteria_dataset_id=two_date_criteria_view_builder.dataset_id,
                criteria_name=two_date_criteria_view_builder.criteria_name,
                criteria_view_id=two_date_criteria_view_builder.materialized_address.table_id,
                state_code=two_date_criteria_view_builder.state_code.value,
            ),
            project_id=metadata.project_id(),
        )
        self.run_query_test(
            query_str=critical_date_split_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 15),
                    "meets_criteria": False,
                    "reason": {
                        "first_reason_date": "2024-03-11",
                        "second_reason_date": "2024-05-15",
                    },
                    "reason_v2": {
                        "first_reason_date": "2024-03-11",
                        "second_reason_date": "2024-05-15",
                    },
                    "criteria_name": two_date_criteria_view_builder.criteria_name,
                },
                # Split by the second_reason_date 3 month condition
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 15),
                    "end_date": date(2024, 2, 26),
                    "meets_criteria": False,
                    "reason": {
                        "first_reason_date": "2024-03-11",
                        "second_reason_date": "2024-05-15",
                    },
                    "reason_v2": {
                        "first_reason_date": "2024-03-11",
                        "second_reason_date": "2024-05-15",
                    },
                    "criteria_name": two_date_criteria_view_builder.criteria_name,
                },
                # Split by the first_reason_date 2 week condition
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 26),
                    "end_date": date(2024, 2, 28),
                    "meets_criteria": False,
                    "reason": {
                        "first_reason_date": "2024-03-11",
                        "second_reason_date": "2024-05-15",
                    },
                    "reason_v2": {
                        "first_reason_date": "2024-03-11",
                        "second_reason_date": "2024-05-15",
                    },
                    "criteria_name": two_date_criteria_view_builder.criteria_name,
                },
                # Split by the criteria reasons change
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 28),
                    "end_date": date(2024, 3, 13),
                    "meets_criteria": False,
                    "reason": {
                        "first_reason_date": "2024-03-27",
                        "second_reason_date": "2024-07-15",
                    },
                    "reason_v2": {
                        "first_reason_date": "2024-03-27",
                        "second_reason_date": "2024-07-15",
                    },
                    "criteria_name": two_date_criteria_view_builder.criteria_name,
                },
                # Split by the first_reason_date 2 week condition
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 13),
                    "end_date": date(2024, 3, 27),
                    "meets_criteria": False,
                    "reason": {
                        "first_reason_date": "2024-03-27",
                        "second_reason_date": "2024-07-15",
                    },
                    "reason_v2": {
                        "first_reason_date": "2024-03-27",
                        "second_reason_date": "2024-07-15",
                    },
                    "criteria_name": two_date_criteria_view_builder.criteria_name,
                },
                # Split by the criteria reasons change
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 27),
                    "end_date": date(2024, 4, 15),
                    "meets_criteria": False,
                    "reason": {
                        "first_reason_date": None,
                        "second_reason_date": "2024-07-15",
                    },
                    "reason_v2": {
                        "first_reason_date": None,
                        "second_reason_date": "2024-07-15",
                    },
                    "criteria_name": two_date_criteria_view_builder.criteria_name,
                },
                # Split by the second_reason_date 3 month condition
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 15),
                    "end_date": None,
                    "meets_criteria": False,
                    "reason": {
                        "first_reason_date": None,
                        "second_reason_date": "2024-07-15",
                    },
                    "reason_v2": {
                        "first_reason_date": None,
                        "second_reason_date": "2024-07-15",
                    },
                    "criteria_name": two_date_criteria_view_builder.criteria_name,
                },
            ],
        )

        # Test the full query generates the eligibility spans correctly
        self.run_query_test(
            query_str=tes_query_builder.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 1,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 15),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": two_date_criteria_view_builder.criteria_name,
                            "reason": {
                                "first_reason_date": "2024-03-11",
                                "second_reason_date": "2024-05-15",
                            },
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": two_date_criteria_view_builder.criteria_name,
                            "reason": {
                                "first_reason_date": "2024-03-11",
                                "second_reason_date": "2024-05-15",
                            },
                        },
                    ],
                    "ineligible_criteria": [
                        two_date_criteria_view_builder.criteria_name,
                    ],
                    "end_reason": "BECAME_ALMOST_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 2,
                    "start_date": date(2024, 2, 15),
                    "end_date": date(2024, 2, 28),
                    "is_eligible": False,
                    "is_almost_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": two_date_criteria_view_builder.criteria_name,
                            "reason": {
                                "first_reason_date": "2024-03-11",
                                "second_reason_date": "2024-05-15",
                            },
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": two_date_criteria_view_builder.criteria_name,
                            "reason": {
                                "first_reason_date": "2024-03-11",
                                "second_reason_date": "2024-05-15",
                            },
                        },
                    ],
                    "ineligible_criteria": [
                        two_date_criteria_view_builder.criteria_name
                    ],
                    "end_reason": "INELIGIBLE_REASONS_CHANGED",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 3,
                    "start_date": date(2024, 2, 28),
                    "end_date": date(2024, 3, 13),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": two_date_criteria_view_builder.criteria_name,
                            "reason": {
                                "first_reason_date": "2024-03-27",
                                "second_reason_date": "2024-07-15",
                            },
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": two_date_criteria_view_builder.criteria_name,
                            "reason": {
                                "first_reason_date": "2024-03-27",
                                "second_reason_date": "2024-07-15",
                            },
                        },
                    ],
                    "ineligible_criteria": [
                        two_date_criteria_view_builder.criteria_name
                    ],
                    "end_reason": "BECAME_ALMOST_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 4,
                    "start_date": date(2024, 3, 13),
                    "end_date": date(2024, 3, 27),
                    "is_eligible": False,
                    "is_almost_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": two_date_criteria_view_builder.criteria_name,
                            "reason": {
                                "first_reason_date": "2024-03-27",
                                "second_reason_date": "2024-07-15",
                            },
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": two_date_criteria_view_builder.criteria_name,
                            "reason": {
                                "first_reason_date": "2024-03-27",
                                "second_reason_date": "2024-07-15",
                            },
                        },
                    ],
                    "ineligible_criteria": [
                        two_date_criteria_view_builder.criteria_name
                    ],
                    "end_reason": "INELIGIBLE_REASONS_CHANGED",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 5,
                    "start_date": date(2024, 3, 27),
                    "end_date": date(2024, 4, 15),
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": two_date_criteria_view_builder.criteria_name,
                            "reason": {
                                "first_reason_date": None,
                                "second_reason_date": "2024-07-15",
                            },
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": two_date_criteria_view_builder.criteria_name,
                            "reason": {
                                "first_reason_date": None,
                                "second_reason_date": "2024-07-15",
                            },
                        },
                    ],
                    "ineligible_criteria": [
                        two_date_criteria_view_builder.criteria_name
                    ],
                    "end_reason": "BECAME_ALMOST_ELIGIBLE",
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_name": TES_QUERY_BUILDER.task_name,
                    "task_eligibility_span_id": 6,
                    "start_date": date(2024, 4, 15),
                    "end_date": None,
                    "is_eligible": False,
                    "is_almost_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": two_date_criteria_view_builder.criteria_name,
                            "reason": {
                                "first_reason_date": None,
                                "second_reason_date": "2024-07-15",
                            },
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": two_date_criteria_view_builder.criteria_name,
                            "reason": {
                                "first_reason_date": None,
                                "second_reason_date": "2024-07-15",
                            },
                        },
                    ],
                    "ineligible_criteria": [
                        two_date_criteria_view_builder.criteria_name
                    ],
                    "end_reason": None,
                },
            ],
        )
