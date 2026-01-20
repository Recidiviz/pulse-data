# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Tests for the BasicSingleTaskEligibilitySpansBigQueryViewBuilder."""
from datetime import date

from google.cloud import bigquery

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.basic_single_task_eligibility_spans_big_query_view_builder import (
    BasicSingleTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_candidate_population_big_query_view_builder import (
    StateSpecificTaskCandidatePopulationBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.task_eligibility.single_task_eligibility_spans_view_builder_test import (
    TEST_CRITERIA_BUILDER_1,
    TEST_CRITERIA_BUILDER_4,
    TEST_POPULATION_BUILDER,
)
from recidiviz.tests.task_eligibility.task_eligibility_big_query_emulator_utils import (
    load_data_for_candidate_population_view,
    load_data_for_task_criteria_view,
)

BASIC_ELIGIBILITY_VIEW_BUILDER = BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_XX,
    task_name="my_task_name",
    candidate_population_view_builder=TEST_POPULATION_BUILDER,
    criteria_spans_view_builders=[
        TEST_CRITERIA_BUILDER_1,
    ],
)


class TestBasicSingleTaskEligibilitySpansBigQueryViewBuilder(BigQueryEmulatorTestCase):
    """Tests for the BasicSingleTaskEligibilitySpansBigQueryViewBuilder."""

    def test_materialized_table_for_task_name(self) -> None:
        self.assertEqual(
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_xx",
                table_id="my_task_name__basic_materialized",
            ),
            BasicSingleTaskEligibilitySpansBigQueryViewBuilder.materialized_table_for_task_name(
                task_name=BASIC_ELIGIBILITY_VIEW_BUILDER.task_name,
                state_code=BASIC_ELIGIBILITY_VIEW_BUILDER.state_code,
            ),
        )

        self.assertEqual(
            BASIC_ELIGIBILITY_VIEW_BUILDER.materialized_address,
            BasicSingleTaskEligibilitySpansBigQueryViewBuilder.materialized_table_for_task_name(
                task_name=BASIC_ELIGIBILITY_VIEW_BUILDER.task_name,
                state_code=BASIC_ELIGIBILITY_VIEW_BUILDER.state_code,
            ),
        )

    def test_mismatched_criteria_state_codes(self) -> None:
        """Verify an error is raised if the task state code does not match one of the criteria"""
        us_yy_criteria = StateSpecificTaskCriteriaBigQueryViewBuilder(
            state_code=StateCode.US_YY,
            criteria_name="US_YY_SIMPLE_CRITERIA",
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
        with self.assertRaises(ValueError):
            BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                task_name="my_task_name",
                candidate_population_view_builder=TEST_POPULATION_BUILDER,
                criteria_spans_view_builders=[
                    us_yy_criteria,
                ],
            )

    def test_mismatched_candidate_population_state_codes(self) -> None:
        """Verify an error is raised if the task state code does not match one of the criteria"""
        us_yy_candidate_population = StateSpecificTaskCandidatePopulationBigQueryViewBuilder(
            state_code=StateCode.US_YY,
            population_name="US_YY_SIMPLE_POPULATION",
            population_spans_query_template="SELECT * FROM `{project_id}.test_dataset.foo`;",
            description="Simple population description",
        )
        with self.assertRaises(ValueError):
            BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                task_name="my_task_name",
                candidate_population_view_builder=us_yy_candidate_population,
                criteria_spans_view_builders=[
                    TEST_CRITERIA_BUILDER_1,
                ],
            )

    def test_simple_tes_query(self) -> None:
        """
        Verify that the Basic Task Eligibility Spans view can properly handle a client
        that goes from ineligible to eligible for a single criteria
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

        self.run_query_test(
            query_str=BASIC_ELIGIBILITY_VIEW_BUILDER.build().view_query,
            expected_result=[
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
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "reason_text"},
                        }
                    ],
                    "ineligible_criteria": [TEST_CRITERIA_BUILDER_1.criteria_name],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 3, 18),
                    "is_eligible": False,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 18),
                    "end_date": date(2024, 4, 8),
                    "is_eligible": True,
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
                },
            ],
        )

    def test_tes_query_covers_candidate_population(self) -> None:
        """Test that the Basic TES spans cover the full candidate population time period:
        - The eligibility span does not start before the candidate population start date
        - The criteria default is used for periods during the population span when the criteria span is missing
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
        tes_view_builder = BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[
                meets_criteria_default_true_view_builder,
            ],
        )

        load_data_for_task_criteria_view(
            emulator=self,
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
        load_data_for_candidate_population_view(
            emulator=self,
            population_date_spans=[
                (date(2024, 1, 1), date(2024, 3, 6)),
            ],
        )
        self.run_query_test(
            query_str=tes_view_builder.build().view_query,
            expected_result=[
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

    def test_tes_query_final_open_span(self) -> None:
        """
        Test the Basic Task Eligibility Spans view for a client with an open eligibility span
        (end_date is NULL) and two eligibility criteria
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
                    "end_date": None,
                    "meets_criteria": True,
                    "reason": {"test_reason": "eligible_reason"},
                    "reason_v2": {"test_reason": "eligible_reason"},
                },
            ],
        )
        load_data_for_task_criteria_view(
            emulator=self,
            criteria_view_builder=TEST_CRITERIA_BUILDER_4,
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
        load_data_for_candidate_population_view(
            emulator=self,
            population_date_spans=[
                (date(2024, 1, 1), None),
            ],
        )
        tes_query_builder = BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[
                TEST_CRITERIA_BUILDER_1,
                TEST_CRITERIA_BUILDER_4,
            ],
        )
        self.run_query_test(
            query_str=tes_query_builder.build().view_query,
            expected_result=[
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

    def test_non_adjacent_population_spans_with_two_person_ids(self) -> None:
        """
        Verify basic eligibility spans are computed per person and cover the entire candidate population span
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
        tes_query_builder = BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[
                TEST_CRITERIA_BUILDER_1,
            ],
        )

        self.run_query_test(
            query_str=tes_query_builder.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 6),
                    "is_eligible": False,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 3, 18),
                    "is_eligible": False,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 8),
                    "end_date": date(2024, 5, 15),
                    "is_eligible": False,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 23456,
                    "start_date": date(2024, 1, 28),
                    "end_date": date(2024, 3, 6),
                    "is_eligible": False,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 23456,
                    "start_date": date(2024, 3, 28),
                    "end_date": date(2024, 4, 4),
                    "is_eligible": False,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 23456,
                    "start_date": date(2024, 4, 4),
                    "end_date": None,
                    "is_eligible": False,
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
                },
            ],
        )

    def test_policy_start_date_turns_eligibility_false_before_date(self) -> None:
        """Test that spans starting before policy_start_date have is_eligible=False.

        This test sets up criteria data with a boundary at the policy date to verify
        that eligibility is correctly set based on the policy date check.
        """
        load_data_for_task_criteria_view(
            emulator=self,
            criteria_view_builder=TEST_CRITERIA_BUILDER_1,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 3, 1),
                    "meets_criteria": True,
                    "reason": {"test_reason": "before_start"},
                    "reason_v2": {"test_reason": "before_start"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2024, 4, 1),
                    "meets_criteria": True,
                    "reason": {"test_reason": "within_range"},
                    "reason_v2": {"test_reason": "within_range"},
                },
            ],
        )
        load_data_for_candidate_population_view(
            emulator=self, population_date_spans=[(date(2024, 1, 1), date(2024, 4, 1))]
        )

        tes_query_builder_with_policy_start = (
            BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                task_name="my_task_name",
                candidate_population_view_builder=TEST_POPULATION_BUILDER,
                criteria_spans_view_builders=[TEST_CRITERIA_BUILDER_1],
                policy_start_date=date(2024, 3, 1),
            )
        )

        self.run_query_test(
            query_str=tes_query_builder_with_policy_start.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 3, 1),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "before_start"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "before_start"},
                        }
                    ],
                    "ineligible_criteria": [],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2024, 4, 1),
                    "is_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "within_range"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "within_range"},
                        }
                    ],
                    "ineligible_criteria": [],
                },
            ],
        )

    def test_policy_end_date_turns_eligibility_false_after_date(self) -> None:
        """Test that spans starting after policy_end_date have is_eligible=False.

        policy_end_date is inclusive, so spans starting ON that date are still eligible.
        """
        load_data_for_task_criteria_view(
            emulator=self,
            criteria_view_builder=TEST_CRITERIA_BUILDER_1,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 1),
                    "meets_criteria": True,
                    "reason": {"test_reason": "before_end"},
                    "reason_v2": {"test_reason": "before_end"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 1),
                    "end_date": date(2024, 3, 1),
                    "meets_criteria": True,
                    "reason": {"test_reason": "at_end"},
                    "reason_v2": {"test_reason": "at_end"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2024, 4, 1),
                    "meets_criteria": True,
                    "reason": {"test_reason": "after_end"},
                    "reason_v2": {"test_reason": "after_end"},
                },
            ],
        )
        load_data_for_candidate_population_view(
            emulator=self, population_date_spans=[(date(2024, 1, 1), date(2024, 4, 1))]
        )

        tes_query_builder_with_policy_end = (
            BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                task_name="my_task_name",
                candidate_population_view_builder=TEST_POPULATION_BUILDER,
                criteria_spans_view_builders=[TEST_CRITERIA_BUILDER_1],
                policy_end_date=date(2024, 2, 1),
            )
        )

        self.run_query_test(
            query_str=tes_query_builder_with_policy_end.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 1),
                    "is_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "before_end"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "before_end"},
                        }
                    ],
                    "ineligible_criteria": [],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 1),
                    "end_date": date(2024, 3, 1),
                    "is_eligible": True,  # inclusive: Feb 1 <= Feb 1
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "at_end"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "at_end"},
                        }
                    ],
                    "ineligible_criteria": [],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2024, 4, 1),
                    "is_eligible": False,  # Mar 1 > Feb 1
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "after_end"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "after_end"},
                        }
                    ],
                    "ineligible_criteria": [],
                },
            ],
        )

    def test_policy_date_range_both_dates(self) -> None:
        """Test that spans are only eligible within the policy date range.

        Both policy_start_date and policy_end_date are inclusive.
        """
        load_data_for_task_criteria_view(
            emulator=self,
            criteria_view_builder=TEST_CRITERIA_BUILDER_1,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 1),
                    "meets_criteria": True,
                    "reason": {"test_reason": "before_start"},
                    "reason_v2": {"test_reason": "before_start"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 1),
                    "end_date": date(2024, 3, 1),
                    "meets_criteria": True,
                    "reason": {"test_reason": "at_start"},
                    "reason_v2": {"test_reason": "at_start"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2024, 4, 1),
                    "meets_criteria": True,
                    "reason": {"test_reason": "at_end"},
                    "reason_v2": {"test_reason": "at_end"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 1),
                    "end_date": date(2024, 6, 1),
                    "meets_criteria": True,
                    "reason": {"test_reason": "after_end"},
                    "reason_v2": {"test_reason": "after_end"},
                },
            ],
        )
        load_data_for_candidate_population_view(
            emulator=self, population_date_spans=[(date(2024, 1, 1), date(2024, 6, 1))]
        )

        tes_query_builder_with_policy_range = (
            BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                task_name="my_task_name",
                candidate_population_view_builder=TEST_POPULATION_BUILDER,
                criteria_spans_view_builders=[TEST_CRITERIA_BUILDER_1],
                policy_start_date=date(2024, 2, 1),
                policy_end_date=date(2024, 3, 1),
            )
        )

        self.run_query_test(
            query_str=tes_query_builder_with_policy_range.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 1),
                    "is_eligible": False,  # Jan 1 < Feb 1
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "before_start"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "before_start"},
                        }
                    ],
                    "ineligible_criteria": [],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 1),
                    "end_date": date(2024, 3, 1),
                    "is_eligible": True,  # Feb 1 >= Feb 1 AND Feb 1 <= Mar 1
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "at_start"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "at_start"},
                        }
                    ],
                    "ineligible_criteria": [],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2024, 4, 1),
                    "is_eligible": True,  # inclusive: Mar 1 <= Mar 1
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "at_end"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "at_end"},
                        }
                    ],
                    "ineligible_criteria": [],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 1),
                    "end_date": date(2024, 6, 1),
                    "is_eligible": False,  # Apr 1 > Mar 1
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "after_end"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "after_end"},
                        }
                    ],
                    "ineligible_criteria": [],
                },
            ],
        )

    def test_policy_date_splits_span_at_boundary(self) -> None:
        """Test that spans are split at policy date boundaries.

        This test verifies that when a criteria span crosses a policy date boundary,
        it gets split into separate eligibility spans - one before the policy date
        (ineligible) and one after (eligible for policy_start_date, or vice versa
        for policy_end_date).
        """
        # Single criteria span that crosses the policy_start_date boundary
        load_data_for_task_criteria_view(
            emulator=self,
            criteria_view_builder=TEST_CRITERIA_BUILDER_1,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 6, 1),
                    "meets_criteria": True,
                    "reason": {"test_reason": "eligible"},
                    "reason_v2": {"test_reason": "eligible"},
                },
            ],
        )
        load_data_for_candidate_population_view(
            emulator=self, population_date_spans=[(date(2024, 1, 1), date(2024, 6, 1))]
        )

        # Policy start date is 2024-03-01, which is in the middle of the criteria span
        tes_query_builder_with_policy_start = (
            BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                task_name="my_task_name",
                candidate_population_view_builder=TEST_POPULATION_BUILDER,
                criteria_spans_view_builders=[TEST_CRITERIA_BUILDER_1],
                policy_start_date=date(2024, 3, 1),
            )
        )

        # The span should be split at 2024-03-01:
        # - Before: 2024-01-01 to 2024-03-01, is_eligible=False (before policy start)
        # - After: 2024-03-01 to 2024-06-01, is_eligible=True (within policy range)
        self.run_query_test(
            query_str=tes_query_builder_with_policy_start.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 3, 1),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible"},
                        }
                    ],
                    "ineligible_criteria": [],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 1),
                    "end_date": date(2024, 6, 1),
                    "is_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible"},
                        }
                    ],
                    "ineligible_criteria": [],
                },
            ],
        )

    def test_policy_date_range_splits_span_at_both_boundaries(self) -> None:
        """Test that spans are split at both policy start and end date boundaries.

        This test verifies that when a criteria span crosses both policy date
        boundaries, it gets split into three separate eligibility spans.
        Both policy_start_date and policy_end_date are inclusive.
        """
        # Single criteria span that crosses both policy date boundaries
        load_data_for_task_criteria_view(
            emulator=self,
            criteria_view_builder=TEST_CRITERIA_BUILDER_1,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 6, 1),
                    "meets_criteria": True,
                    "reason": {"test_reason": "eligible"},
                    "reason_v2": {"test_reason": "eligible"},
                },
            ],
        )
        load_data_for_candidate_population_view(
            emulator=self, population_date_spans=[(date(2024, 1, 1), date(2024, 6, 1))]
        )

        # Policy range is 2024-02-01 to 2024-03-31 (inclusive), both in the middle of the span
        tes_query_builder_with_policy_range = (
            BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                task_name="my_task_name",
                candidate_population_view_builder=TEST_POPULATION_BUILDER,
                criteria_spans_view_builders=[TEST_CRITERIA_BUILDER_1],
                policy_start_date=date(2024, 2, 1),
                policy_end_date=date(2024, 3, 31),
            )
        )

        # The span should be split at both policy dates:
        # - Before: 2024-01-01 to 2024-02-01, is_eligible=False (before policy start)
        # - Within: 2024-02-01 to 2024-03-31, is_eligible=True (within policy range, inclusive)
        # - After: 2024-03-31 to 2024-06-01, is_eligible=True (starts on policy end date, inclusive)
        self.run_query_test(
            query_str=tes_query_builder_with_policy_range.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 1),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible"},
                        }
                    ],
                    "ineligible_criteria": [],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 1),
                    "end_date": date(2024, 3, 31),
                    "is_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible"},
                        }
                    ],
                    "ineligible_criteria": [],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 31),
                    "end_date": date(2024, 6, 1),
                    "is_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible"},
                        }
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_1.criteria_name,
                            "reason": {"test_reason": "eligible"},
                        }
                    ],
                    "ineligible_criteria": [],
                },
            ],
        )
