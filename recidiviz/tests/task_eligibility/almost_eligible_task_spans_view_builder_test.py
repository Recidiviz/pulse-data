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
"""Tests for the AlmostEligibleSpansBigQueryViewBuilder."""
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
    EligibleCriteriaCondition,
    LessThanOrEqualCriteriaCondition,
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    ReasonDateInCalendarWeekCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateSpecificTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    StateAgnosticTaskCriteriaGroupBigQueryViewBuilder,
    TaskCriteriaGroupLogicType,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.task_eligibility.basic_single_task_eligibility_spans_big_query_view_builder_test import (
    BASIC_ELIGIBILITY_VIEW_BUILDER,
)
from recidiviz.tests.task_eligibility.single_task_eligibility_spans_view_builder_test import (
    TEST_CRITERIA_BUILDER_1,
    TEST_CRITERIA_BUILDER_2,
    TEST_CRITERIA_BUILDER_3,
    TEST_CRITERIA_BUILDER_4,
    TEST_POPULATION_BUILDER,
)
from recidiviz.tests.task_eligibility.task_eligibility_big_query_emulator_utils import (
    load_data_for_basic_task_eligibility_span_view,
)

ALMOST_ELIGIBLE_VIEW_BUILDER = AlmostEligibleSpansBigQueryViewBuilder(
    basic_tes_view_builder=BASIC_ELIGIBILITY_VIEW_BUILDER,
    almost_eligible_condition=NotEligibleCriteriaCondition(
        criteria=TEST_CRITERIA_BUILDER_1,
        description="Not eligible criteria condition",
    ),
)


class TestAlmostEligibleSpansBigQueryViewBuilder(BigQueryEmulatorTestCase):
    """Tests for the AlmostEligibleSpansBigQueryViewBuilder."""

    def test_materialized_table_for_task_name(self) -> None:
        self.assertEqual(
            BigQueryAddress(
                dataset_id="task_eligibility_spans_us_xx",
                table_id="my_task_name__with_is_almost_eligible_materialized",
            ),
            AlmostEligibleSpansBigQueryViewBuilder.materialized_table_for_task_name(
                task_name=ALMOST_ELIGIBLE_VIEW_BUILDER.task_name,
                state_code=ALMOST_ELIGIBLE_VIEW_BUILDER.state_code,
            ),
        )

        self.assertEqual(
            ALMOST_ELIGIBLE_VIEW_BUILDER.materialized_address,
            AlmostEligibleSpansBigQueryViewBuilder.materialized_table_for_task_name(
                task_name=ALMOST_ELIGIBLE_VIEW_BUILDER.task_name,
                state_code=ALMOST_ELIGIBLE_VIEW_BUILDER.state_code,
            ),
        )

    def test_almost_eligible_condition_nested_in_criteria_list(self) -> None:
        """
        Verify an error is raised if the almost eligible condition is applied to a criteria that is not in the
        top-level criteria list
        """
        basic_tes_view_builder = BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[
                StateAgnosticTaskCriteriaGroupBigQueryViewBuilder(
                    logic_type=TaskCriteriaGroupLogicType.AND,
                    sub_criteria_list=[
                        TEST_CRITERIA_BUILDER_1,
                        TEST_CRITERIA_BUILDER_2,
                    ],
                    criteria_name="AND_TASK_CRITERIA_GROUP",
                    allowed_duplicate_reasons_keys=[],
                ),
            ],
        )
        almost_eligible_condition = NotEligibleCriteriaCondition(
            criteria=TEST_CRITERIA_BUILDER_1,
            description="Criteria 1 is not met",
        )
        with self.assertRaises(ValueError):
            AlmostEligibleSpansBigQueryViewBuilder(
                basic_tes_view_builder=basic_tes_view_builder,
                almost_eligible_condition=almost_eligible_condition,
            )

    def test_simple_almost_eligible_time_condition(self) -> None:
        """Test simple time dependent almost eligible condition on top of a basic eligibility span"""
        basic_tes_view_builder = BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[
                TEST_CRITERIA_BUILDER_3,
            ],
        )
        load_data_for_basic_task_eligibility_span_view(
            emulator=self,
            basic_eligibility_address=basic_tes_view_builder.table_for_query,
            basic_eligibility_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 3, 18),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_3.criteria_name,
                    ],
                },
                # This eligible span should get dropped by the Almost Eligible builder
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 18),
                    "end_date": date(2024, 4, 8),
                    "is_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {"test_reason_date": "2024-03-18"},
                        },
                    ],
                    "ineligible_criteria": [],
                },
            ],
        )
        almost_eligible_builder = AlmostEligibleSpansBigQueryViewBuilder(
            basic_tes_view_builder=basic_tes_view_builder,
            almost_eligible_condition=TimeDependentCriteriaCondition(
                criteria=TEST_CRITERIA_BUILDER_3,
                reasons_date_field="test_reason_date",
                interval_length=1,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Within 1 month of the test reason date",
            ),
        )

        self.run_query_test(
            query_str=almost_eligible_builder.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 18),
                    "is_almost_eligible": False,
                },
                # Start an almost eligible span 1 month before the reason date
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 18),
                    "end_date": date(2024, 3, 18),
                    "is_almost_eligible": True,
                },
            ],
        )

    def test_almost_eligible_if_x_or_y(self) -> None:
        """
        Test the composite criteria condition for inclusive OR logic (Criteria_X OR Criteria_Y)
        """
        basic_tes_view_builder = BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[
                TEST_CRITERIA_BUILDER_1,
                TEST_CRITERIA_BUILDER_2,
            ],
        )
        load_data_for_basic_task_eligibility_span_view(
            emulator=self,
            basic_eligibility_address=basic_tes_view_builder.table_for_query,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 2, 15),
                    "is_eligible": False,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 15),
                    "end_date": date(2024, 3, 28),
                    "is_eligible": False,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 28),
                    "end_date": date(2024, 4, 8),
                    "is_eligible": True,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 8),
                    "end_date": None,
                    "is_eligible": False,
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
                },
            ],
        )
        almost_eligible_builder = AlmostEligibleSpansBigQueryViewBuilder(
            basic_tes_view_builder=basic_tes_view_builder,
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
            query_str=almost_eligible_builder.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 6),
                    "is_almost_eligible": True,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 2, 15),
                    "is_almost_eligible": False,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 15),
                    "end_date": date(2024, 3, 28),
                    "is_almost_eligible": True,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 8),
                    "end_date": None,
                    "is_almost_eligible": True,
                },
            ],
        )

    def test_almost_eligible_if_x_xor_y_and_z(self) -> None:
        """
        Test three criteria used in a nested composite criteria condition (Criteria_X XOR (Criteria_Y AND Criteria_Z))
        """
        basic_tes_view_builder = BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[
                TEST_CRITERIA_BUILDER_1,
                TEST_CRITERIA_BUILDER_2,
                TEST_CRITERIA_BUILDER_3,
            ],
        )
        load_data_for_basic_task_eligibility_span_view(
            emulator=self,
            basic_eligibility_address=basic_tes_view_builder.table_for_query,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 2, 15),
                    "is_eligible": False,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 15),
                    "end_date": date(2024, 3, 17),
                    "is_eligible": False,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 17),
                    "end_date": date(2024, 3, 18),
                    "is_eligible": False,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 18),
                    "end_date": date(2024, 3, 28),
                    "is_eligible": False,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 28),
                    "end_date": date(2024, 4, 8),
                    "is_eligible": True,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 8),
                    "end_date": None,
                    "is_eligible": False,
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
                },
            ],
        )
        almost_eligible_builder = AlmostEligibleSpansBigQueryViewBuilder(
            basic_tes_view_builder=basic_tes_view_builder,
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
            query_str=almost_eligible_builder.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 3, 17),
                    "is_almost_eligible": False,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 17),
                    "end_date": date(2024, 3, 18),
                    "is_almost_eligible": True,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 18),
                    "end_date": date(2024, 3, 28),
                    "is_almost_eligible": False,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 8),
                    "end_date": None,
                    "is_almost_eligible": True,
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
        basic_tes_view_builder = BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[two_date_criteria_view_builder],
        )

        almost_eligible_builder = AlmostEligibleSpansBigQueryViewBuilder(
            basic_tes_view_builder=basic_tes_view_builder,
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
        # Load basic eligibility spans that do not align with the time dependent
        # criteria to test that the almost eligible spans are split on the
        # correct almost eligible "critical date"
        load_data_for_basic_task_eligibility_span_view(
            emulator=self,
            basic_eligibility_address=basic_tes_view_builder.table_for_query,
            basic_eligibility_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 28),
                    "is_eligible": False,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 28),
                    "end_date": date(2024, 3, 27),
                    "is_eligible": False,
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
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "task_eligibility_span_id": 5,
                    "start_date": date(2024, 3, 27),
                    "end_date": None,
                    "is_eligible": False,
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
                },
            ],
        )
        # Test the criteria spans are split on the critical date boundaries correctly
        if almost_eligible_builder.almost_eligible_condition is None:
            raise ValueError(
                "AlmostEligibleSpansBigQueryViewBuilder must have the `almost_eligible_condition` attribute"
                " populated when almost eligible conditions are supplied."
            )

        self.run_query_test(
            query_str=almost_eligible_builder.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 15),
                    "is_almost_eligible": False,
                },
                # Span becomes almost eligible 3 months before "second_reason_date": "2024-05-15"
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 15),
                    "end_date": date(2024, 2, 28),
                    "is_almost_eligible": True,
                },
                # "second_reason_date" changes, span becomes fully ineligible
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 28),
                    "end_date": date(2024, 3, 13),
                    "is_almost_eligible": False,
                },
                # Span becomes almost eligible 2 weeks before "first_reason_date": "2024-03-27"
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 13),
                    "end_date": date(2024, 3, 27),
                    "is_almost_eligible": True,
                },
                # "first_reason_date" changes to null, span becomes fully ineligible
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 27),
                    "end_date": date(2024, 4, 15),
                    "is_almost_eligible": False,
                },
                # Span becomes almost eligible 3 months before "second_reason_date": "2024-07-15",
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 15),
                    "end_date": None,
                    "is_almost_eligible": True,
                },
            ],
        )

    def test_two_conditions_time_until_fully_eligible(
        self,
    ) -> None:
        """
        Test the Almost Eligible Spans view for a task with two time dependent criteria conditions applied with
        EligibleCriteriaCondition objects to represent "almost eligible if less than 2 weeks away being fully eligible"
        """
        basic_tes_view_builder = BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[
                TEST_CRITERIA_BUILDER_3,
                TEST_CRITERIA_BUILDER_4,
            ],
        )

        load_data_for_basic_task_eligibility_span_view(
            emulator=self,
            basic_eligibility_address=basic_tes_view_builder.table_for_query,
            basic_eligibility_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 3, 4),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-18",
                            },
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-10",
                            },
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-18",
                            },
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-10",
                            },
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_3.criteria_name,
                        TEST_CRITERIA_BUILDER_4.criteria_name,
                    ],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 4),
                    "end_date": date(2024, 3, 10),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-18",
                            },
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-10",
                            },
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-18",
                            },
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-10",
                            },
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_3.criteria_name,
                        TEST_CRITERIA_BUILDER_4.criteria_name,
                    ],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 10),
                    "end_date": date(2024, 3, 12),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-18",
                            },
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-10",
                            },
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-18",
                            },
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-10",
                            },
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_3.criteria_name,
                    ],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 12),
                    "end_date": date(2024, 3, 18),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-18",
                            },
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {
                                "test_reason_date": None,
                            },
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-18",
                            },
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {
                                "test_reason_date": None,
                            },
                        },
                    ],
                    "ineligible_criteria": [TEST_CRITERIA_BUILDER_3.criteria_name],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 18),
                    "end_date": None,
                    "is_eligible": True,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-18",
                            },
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {
                                "test_reason_date": None,
                            },
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                            "reason": {
                                "test_reason_date": "2024-03-18",
                            },
                        },
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_4.criteria_name,
                            "reason": {
                                "test_reason_date": None,
                            },
                        },
                    ],
                    "ineligible_criteria": [],
                },
            ],
        )
        almost_eligible_builder = AlmostEligibleSpansBigQueryViewBuilder(
            basic_tes_view_builder=basic_tes_view_builder,
            almost_eligible_condition=PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    PickNCompositeCriteriaCondition(
                        sub_conditions_list=[
                            TimeDependentCriteriaCondition(
                                criteria=TEST_CRITERIA_BUILDER_3,
                                reasons_date_field="test_reason_date",
                                interval_length=2,
                                interval_date_part=BigQueryDateInterval.WEEK,
                                description="Within 2 weeks of first reason date",
                            ),
                            EligibleCriteriaCondition(
                                criteria=TEST_CRITERIA_BUILDER_3,
                                description="Criteria 3 is met",
                            ),
                        ],
                        at_least_n_conditions_true=1,
                    ),
                    PickNCompositeCriteriaCondition(
                        sub_conditions_list=[
                            TimeDependentCriteriaCondition(
                                criteria=TEST_CRITERIA_BUILDER_4,
                                reasons_date_field="test_reason_date",
                                interval_length=2,
                                interval_date_part=BigQueryDateInterval.WEEK,
                                description="Within 2 weeks of second reason date",
                            ),
                            EligibleCriteriaCondition(
                                criteria=TEST_CRITERIA_BUILDER_4,
                                description="Criteria 4 is met",
                            ),
                        ],
                        at_least_n_conditions_true=1,
                    ),
                ],
                at_least_n_conditions_true=2,
            ),
        )

        self.run_query_test(
            query_str=almost_eligible_builder.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 3, 4),
                    "is_almost_eligible": False,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 4),
                    "end_date": date(2024, 3, 18),
                    "is_almost_eligible": True,
                },
            ],
        )

    def test_non_adjacent_with_two_person_ids(self) -> None:
        """
        Verify almost eligible spans are computed per person
        """
        basic_tes_view_builder = BasicSingleTaskEligibilitySpansBigQueryViewBuilder(
            state_code=StateCode.US_XX,
            task_name="my_task_name",
            candidate_population_view_builder=TEST_POPULATION_BUILDER,
            criteria_spans_view_builders=[
                TEST_CRITERIA_BUILDER_2,
            ],
        )
        load_data_for_basic_task_eligibility_span_view(
            emulator=self,
            basic_eligibility_address=basic_tes_view_builder.table_for_query,
            basic_eligibility_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 6),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": None,
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": None,
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_2.criteria_name,
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
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": None},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": None},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_2.criteria_name,
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
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 50},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 50},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_2.criteria_name,
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
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 150},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 150},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_2.criteria_name,
                    ],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 23456,
                    "task_eligibility_span_id": 2,
                    "start_date": date(2024, 3, 28),
                    "end_date": date(2024, 4, 4),
                    "is_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 100},
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": {"test_reason_int": 100},
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_2.criteria_name,
                    ],
                },
                {
                    "state_code": "US_XX",
                    "person_id": 23456,
                    "start_date": date(2024, 4, 4),
                    "end_date": None,
                    "is_eligible": False,
                    "is_almost_eligible": False,
                    "reasons": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": None,
                        },
                    ],
                    "reasons_v2": [
                        {
                            "criteria_name": TEST_CRITERIA_BUILDER_2.criteria_name,
                            "reason": None,
                        },
                    ],
                    "ineligible_criteria": [
                        TEST_CRITERIA_BUILDER_2.criteria_name,
                    ],
                },
            ],
        )
        almost_eligible_builder = AlmostEligibleSpansBigQueryViewBuilder(
            basic_tes_view_builder=basic_tes_view_builder,
            almost_eligible_condition=LessThanOrEqualCriteriaCondition(
                criteria=TEST_CRITERIA_BUILDER_2,
                reasons_numerical_field="test_reason_int",
                value=100,
                description="Reason value less than or equal to 100",
            ),
        )

        self.run_query_test(
            query_str=almost_eligible_builder.build().view_query,
            expected_result=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 3, 18),
                    "is_almost_eligible": False,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 8),
                    "end_date": date(2024, 5, 15),
                    "is_almost_eligible": True,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 23456,
                    "start_date": date(2024, 1, 28),
                    "end_date": date(2024, 3, 6),
                    "is_almost_eligible": False,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 23456,
                    "start_date": date(2024, 3, 28),
                    "end_date": date(2024, 4, 4),
                    "is_almost_eligible": True,
                },
                {
                    "state_code": "US_XX",
                    "person_id": 23456,
                    "start_date": date(2024, 4, 4),
                    "end_date": None,
                    "is_almost_eligible": False,
                },
            ],
        )
