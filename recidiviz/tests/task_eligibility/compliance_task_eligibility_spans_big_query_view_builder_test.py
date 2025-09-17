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
"""Tests for the ComplianceTaskEligibilitySpansBigQueryViewBuilder."""
import re
from datetime import date

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.state.views.workflows.firestore.task_config import (
    ComplianceType,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.task_eligibility.compliance_task_eligibility_spans_big_query_view_builder import (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.task_eligibility.single_task_eligibility_spans_view_builder_test import (
    TEST_CRITERIA_BUILDER_1,
    TEST_CRITERIA_BUILDER_3,
    TEST_POPULATION_BUILDER,
)
from recidiviz.tests.task_eligibility.task_eligibility_big_query_emulator_utils import (
    load_data_for_candidate_population_view,
    load_data_for_task_criteria_view,
)

COMPLIANCE_ELIGIBILITY_VIEW_BUILDER = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
    state_code=StateCode.US_XX,
    task_name="my_task_name",
    candidate_population_view_builder=TEST_POPULATION_BUILDER,
    criteria_spans_view_builders=[
        TEST_CRITERIA_BUILDER_3,
    ],
    compliance_type=ComplianceType.ASSESSMENT,
    due_date_field="test_reason_date",
    due_date_criteria_builder=TEST_CRITERIA_BUILDER_3,
)

COMPLIANCE_ELIGIBILITY_VIEW_BUILDER_NO_DUE_DATE_CRITERIA = (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder(
        state_code=StateCode.US_XX,
        task_name="my_task_name",
        candidate_population_view_builder=TEST_POPULATION_BUILDER,
        criteria_spans_view_builders=[
            TEST_CRITERIA_BUILDER_3,
        ],
        compliance_type=ComplianceType.ASSESSMENT,
        due_date_field="test_reason_date",
    )
)


class TestComplianceTaskEligibilitySpansBigQueryViewBuilder(BigQueryEmulatorTestCase):
    """Tests for the ComplianceTaskEligibilitySpansBigQueryViewBuilder."""

    def test_materialized_table_for_task_name(self) -> None:
        """Test that materialized address generates as expected for compliance task eligibility spans"""
        self.assertEqual(
            BigQueryAddress(
                dataset_id="compliance_task_eligibility_spans_us_xx",
                table_id="my_task_name_materialized",
            ),
            ComplianceTaskEligibilitySpansBigQueryViewBuilder.materialized_table_for_task_name(
                task_name=COMPLIANCE_ELIGIBILITY_VIEW_BUILDER.task_name,
                state_code=COMPLIANCE_ELIGIBILITY_VIEW_BUILDER.state_code,
            ),
        )

        self.assertEqual(
            COMPLIANCE_ELIGIBILITY_VIEW_BUILDER.materialized_address,
            ComplianceTaskEligibilitySpansBigQueryViewBuilder.materialized_table_for_task_name(
                task_name=COMPLIANCE_ELIGIBILITY_VIEW_BUILDER.task_name,
                state_code=COMPLIANCE_ELIGIBILITY_VIEW_BUILDER.state_code,
            ),
        )

    def test_raises_error_when_due_date_field_missing_in_criteria_reasons(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Criteria SIMPLE_CRITERIA has no reason field named test_reason_date"
            ),
        ):
            _ = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                task_name="my_task_name",
                candidate_population_view_builder=TEST_POPULATION_BUILDER,
                criteria_spans_view_builders=[
                    TEST_CRITERIA_BUILDER_1,
                ],
                compliance_type=ComplianceType.ASSESSMENT,
                due_date_field="test_reason_date",
                due_date_criteria_builder=TEST_CRITERIA_BUILDER_1,
            )

    def test_raises_error_when_due_date_criteria_missing_in_criteria_builders(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "The due_date_criteria_builder US_XX_SIMPLE_CRITERIA not found among criteria_spans_view_builders."
            ),
        ):
            _ = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                task_name="my_task_name",
                candidate_population_view_builder=TEST_POPULATION_BUILDER,
                criteria_spans_view_builders=[
                    TEST_CRITERIA_BUILDER_1,
                ],
                compliance_type=ComplianceType.ASSESSMENT,
                due_date_field="test_reason_date",
                due_date_criteria_builder=TEST_CRITERIA_BUILDER_3,
            )

    def test_raises_error_when_due_date_criteria_not_provided_and_multiple_criteria_builders(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "Must specify due_date_criteria_builder when providing multiple criteria_spans_view_builders."
            ),
        ):
            _ = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                task_name="my_task_name",
                candidate_population_view_builder=TEST_POPULATION_BUILDER,
                criteria_spans_view_builders=[
                    TEST_CRITERIA_BUILDER_1,
                    TEST_CRITERIA_BUILDER_3,
                ],
                compliance_type=ComplianceType.ASSESSMENT,
                due_date_field="test_reason_date",
            )

    def test_simple_compliance_tes_query(self) -> None:
        """
        Verify that compliance task eligibility spans properly wraps basic task eligibility spans
        with additional attributes.
        """
        load_data_for_task_criteria_view(
            emulator=self,
            criteria_view_builder=TEST_CRITERIA_BUILDER_3,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 6),
                    "meets_criteria": False,
                    "reason": {"test_reason_date": "2024-02-08"},
                    "reason_v2": {"test_reason_date": "2024-02-08"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 3, 18),
                    "meets_criteria": False,
                    "reason": {"test_reason_date": "2024-04-03"},
                    "reason_v2": {"test_reason_date": "2024-04-03"},
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 18),
                    "end_date": date(2024, 4, 8),
                    "meets_criteria": True,
                    "reason": {"test_reason_date": "2024-04-10"},
                    "reason_v2": {"test_reason_date": "2024-04-10"},
                },
            ],
        )
        load_data_for_candidate_population_view(
            emulator=self, population_date_spans=[(date(2024, 1, 1), date(2024, 4, 8))]
        )

        expected_result = [
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "start_date": date(2024, 1, 1),
                "end_date": date(2024, 2, 6),
                "is_eligible": False,
                "reasons": [
                    {
                        "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                        "reason": {"test_reason_date": "2024-02-08"},
                    }
                ],
                "reasons_v2": [
                    {
                        "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                        "reason": {"test_reason_date": "2024-02-08"},
                    }
                ],
                "ineligible_criteria": [TEST_CRITERIA_BUILDER_3.criteria_name],
                "due_date": date(2024, 2, 8),
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "start_date": date(2024, 2, 6),
                "end_date": date(2024, 3, 18),
                "is_eligible": False,
                "reasons": [
                    {
                        "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                        "reason": {"test_reason_date": "2024-04-03"},
                    }
                ],
                "reasons_v2": [
                    {
                        "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                        "reason": {"test_reason_date": "2024-04-03"},
                    }
                ],
                "ineligible_criteria": [TEST_CRITERIA_BUILDER_3.criteria_name],
                "due_date": date(2024, 4, 3),
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "start_date": date(2024, 3, 18),
                "end_date": date(2024, 4, 8),
                "is_eligible": True,
                "reasons": [
                    {
                        "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                        "reason": {"test_reason_date": "2024-04-10"},
                    }
                ],
                "reasons_v2": [
                    {
                        "criteria_name": TEST_CRITERIA_BUILDER_3.criteria_name,
                        "reason": {"test_reason_date": "2024-04-10"},
                    }
                ],
                "ineligible_criteria": [],
                "due_date": date(2024, 4, 10),
            },
        ]

        self.run_query_test(
            query_str=COMPLIANCE_ELIGIBILITY_VIEW_BUILDER.build().view_query,
            expected_result=expected_result,
        )

        # Check that the output is the same when no due date criteria builder is specified
        self.run_query_test(
            query_str=COMPLIANCE_ELIGIBILITY_VIEW_BUILDER_NO_DUE_DATE_CRITERIA.build().view_query,
            expected_result=expected_result,
        )
