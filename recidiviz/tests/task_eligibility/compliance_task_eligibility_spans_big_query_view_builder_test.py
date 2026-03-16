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
from recidiviz.big_query.big_query_view_column import Bool, Date, Integer, Json, String
from recidiviz.calculator.query.state.views.tasks.compliance_type import (
    CadenceType,
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
    TEST_CRITERIA_BUILDER_5,
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
        TEST_CRITERIA_BUILDER_5,
    ],
    compliance_type=ComplianceType.ASSESSMENT,
    cadence_type=CadenceType.RECURRING_FIXED,
    due_date_field="test_reason_date",
    due_date_criteria_builder=TEST_CRITERIA_BUILDER_5,
    last_task_completed_date_field="last_contacted_date",
    last_task_completed_date_criteria_builder=TEST_CRITERIA_BUILDER_5,
)

ROLLING_COMPLIANCE_ELIGIBILITY_VIEW_BUILDER = (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder(
        state_code=StateCode.US_XX,
        task_name="my_rolling_task",
        candidate_population_view_builder=TEST_POPULATION_BUILDER,
        criteria_spans_view_builders=[
            TEST_CRITERIA_BUILDER_5,
        ],
        compliance_type=ComplianceType.CONTACT,
        cadence_type=CadenceType.RECURRING_ROLLING,
        due_date_field="test_reason_date",
        last_task_completed_date_field="last_contacted_date",
    )
)

COMPLIANCE_ELIGIBILITY_VIEW_BUILDER_NO_DUE_DATE_CRITERIA = (
    ComplianceTaskEligibilitySpansBigQueryViewBuilder(
        state_code=StateCode.US_XX,
        task_name="my_task_name",
        candidate_population_view_builder=TEST_POPULATION_BUILDER,
        criteria_spans_view_builders=[
            TEST_CRITERIA_BUILDER_5,
        ],
        compliance_type=ComplianceType.ASSESSMENT,
        cadence_type=CadenceType.RECURRING_FIXED,
        due_date_field="test_reason_date",
        last_task_completed_date_field="last_contacted_date",
    )
)


class TestComplianceTaskEligibilitySpansBigQueryViewBuilder(BigQueryEmulatorTestCase):
    """Tests for the ComplianceTaskEligibilitySpansBigQueryViewBuilder."""

    def test_schema(self) -> None:
        """Test that the compliance builder schema includes due_date, last_task_completed_date, and is_overdue."""
        schema = COMPLIANCE_ELIGIBILITY_VIEW_BUILDER.schema
        self.assertIsNotNone(schema)
        assert schema is not None
        self.assertEqual(
            [(type(col), col.name, col.mode) for col in schema],
            [
                (String, "state_code", "REQUIRED"),
                (Integer, "person_id", "REQUIRED"),
                (Date, "start_date", "REQUIRED"),
                (Date, "end_date", "NULLABLE"),
                (Bool, "is_eligible", "REQUIRED"),
                (Json, "reasons", "NULLABLE"),
                (Json, "reasons_v2", "REQUIRED"),
                (String, "ineligible_criteria", "REPEATED"),
                (Date, "due_date", "NULLABLE"),
                (Date, "last_task_completed_date", "NULLABLE"),
                (Bool, "is_overdue", "REQUIRED"),
            ],
        )

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
                    TEST_CRITERIA_BUILDER_5,
                ],
                compliance_type=ComplianceType.ASSESSMENT,
                cadence_type=CadenceType.RECURRING_FIXED,
                due_date_field="test_reason_date",
                due_date_criteria_builder=TEST_CRITERIA_BUILDER_1,
                last_task_completed_date_field="last_contacted_date",
                last_task_completed_date_criteria_builder=TEST_CRITERIA_BUILDER_5,
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
                    TEST_CRITERIA_BUILDER_5,
                ],
                compliance_type=ComplianceType.ASSESSMENT,
                cadence_type=CadenceType.RECURRING_FIXED,
                due_date_field="test_reason_date",
                due_date_criteria_builder=TEST_CRITERIA_BUILDER_3,
                last_task_completed_date_field="last_contacted_date",
                last_task_completed_date_criteria_builder=TEST_CRITERIA_BUILDER_1,
            )

    def test_raises_error_when_last_task_completed_date_criteria_missing_in_criteria_builders(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            re.escape(
                "The last_task_completed_date_criteria_builder US_XX_SIMPLE_CRITERIA not found among criteria_spans_view_builders."
            ),
        ):
            _ = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                task_name="my_task_name",
                candidate_population_view_builder=TEST_POPULATION_BUILDER,
                criteria_spans_view_builders=[
                    TEST_CRITERIA_BUILDER_1,
                    TEST_CRITERIA_BUILDER_5,
                ],
                compliance_type=ComplianceType.ASSESSMENT,
                cadence_type=CadenceType.RECURRING_FIXED,
                due_date_field="test_reason_date",
                due_date_criteria_builder=TEST_CRITERIA_BUILDER_5,
                last_task_completed_date_field="last_contacted_date",
                last_task_completed_date_criteria_builder=TEST_CRITERIA_BUILDER_3,
            )

    def test_raises_error_when_due_date_criteria_not_provided_and_multiple_criteria_builders(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "Must specify due_date_criteria_builder when providing multiple criteria_spans_view_builders.",
        ):
            _ = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                task_name="my_task_name",
                candidate_population_view_builder=TEST_POPULATION_BUILDER,
                criteria_spans_view_builders=[
                    TEST_CRITERIA_BUILDER_1,
                    TEST_CRITERIA_BUILDER_3,
                    TEST_CRITERIA_BUILDER_5,
                ],
                compliance_type=ComplianceType.ASSESSMENT,
                cadence_type=CadenceType.RECURRING_FIXED,
                due_date_field="test_reason_date",
                last_task_completed_date_field="last_contacted_date",
                last_task_completed_date_criteria_builder=TEST_CRITERIA_BUILDER_5,
            )

    def test_raises_error_when_last_task_completed_criteria_not_provided_and_multiple_criteria_builders(
        self,
    ) -> None:
        with self.assertRaisesRegex(
            ValueError,
            "Must specify last_task_completed_date_criteria_builder when providing multiple criteria_spans_view_builders.",
        ):
            _ = ComplianceTaskEligibilitySpansBigQueryViewBuilder(
                state_code=StateCode.US_XX,
                task_name="my_task_name",
                candidate_population_view_builder=TEST_POPULATION_BUILDER,
                criteria_spans_view_builders=[
                    TEST_CRITERIA_BUILDER_1,
                    TEST_CRITERIA_BUILDER_3,
                    TEST_CRITERIA_BUILDER_5,
                ],
                compliance_type=ComplianceType.ASSESSMENT,
                cadence_type=CadenceType.RECURRING_FIXED,
                due_date_field="test_reason_date",
                due_date_criteria_builder=TEST_CRITERIA_BUILDER_3,
                last_task_completed_date_field="last_contacted_date",
            )

    def test_simple_compliance_tes_query(self) -> None:
        """
        Verify that compliance task eligibility spans properly wraps basic task eligibility spans
        with additional attributes.
        """
        load_data_for_task_criteria_view(
            emulator=self,
            criteria_view_builder=TEST_CRITERIA_BUILDER_5,
            criteria_data=[
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 6),
                    "meets_criteria": False,
                    "reason": {
                        "test_reason_date": "2024-02-08",
                        "last_contacted_date": "2024-01-01",
                    },
                    "reason_v2": {
                        "test_reason_date": "2024-02-08",
                        "last_contacted_date": "2024-01-01",
                    },
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 6),
                    "end_date": date(2024, 3, 18),
                    "meets_criteria": False,
                    "reason": {
                        "test_reason_date": "2024-04-03",
                        "last_contacted_date": "2024-04-01",
                    },
                    "reason_v2": {
                        "test_reason_date": "2024-04-03",
                        "last_contacted_date": "2024-04-01",
                    },
                },
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 3, 18),
                    "end_date": date(2024, 4, 8),
                    "meets_criteria": True,
                    "reason": {
                        "test_reason_date": "2024-04-10",
                        "last_contacted_date": "2024-04-01",
                    },
                    "reason_v2": {
                        "test_reason_date": "2024-04-10",
                        "last_contacted_date": "2024-04-01",
                    },
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
                        "criteria_name": TEST_CRITERIA_BUILDER_5.criteria_name,
                        "reason": {
                            "test_reason_date": "2024-02-08",
                            "last_contacted_date": "2024-01-01",
                        },
                    }
                ],
                "reasons_v2": [
                    {
                        "criteria_name": TEST_CRITERIA_BUILDER_5.criteria_name,
                        "reason": {
                            "test_reason_date": "2024-02-08",
                            "last_contacted_date": "2024-01-01",
                        },
                    }
                ],
                "ineligible_criteria": [TEST_CRITERIA_BUILDER_5.criteria_name],
                "due_date": date(2024, 2, 8),
                "last_task_completed_date": date(2024, 1, 1),
                "is_overdue": False,
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "start_date": date(2024, 2, 6),
                "end_date": date(2024, 3, 18),
                "is_eligible": False,
                "reasons": [
                    {
                        "criteria_name": TEST_CRITERIA_BUILDER_5.criteria_name,
                        "reason": {
                            "test_reason_date": "2024-04-03",
                            "last_contacted_date": "2024-04-01",
                        },
                    }
                ],
                "reasons_v2": [
                    {
                        "criteria_name": TEST_CRITERIA_BUILDER_5.criteria_name,
                        "reason": {
                            "test_reason_date": "2024-04-03",
                            "last_contacted_date": "2024-04-01",
                        },
                    }
                ],
                "ineligible_criteria": [TEST_CRITERIA_BUILDER_5.criteria_name],
                "due_date": date(2024, 4, 3),
                "last_task_completed_date": date(2024, 4, 1),
                "is_overdue": False,
            },
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "start_date": date(2024, 3, 18),
                "end_date": date(2024, 4, 8),
                "is_eligible": True,
                "reasons": [
                    {
                        "criteria_name": TEST_CRITERIA_BUILDER_5.criteria_name,
                        "reason": {
                            "test_reason_date": "2024-04-10",
                            "last_contacted_date": "2024-04-01",
                        },
                    }
                ],
                "reasons_v2": [
                    {
                        "criteria_name": TEST_CRITERIA_BUILDER_5.criteria_name,
                        "reason": {
                            "test_reason_date": "2024-04-10",
                            "last_contacted_date": "2024-04-01",
                        },
                    }
                ],
                "ineligible_criteria": [],
                "due_date": date(2024, 4, 10),
                "last_task_completed_date": date(2024, 4, 1),
                "is_overdue": False,
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

    def test_rolling_cadence_splits_span_at_due_date(self) -> None:
        """
        Verify that rolling cadence compliance TES splits an eligible span at
        the due date, producing an overdue portion after the due date.
        """
        load_data_for_task_criteria_view(
            emulator=self,
            criteria_view_builder=TEST_CRITERIA_BUILDER_5,
            criteria_data=[
                # Ineligible span: due date is within span but is_eligible=False,
                # so no split and is_overdue=False.
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 2, 1),
                    "meets_criteria": False,
                    "reason": {
                        "test_reason_date": "2024-01-31",
                        "last_contacted_date": "2024-01-01",
                    },
                    "reason_v2": {
                        "test_reason_date": "2024-01-31",
                        "last_contacted_date": "2024-01-01",
                    },
                },
                # Eligible span where due date (2024-02-29) falls within the
                # span [2024-02-01, 2024-04-01). Since the due date is inclusive,
                # the overdue period starts the day after, so this splits into:
                #   [2024-02-01, 2024-03-01) is_overdue=False
                #   [2024-03-01, 2024-04-01) is_overdue=True
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 2, 1),
                    "end_date": date(2024, 4, 1),
                    "meets_criteria": True,
                    "reason": {
                        "test_reason_date": "2024-02-29",
                        "last_contacted_date": "2024-01-15",
                    },
                    "reason_v2": {
                        "test_reason_date": "2024-02-29",
                        "last_contacted_date": "2024-01-15",
                    },
                },
                # Eligible span where due_date + 1 day (2024-04-01) <= start_date
                # (2024-04-01), so entire span is overdue.
                {
                    "state_code": "US_XX",
                    "person_id": 12345,
                    "start_date": date(2024, 4, 1),
                    "end_date": date(2024, 5, 1),
                    "meets_criteria": True,
                    "reason": {
                        "test_reason_date": "2024-03-31",
                        "last_contacted_date": "2024-01-15",
                    },
                    "reason_v2": {
                        "test_reason_date": "2024-03-31",
                        "last_contacted_date": "2024-01-15",
                    },
                },
            ],
        )
        load_data_for_candidate_population_view(
            emulator=self,
            population_date_spans=[(date(2024, 1, 1), date(2024, 5, 1))],
        )

        reasons_ineligible = [
            {
                "criteria_name": TEST_CRITERIA_BUILDER_5.criteria_name,
                "reason": {
                    "test_reason_date": "2024-01-31",
                    "last_contacted_date": "2024-01-01",
                },
            }
        ]
        reasons_eligible_split = [
            {
                "criteria_name": TEST_CRITERIA_BUILDER_5.criteria_name,
                "reason": {
                    "test_reason_date": "2024-02-29",
                    "last_contacted_date": "2024-01-15",
                },
            }
        ]
        reasons_entirely_overdue = [
            {
                "criteria_name": TEST_CRITERIA_BUILDER_5.criteria_name,
                "reason": {
                    "test_reason_date": "2024-03-31",
                    "last_contacted_date": "2024-01-15",
                },
            }
        ]

        expected_result = [
            # Ineligible span: no split, is_overdue=False
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "start_date": date(2024, 1, 1),
                "end_date": date(2024, 2, 1),
                "is_eligible": False,
                "reasons": reasons_ineligible,
                "reasons_v2": reasons_ineligible,
                "ineligible_criteria": [TEST_CRITERIA_BUILDER_5.criteria_name],
                "due_date": date(2024, 1, 31),
                "last_task_completed_date": date(2024, 1, 1),
                "is_overdue": False,
            },
            # Pre-due-date portion: eligible, not yet overdue (includes the due date itself)
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "start_date": date(2024, 2, 1),
                "end_date": date(2024, 3, 1),
                "is_eligible": True,
                "reasons": reasons_eligible_split,
                "reasons_v2": reasons_eligible_split,
                "ineligible_criteria": [],
                "due_date": date(2024, 2, 29),
                "last_task_completed_date": date(2024, 1, 15),
                "is_overdue": False,
            },
            # Entirely overdue span (due_date + 1 day <= start_date)
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "start_date": date(2024, 4, 1),
                "end_date": date(2024, 5, 1),
                "is_eligible": True,
                "reasons": reasons_entirely_overdue,
                "reasons_v2": reasons_entirely_overdue,
                "ineligible_criteria": [],
                "due_date": date(2024, 3, 31),
                "last_task_completed_date": date(2024, 1, 15),
                "is_overdue": True,
            },
            # Post-due-date portion: eligible and overdue (from overdue_splits, starts day after due date)
            {
                "state_code": "US_XX",
                "person_id": 12345,
                "start_date": date(2024, 3, 1),
                "end_date": date(2024, 4, 1),
                "is_eligible": True,
                "reasons": reasons_eligible_split,
                "reasons_v2": reasons_eligible_split,
                "ineligible_criteria": [],
                "due_date": date(2024, 2, 29),
                "last_task_completed_date": date(2024, 1, 15),
                "is_overdue": True,
            },
        ]

        self.run_query_test(
            query_str=ROLLING_COMPLIANCE_ELIGIBILITY_VIEW_BUILDER.build().view_query,
            expected_result=expected_result,
        )
