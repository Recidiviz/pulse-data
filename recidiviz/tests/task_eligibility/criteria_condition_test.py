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
"""Tests for the CriteriaCondition."""
import unittest
from typing import Dict, List, Optional

from google.cloud import bigquery

from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.task_eligibility.criteria_condition import (
    EligibleCriteriaCondition,
    LessThanCriteriaCondition,
    NotEligibleCriteriaCondition,
    PickNCompositeCriteriaCondition,
    ReasonDateInCalendarWeekCriteriaCondition,
    TimeDependentCriteriaCondition,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    StateAgnosticTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.tests.task_eligibility.task_criteria_group_big_query_view_builder_test import (
    CRITERIA_1_STATE_AGNOSTIC,
    CRITERIA_2_STATE_AGNOSTIC,
    CRITERIA_4_STATE_SPECIFIC,
)

NOT_ELIGIBLE_CRITERIA_CONDITION = NotEligibleCriteriaCondition(
    criteria=CRITERIA_1_STATE_AGNOSTIC,
    description="Not eligible criteria condition",
)
ELIGIBLE_CRITERIA_CONDITION = EligibleCriteriaCondition(
    criteria=CRITERIA_4_STATE_SPECIFIC,
    description="Eligible criteria condition",
)
LESS_THAN_CRITERIA_CONDITION = LessThanCriteriaCondition(
    criteria=CRITERIA_2_STATE_AGNOSTIC,
    reasons_numerical_field="fees_owed",
    value=1000,
    description="Less than value criteria condition",
)
TIME_DEPENDENT_CRITERIA_CONDITION = TimeDependentCriteriaCondition(
    criteria=CRITERIA_4_STATE_SPECIFIC,
    reasons_date_field="latest_violation_date",
    interval_length=1,
    interval_date_part=BigQueryDateInterval.MONTH,
    description="Time dependent criteria condition",
)
REASON_DATE_IN_CALENDAR_WEEK_CONDITION = ReasonDateInCalendarWeekCriteriaCondition(
    criteria=CRITERIA_4_STATE_SPECIFIC,
    reasons_date_field="latest_violation_date",
    description="Reason date in calendar week criteria condition",
)


class TestCriteriaCondition(unittest.TestCase):
    """Tests for the CriteriaCondition class."""

    def test_single_not_eligible_criteria_condition(self) -> None:
        """Checks a single NotEligibleCriteriaCondition"""
        self.assertEqual(
            [CRITERIA_1_STATE_AGNOSTIC],
            NOT_ELIGIBLE_CRITERIA_CONDITION.get_criteria_builders(),
        )
        self.assertEqual(
            "Not eligible criteria condition",
            NOT_ELIGIBLE_CRITERIA_CONDITION.description,
        )

    def test_single_eligible_criteria_condition(self) -> None:
        """Checks a single EligibleCriteriaCondition"""
        self.assertEqual(
            [CRITERIA_4_STATE_SPECIFIC],
            ELIGIBLE_CRITERIA_CONDITION.get_criteria_builders(),
        )
        self.assertEqual(
            "Eligible criteria condition",
            ELIGIBLE_CRITERIA_CONDITION.description,
        )

    def test_less_than_criteria_condition(self) -> None:
        """Checks a single LessThanCriteriaCondition"""
        self.assertEqual(
            [CRITERIA_2_STATE_AGNOSTIC],
            LESS_THAN_CRITERIA_CONDITION.get_criteria_builders(),
        )
        self.assertEqual(
            "Less than value criteria condition",
            LESS_THAN_CRITERIA_CONDITION.description,
        )

    def test_less_than_criteria_condition_wrong_reason_name(self) -> None:
        """Check the LessThanCriteriaCondition raises an error if the reason field is not found within the criteria"""
        with self.assertRaises(ValueError):
            test_condition = LessThanCriteriaCondition(
                criteria=CRITERIA_2_STATE_AGNOSTIC,
                reasons_numerical_field="other_reason",
                value=1000,
                description="Less than value criteria condition",
            )
            test_condition.get_criteria_builders()

    def test_less_than_criteria_condition_non_numeric_reason_type(self) -> None:
        """Check the LessThanCriteriaCondition raises an error if the supplied reason field type is non-numeric"""
        with self.assertRaises(ValueError):
            test_condition = LessThanCriteriaCondition(
                criteria=CRITERIA_4_STATE_SPECIFIC,
                reasons_numerical_field="latest_violation_date",
                value=1000,
                description="Less than value criteria condition",
            )
            test_condition.get_criteria_builders()


class TestTimeDependentCriteriaCondition(unittest.TestCase):
    """Tests for the TimeDependentCriteriaCondition class."""

    def test_time_dependent_criteria_condition(self) -> None:
        """Checks a single TimeDependentCriteriaCondition"""
        self.assertEqual(
            [CRITERIA_4_STATE_SPECIFIC],
            TIME_DEPENDENT_CRITERIA_CONDITION.get_criteria_builders(),
        )
        self.assertEqual(
            "Time dependent criteria condition",
            TIME_DEPENDENT_CRITERIA_CONDITION.description,
        )

    def test_time_dependent_criteria_condition_reason_date_field_wrong_name(
        self,
    ) -> None:
        """Check that an error is thrown when the reason date field is not in the criteria reasons fields list"""
        with self.assertRaises(ValueError):
            TimeDependentCriteriaCondition(
                criteria=CRITERIA_4_STATE_SPECIFIC,
                reasons_date_field="invalid_date_field",
                interval_length=1,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Time dependent criteria condition",
            )

    def test_time_dependent_criteria_condition_reason_date_field_wrong_type(
        self,
    ) -> None:
        """Check that an error is thrown when the reason date field within the criteria is not a date type"""
        with self.assertRaises(ValueError):
            TimeDependentCriteriaCondition(
                criteria=CRITERIA_4_STATE_SPECIFIC,
                reasons_date_field="violations",
                interval_length=1,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Time dependent criteria condition",
            )

    def test_time_dependent_criteria_condition_no_criteria_reasons(
        self,
    ) -> None:
        """Check that an error is thrown when the criteria span reasons fields list is empty"""
        with self.assertRaises(ValueError):
            TimeDependentCriteriaCondition(
                criteria=CRITERIA_1_STATE_AGNOSTIC,
                reasons_date_field="violations",
                interval_length=1,
                interval_date_part=BigQueryDateInterval.MONTH,
                description="Time dependent criteria condition",
            )


class TestReasonDateInCalendarWeekCriteriaCondition(unittest.TestCase):
    """Tests for the ReasonDateInCalendarWeekCriteriaCondition class."""

    def test_reason_date_in_calendar_week_criteria_condition(self) -> None:
        """Checks a single ReasonDateInCalendarWeekCriteriaCondition"""
        self.assertEqual(
            [CRITERIA_4_STATE_SPECIFIC],
            REASON_DATE_IN_CALENDAR_WEEK_CONDITION.get_criteria_builders(),
        )
        self.assertEqual(
            "Reason date in calendar week criteria condition",
            REASON_DATE_IN_CALENDAR_WEEK_CONDITION.description,
        )


class TestPickNCompositeCriteriaCondition(unittest.TestCase):
    """Tests for the PickNCompositeCriteriaCondition class."""

    def test_composite_criteria_three_sub_conditions(self) -> None:
        """Checks the composite criteria can handle three sub conditions within one composite group"""
        composite_criteria = PickNCompositeCriteriaCondition(
            sub_conditions_list=[
                NOT_ELIGIBLE_CRITERIA_CONDITION,
                LESS_THAN_CRITERIA_CONDITION,
                TIME_DEPENDENT_CRITERIA_CONDITION,
            ],
            at_least_n_conditions_true=2,
            at_most_n_conditions_true=2,
        )
        self.assertEqual(
            [
                CRITERIA_1_STATE_AGNOSTIC,
                CRITERIA_2_STATE_AGNOSTIC,
                CRITERIA_4_STATE_SPECIFIC,
            ],
            composite_criteria.get_criteria_builders(),
        )
        self.assertEqual(
            """Exactly 2 of the following conditions met to qualify as almost eligible:
    Not eligible criteria condition
    Less than value criteria condition
    Time dependent criteria condition""",
            composite_criteria.description,
        )

    def test_composite_criteria_two_critical_dates_two_criteria(self) -> None:
        """Checks the composite criteria can handle two time dependent criteria conditions within one composite group"""
        other_date_criteria = StateAgnosticTaskCriteriaBigQueryViewBuilder(
            criteria_name="ANOTHER_STATE_AGNOSTIC_CRITERIA",
            description="Another state-agnostic criteria",
            criteria_spans_query_template="SELECT * FROM `{project_id}.sessions.super_sessions_materialized`",
            meets_criteria_default=False,
            reasons_fields=[
                ReasonsField(
                    name="eligible_date",
                    type=bigquery.enums.StandardSqlTypeNames.DATE,
                    description="Date the criteria is met",
                ),
            ],
        )
        composite_criteria = PickNCompositeCriteriaCondition(
            sub_conditions_list=[
                TIME_DEPENDENT_CRITERIA_CONDITION,
                TimeDependentCriteriaCondition(
                    criteria=other_date_criteria,
                    reasons_date_field="eligible_date",
                    interval_length=2,
                    interval_date_part=BigQueryDateInterval.DAY,
                    description="Another time dependent criteria condition",
                ),
            ],
            at_least_n_conditions_true=1,
        )
        self.assertEqual(
            [CRITERIA_4_STATE_SPECIFIC, other_date_criteria],
            composite_criteria.get_criteria_builders(),
        )
        self.assertEqual(
            """At least 1 and at most 2 of the following conditions met to qualify as almost eligible:
    Time dependent criteria condition
    Another time dependent criteria condition""",
            composite_criteria.description,
        )

    def test_composite_criteria_two_critical_dates_one_criteria(self) -> None:
        """
        Checks the composite criteria condition can combine two time dependent conditions for one eligibility criteria.
        """
        two_date_criteria = StateAgnosticTaskCriteriaBigQueryViewBuilder(
            criteria_name="CRITERIA_4",
            description="A criteria for residents",
            criteria_spans_query_template="SELECT * FROM `{project_id}.sessions.super_sessions_materialized`",
            meets_criteria_default=False,
            reasons_fields=[
                ReasonsField(
                    name="criteria_date_1",
                    type=bigquery.enums.StandardSqlTypeNames.DATE,
                    description="First date relevant to the criteria",
                ),
                ReasonsField(
                    name="criteria_date_2",
                    type=bigquery.enums.StandardSqlTypeNames.DATE,
                    description="Second date relevant to the criteria",
                ),
            ],
        )
        composite_criteria = PickNCompositeCriteriaCondition(
            sub_conditions_list=[
                TimeDependentCriteriaCondition(
                    criteria=two_date_criteria,
                    reasons_date_field="criteria_date_1",
                    interval_length=1,
                    interval_date_part=BigQueryDateInterval.MONTH,
                    description="Within 1 month of criteria date 1",
                ),
                TimeDependentCriteriaCondition(
                    criteria=two_date_criteria,
                    reasons_date_field="criteria_date_2",
                    interval_length=2,
                    interval_date_part=BigQueryDateInterval.DAY,
                    description="Within 2 days of criteria date 2",
                ),
            ],
            at_least_n_conditions_true=1,
        )
        self.assertEqual(
            [two_date_criteria, two_date_criteria],
            composite_criteria.get_criteria_builders(),
        )
        self.assertEqual(
            """At least 1 and at most 2 of the following conditions met to qualify as almost eligible:
    Within 1 month of criteria date 1
    Within 2 days of criteria date 2""",
            composite_criteria.description,
        )

    def test_composite_criteria_not_enough_sub_conditions(self) -> None:
        """Checks an error is raised when PickNCompositeCriteriaCondition is initialized too few sub conditions"""
        with self.assertRaises(ValueError):
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[TIME_DEPENDENT_CRITERIA_CONDITION],
                at_least_n_conditions_true=1,
            )
        with self.assertRaises(ValueError):
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[],
                at_least_n_conditions_true=1,
            )

    def test_composite_criteria_opposing_sub_criteria(self) -> None:
        """
        Checks an error is raised when PickNCompositeCriteriaCondition is initialized with
        EligibleCriteriaCondition and NotEligibleCriteriaCondition applied to the same criteria
        """
        with self.assertRaises(ValueError):
            PickNCompositeCriteriaCondition(
                sub_conditions_list=[
                    NOT_ELIGIBLE_CRITERIA_CONDITION,
                    EligibleCriteriaCondition(
                        criteria=CRITERIA_1_STATE_AGNOSTIC,
                        description="Criteria 1 is met",
                    ),
                ],
                at_least_n_conditions_true=1,
            )

    def test_composite_criteria_bad_condition_count_limits(self) -> None:
        """
        Checks an error is raised when at_least_n_conditions_true and at_most_n_conditions_true are not properly set
        """
        sub_conditions_list = [
            TIME_DEPENDENT_CRITERIA_CONDITION,
            LESS_THAN_CRITERIA_CONDITION,
        ]
        error_combinations: List[Dict[str, Optional[int]]] = [
            # Both None
            {"at_least": None, "at_most": None},
            # Greater than total sub conditions
            {"at_least": 3, "at_most": None},
            {"at_least": None, "at_most": 3},
            {"at_least": 1, "at_most": 3},
            # Least is greater than most
            {"at_least": 2, "at_most": 1},
            # N conditions is zero
            {"at_least": 0, "at_most": 1},
            {"at_least": 1, "at_most": 0},
            {"at_least": 0, "at_most": None},
            {"at_least": None, "at_most": 0},
            # N conditions is negative
            {"at_least": -1, "at_most": 1},
            {"at_least": 1, "at_most": -1},
            {"at_least": None, "at_most": -1},
            {"at_least": -1, "at_most": None},
        ]
        for error_combo in error_combinations:
            with self.assertRaises(ValueError):
                PickNCompositeCriteriaCondition(
                    sub_conditions_list=sub_conditions_list,
                    at_least_n_conditions_true=error_combo["at_least"],
                    at_most_n_conditions_true=error_combo["at_most"],
                )
