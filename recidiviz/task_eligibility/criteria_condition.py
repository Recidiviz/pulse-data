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
"""Defines CriteriaCondition that can be used to set eligibility/almost eligibility logic on
task eligibility spans.
"""
import abc
import itertools
from typing import List, Optional, Tuple

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.task_eligibility.inverted_task_criteria_big_query_view_builder import (
    InvertedTaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    TaskCriteriaGroupBigQueryViewBuilder,
)

AnyTaskCriteriaViewBuilder = (
    TaskCriteriaBigQueryViewBuilder
    | TaskCriteriaGroupBigQueryViewBuilder
    | InvertedTaskCriteriaBigQueryViewBuilder
)


def get_criteria_reason_field(
    criteria: AnyTaskCriteriaViewBuilder, reason_field_name: str
) -> ReasonsField:
    """
    Fetch the corresponding reason field within the task criteria. Raise an error if the
    reason is not in the criteria reason fields.
    """
    try:
        return next(
            reason
            for reason in criteria.reasons_fields
            if reason.name == reason_field_name
        )
    except StopIteration as exc:
        raise ValueError(
            f"Reason [{reason_field_name}] is not in criteria "
            f"[{criteria.criteria_name}] reasons fields"
        ) from exc


@attr.define
class CriteriaCondition:

    description: str

    @abc.abstractmethod
    def get_criteria_builders(
        self,
    ) -> List[AnyTaskCriteriaViewBuilder]:
        """Return the list of criteria view builders that the CriteriaCondition covers"""


@attr.define
class NotEligibleCriteriaCondition(CriteriaCondition):
    """Condition relating to a single criteria being not eligible"""

    criteria: AnyTaskCriteriaViewBuilder

    def get_criteria_builders(self) -> List[AnyTaskCriteriaViewBuilder]:
        return [self.criteria]


@attr.define
class EligibleCriteriaCondition(CriteriaCondition):
    """
    Condition relating to a single criteria being eligible. This condition can be used in a
    PickNCompositeCriteriaCondition to increase the counts for the at_least_n_conditions_true
    and at_most_n_conditions_true parameters during spans when the criterion is met.
    """

    criteria: AnyTaskCriteriaViewBuilder

    def get_criteria_builders(self) -> List[AnyTaskCriteriaViewBuilder]:
        return [self.criteria]


class ComparatorCriteriaCondition(CriteriaCondition):
    """Condition relating to a static value within the criteria reasons fields"""

    criteria: AnyTaskCriteriaViewBuilder
    reasons_field: ReasonsField
    value: float
    comparison_operator: str

    def __init__(
        self,
        criteria: AnyTaskCriteriaViewBuilder,
        reasons_numerical_field: str,
        value: float,
        comparison_operator: str,
        description: str,
    ):
        self.criteria = criteria
        self.reasons_field = get_criteria_reason_field(
            criteria, reasons_numerical_field
        )
        # Check the reason field is a supported type
        supported_types = (
            bigquery.enums.StandardSqlTypeNames.INT64,
            bigquery.enums.StandardSqlTypeNames.FLOAT64,
        )
        if self.reasons_field.type not in supported_types:
            raise ValueError(
                f"Reason Field {self.reasons_field.name} has unsupported type "
                + self.reasons_field.type.value
                + f" for Criteria Condition, supported types are [{', '.join(supported_types)}]"
            )
        self.comparison_operator = comparison_operator
        self.value = value
        super().__init__(description)

    def get_criteria_builders(
        self,
    ) -> List[AnyTaskCriteriaViewBuilder]:
        """Return the single criteria view builder"""
        return [self.criteria]


class LessThanCriteriaCondition(ComparatorCriteriaCondition):
    def __init__(
        self,
        criteria: AnyTaskCriteriaViewBuilder,
        reasons_numerical_field: str,
        value: float,
        description: str,
    ):
        super().__init__(
            criteria=criteria,
            reasons_numerical_field=reasons_numerical_field,
            value=value,
            comparison_operator="<",
            description=description,
        )


class LessThanOrEqualCriteriaCondition(ComparatorCriteriaCondition):
    def __init__(
        self,
        criteria: AnyTaskCriteriaViewBuilder,
        reasons_numerical_field: str,
        value: float,
        description: str,
    ):
        super().__init__(
            criteria=criteria,
            reasons_numerical_field=reasons_numerical_field,
            value=value,
            comparison_operator="<=",
            description=description,
        )


class DateComparatorCriteriaCondition(CriteriaCondition):
    """
    Condition relating to a date value within the criteria reasons fields. The condition
    will be true for the portion of the original criteria span when
    |critical_date_condition_query_template| returns a True value.
    """

    criteria: AnyTaskCriteriaViewBuilder
    reasons_field: ReasonsField
    critical_date_condition_query_template: str

    def __init__(
        self,
        criteria: AnyTaskCriteriaViewBuilder,
        reasons_date_field: str,
        critical_date_condition_query_template: str,
        description: str,
    ):
        """Initialize the _DateComparatorCriteriaCondition object and validate the inputs"""
        self.criteria = criteria
        self.reasons_field = get_criteria_reason_field(criteria, reasons_date_field)
        # Check the reasons field is a DATE type
        if self.reasons_field.type != bigquery.enums.SqlTypeNames.DATE:
            raise ValueError(
                f"Reason date field {self.reasons_field} is of type {self.reasons_field.type.value}, "
                f"expected type {bigquery.enums.SqlTypeNames.DATE.value}"
            )
        # Check the {reason_field} formatting variable is in the date query template string
        if "{reasons_date}" not in critical_date_condition_query_template:
            raise ValueError(
                "Expected {reasons_date} formatting variable within the "
                f"{criteria.criteria_name} CustomTimeDependentCriteriaCondition: "
                + critical_date_condition_query_template
            )
        self.critical_date_condition_query_template = (
            critical_date_condition_query_template
        )
        super().__init__(description)

    def get_criteria_builders(
        self,
    ) -> List[AnyTaskCriteriaViewBuilder]:
        """Return the single criteria view builder"""
        return [self.criteria]


class TimeDependentCriteriaCondition(DateComparatorCriteriaCondition):
    """
    Condition relating to a date within the criteria reasons fields. The condition will be true for the portion of the
    original eligibility criteria span that has passed the reasons date MINUS the interval window.
    Example:
        Almost eligible if 3 months before `eligible_date`
        TimeDependentCriteriaCondition(
            criteria=example_criteria_view_builder,
            reasons_date_field="eligible_date",
            interval_length=3,
            interval_date_part=BigQueryDateInterval.MONTH,
        )

    A negative |interval_length| value will create an interval window that starts _after_ the reasons date for criteria
    such as "almost eligible 3 months after reasons date".
    """

    def __init__(
        self,
        criteria: AnyTaskCriteriaViewBuilder,
        reasons_date_field: str,
        interval_length: int,
        interval_date_part: BigQueryDateInterval,
        description: str,
    ) -> None:
        """
        Initialize the TimeDependentCriteriaCondition and validate that the reasons field is defined within the criteria
        and the reasons field type is DATE.
        """
        critical_date_condition_query_template = f"""DATE_SUB({{reasons_date}}, INTERVAL {interval_length} {interval_date_part.value})"""
        super().__init__(
            criteria,
            reasons_date_field,
            critical_date_condition_query_template,
            description,
        )


class ReasonDateInCalendarWeekCriteriaCondition(DateComparatorCriteriaCondition):
    """
    Condition relating to a date within the criteria reasons fields. The condition will be true for the portion of the
    original eligibility criteria span starting the Sunday before the reasons date and ending on the reasons date
    (end date exclusive).
    Example:
        Almost eligible if `eligible_date` is within the calendar week (starting Sunday)
        ReasonDateInCalendarWeekCriteriaCondition(
            criteria=example_criteria_view_builder,
            reasons_date_field="eligible_date",
        )
    """

    def __init__(
        self,
        criteria: AnyTaskCriteriaViewBuilder,
        reasons_date_field: str,
        description: str,
    ) -> None:
        """
        Initialize the ReasonDateInCalendarWeekCriteriaCondition with a critical date query template for the Sunday
        before the reasons date
        """
        critical_date_condition_query_template = (
            "DATE_TRUNC({reasons_date}, WEEK(SUNDAY))"
        )
        super().__init__(
            criteria,
            reasons_date_field,
            critical_date_condition_query_template,
            description,
        )


class PickNCompositeCriteriaCondition(CriteriaCondition):
    """
    Collection of criteria conditions to encapsulate multiple eligibility conditions.

    The `at_least_n_conditions_true` and `at_most_n_conditions_true` arguments can be used to configure AND/OR/XOR
    logic between conditions.
    """

    # List of all criteria conditions that make up the group
    sub_conditions_list: List[CriteriaCondition]

    at_least_n_conditions_true: int
    at_most_n_conditions_true: int

    def __init__(
        self,
        sub_conditions_list: List[CriteriaCondition],
        at_least_n_conditions_true: Optional[int] = None,
        at_most_n_conditions_true: Optional[int] = None,
    ) -> None:
        """
        Initialized the PickNCompositeCriteriaCondition object and validate that the least/most n conditions true are
        set properly.
        """
        self._validate_sub_conditions(sub_conditions_list)
        self.sub_conditions_list = sub_conditions_list

        (
            self.at_least_n_conditions_true,
            self.at_most_n_conditions_true,
        ) = self._validate_condition_count(
            len(sub_conditions_list),
            at_least_n_conditions_true,
            at_most_n_conditions_true,
        )

        description = self.get_description()
        super().__init__(description)

    def get_criteria_builders(
        self,
    ) -> List[AnyTaskCriteriaViewBuilder]:
        """Return all criteria view builders by recursively merging all sub condition criteria builders"""
        return list(
            itertools.chain.from_iterable(
                [
                    criteria.get_criteria_builders()
                    for criteria in self.sub_conditions_list
                ]
            )
        )

    @staticmethod
    def _validate_sub_conditions(
        sub_conditions_list: List[CriteriaCondition],
    ) -> None:
        """Validate the sub conditions list can be initialized properly"""
        # Require at least 2 sub conditions
        if len(sub_conditions_list) <= 1:
            raise ValueError(
                "PickNCompositeCriteriaCondition requires 2 or more sub conditions"
            )
        # Require a single eligibility criteria cannot be used for EligibleCriteriaCondition
        # and NotEligibleCriteriaCondition within the same group and level
        eligible_condition_criteria = {
            condition.criteria.criteria_name
            for condition in sub_conditions_list
            if isinstance(condition, EligibleCriteriaCondition)
        }
        not_eligible_condition_criteria = {
            condition.criteria.criteria_name
            for condition in sub_conditions_list
            if isinstance(condition, NotEligibleCriteriaCondition)
        }
        overlapping_criteria = eligible_condition_criteria.intersection(
            not_eligible_condition_criteria
        )
        if overlapping_criteria:
            raise ValueError(
                "Cannot initialize PickNCompositeCriteriaCondition with EligibleCriteriaCondition"
                f" and NotEligibleCriteriaCondition applied to the criteria: {', '.join(overlapping_criteria)}"
            )

    @staticmethod
    def _validate_condition_count(
        total_conditions: int,
        at_least_n_conditions_true: Optional[int],
        at_most_n_conditions_true: Optional[int],
    ) -> Tuple[int, int]:
        """Raise an error if the at_least_n_conditions_true and at_most_n_conditions_true arguments do not adhere to
        the logical requirements (one must be set, both must be less than or equal to the total conditions, etc.)."""
        # One of the two bounds must be set
        if (at_least_n_conditions_true is None) & (at_most_n_conditions_true is None):
            raise ValueError(
                "At least one argument is required for the PickNCompositeCriteria "
                "[at_least_n_conditions_true, at_most_n_conditions_true]"
            )
        # Set the lower bound to 1 if the attribute was not set
        if at_least_n_conditions_true is None:
            at_least_n_conditions_true = 1

        # Set the upper bound to |total_conditions| if the attribute was not set
        if at_most_n_conditions_true is None:
            at_most_n_conditions_true = total_conditions

        if at_most_n_conditions_true < at_least_n_conditions_true:
            raise ValueError(
                f"at_most_n_conditions_true ({at_most_n_conditions_true}) must be greater than or equal to "
                f"at_least_n_conditions_true ({at_least_n_conditions_true})"
            )

        if (at_most_n_conditions_true > total_conditions) | (
            at_most_n_conditions_true <= 0
        ):
            raise ValueError(
                f"Invalid value at_most_n_conditions_true ({at_most_n_conditions_true}) must be between 1 "
                f"and {total_conditions}"
            )

        if (at_least_n_conditions_true > total_conditions) | (
            at_least_n_conditions_true <= 0
        ):
            raise ValueError(
                f"Invalid value |at_least_n_conditions_true|={at_least_n_conditions_true} must be between 1 "
                f"and {total_conditions}"
            )

        return at_least_n_conditions_true, at_most_n_conditions_true

    def get_description(self) -> str:
        """Aggregate the description field across all the sub conditions"""
        predicate = (
            " of the following conditions met to qualify as almost eligible:\n    "
        )
        if self.at_least_n_conditions_true == self.at_most_n_conditions_true:
            description = f"Exactly {self.at_least_n_conditions_true}" + predicate
        else:
            description = (
                f"At least {self.at_least_n_conditions_true} and at most {self.at_most_n_conditions_true}"
                + predicate
            )
        return description + "\n    ".join(
            [condition.description for condition in self.sub_conditions_list]
        )
