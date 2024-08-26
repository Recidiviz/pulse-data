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
from collections import defaultdict
from textwrap import indent
from typing import Dict, List, Optional, Tuple

import attr
from google.cloud import bigquery

from recidiviz.big_query.big_query_utils import BigQueryDateInterval
from recidiviz.task_eligibility.reasons_field import ReasonsField
from recidiviz.task_eligibility.task_criteria_big_query_view_builder import (
    TaskCriteriaBigQueryViewBuilder,
)
from recidiviz.task_eligibility.task_criteria_group_big_query_view_builder import (
    InvertedTaskCriteriaBigQueryViewBuilder,
    TaskCriteriaGroupBigQueryViewBuilder,
)
from recidiviz.task_eligibility.utils.state_dataset_query_fragments import (
    extract_object_from_json,
)
from recidiviz.utils.string import StrictStringFormatter

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
    def get_criteria_query_fragment(self) -> str:
        """
        Returns a query fragment that applies the criteria condition logic.
        """

    @abc.abstractmethod
    def get_criteria_builders(
        self,
    ) -> List[AnyTaskCriteriaViewBuilder]:
        """Return the list of criteria view builders that the CriteriaCondition covers"""

    def get_critical_date_parsing_fragments_by_criteria(
        self,
    ) -> Optional[Dict[str, List[str]]]:
        """
        Return a dictionary with the criteria name mapped to a list of critical date parsing query fragments used
        """
        return None


@attr.define
class NotEligibleCriteriaCondition(CriteriaCondition):
    """Condition relating to a single criteria being not eligible"""

    criteria: AnyTaskCriteriaViewBuilder

    def get_criteria_builders(
        self,
    ) -> List[AnyTaskCriteriaViewBuilder]:
        """Return the single criteria view builder"""
        return [self.criteria]

    def get_criteria_query_fragment(self) -> str:
        """
        Returns a query fragment that applies the criteria condition logic for NotEligibleCriteriaCondition.
        """
        return f"""
    SELECT
        *,
        TRUE AS is_almost_eligible,
    FROM potential_almost_eligible
    WHERE "{self.criteria.criteria_name}" IN UNNEST(ineligible_criteria)
"""


@attr.define
class _ComparatorCriteriaCondition(CriteriaCondition):
    """Condition relating to a static value within the criteria reasons fields"""

    criteria: AnyTaskCriteriaViewBuilder
    reasons_numerical_field: str
    value: float

    @staticmethod
    @abc.abstractmethod
    def _comparison_operator() -> str:
        """
        Return the logical operator used to compare the reasons numerical field to the value
        """

    def get_criteria_builders(
        self,
    ) -> List[AnyTaskCriteriaViewBuilder]:
        """Return the single criteria view builder"""
        return [self.criteria]

    def get_criteria_query_fragment(self) -> str:
        """
        Returns a query fragment that applies the comparator logic to the numeric reason field and the comparison value.
        """

        reasons_field = get_criteria_reason_field(
            self.criteria, self.reasons_numerical_field
        )

        supported_types = (
            bigquery.enums.StandardSqlTypeNames.INT64,
            bigquery.enums.StandardSqlTypeNames.FLOAT64,
        )
        if reasons_field.type not in supported_types:
            raise ValueError(
                f"Reason Field {reasons_field.name} has unsupported type {reasons_field.type.value} for "
                f"Criteria Condition, supported types are [{', '.join(supported_types)}]"
            )

        parse_reasons_value = extract_object_from_json(
            json_column="criteria_reason",
            object_column=f"reason.{reasons_field.name}",
            object_type=str(reasons_field.type.value),
        )

        return indent(
            f"""
SELECT
    * EXCEPT(criteria_reason),
    {parse_reasons_value} {self._comparison_operator()} {self.value} AS is_almost_eligible,
FROM potential_almost_eligible,
UNNEST(JSON_QUERY_ARRAY(reasons_v2)) AS criteria_reason
WHERE "{self.criteria.criteria_name}" IN UNNEST(ineligible_criteria)
    AND {extract_object_from_json(
            json_column="criteria_reason",
            object_column="criteria_name",
            object_type="STRING",
        )} = "{self.criteria.criteria_name}"
""",
            " " * 4,
        )


@attr.define
class LessThanCriteriaCondition(_ComparatorCriteriaCondition):
    @staticmethod
    def _comparison_operator() -> str:
        return "<"


@attr.define
class LessThanOrEqualCriteriaCondition(_ComparatorCriteriaCondition):
    @staticmethod
    def _comparison_operator() -> str:
        return "<="


class _DateComparatorCriteriaCondition(CriteriaCondition):
    """
    Condition relating to a date value within the criteria reasons fields. The condition will be true for the portion
    of the original criteria span beginning on the critical date as defined in the
    |critical_date_condition_query_template|
    """

    criteria: AnyTaskCriteriaViewBuilder
    reasons_date_field: ReasonsField
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
        self.reasons_date_field = get_criteria_reason_field(
            criteria, reasons_date_field
        )
        # Check the reasons field is a DATE type
        if self.reasons_date_field.type != bigquery.enums.SqlTypeNames.DATE:
            raise ValueError(
                f"Reason date field {self.reasons_date_field} is of type {self.reasons_date_field.type.value}, "
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

    def get_criteria_query_fragment(self) -> str:
        """
        Returns a query fragment that applies the critical date condition logic.
        """
        return indent(
            f"""
SELECT
    * EXCEPT(criteria_reason),
    -- Parse out the date relevant for the almost eligibility
    IFNULL(
        {self._get_criteria_condition_date_fragment(
            json_reasons_column="criteria_reason",
            nested_reason=True
        )} <= start_date,
        FALSE
    ) AS is_almost_eligible,
FROM potential_almost_eligible,
UNNEST(JSON_QUERY_ARRAY(reasons_v2)) AS criteria_reason
WHERE "{self.criteria.criteria_name}" IN UNNEST(ineligible_criteria)
    AND {extract_object_from_json(
            json_column="criteria_reason",
            object_column="criteria_name",
            object_type="STRING",
        )} = "{self.criteria.criteria_name}"
""",
            " " * 4,
        )

    def get_critical_date_parsing_fragments_by_criteria(
        self,
    ) -> Optional[Dict[str, List[str]]]:
        """Return the critical date parsing query for the reason field in the criteria"""
        return {
            self.criteria.criteria_name: [
                self._get_criteria_condition_date_fragment(
                    json_reasons_column="reason_v2"
                )
            ]
        }

    def _get_criteria_condition_date_fragment(
        self, json_reasons_column: str, nested_reason: bool = False
    ) -> str:
        """Return the query fragment that parses and computes the criteria condition date"""
        # Prepend "reason." to the field name string if the reasons blob is nested -
        # The reasons blob in eligibility spans is nested, the reason blob in criteria spans is not nested
        object_column = (
            f"reason.{self.reasons_date_field.name}"
            if nested_reason
            else self.reasons_date_field.name
        )
        return StrictStringFormatter().format(
            self.critical_date_condition_query_template,
            reasons_date=extract_object_from_json(
                json_column=json_reasons_column,
                object_column=object_column,
                object_type=str(self.reasons_date_field.type.value),
            ),
        )


class TimeDependentCriteriaCondition(_DateComparatorCriteriaCondition):
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


class ReasonDateInCalendarWeekCriteriaCondition(_DateComparatorCriteriaCondition):
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

    At most 1 time dependent condition can be included for a single eligibility criteria.
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
        if len(sub_conditions_list) <= 1:
            raise ValueError(
                "PickNCompositeCriteriaCondition requires 2 or more sub conditions"
            )
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

    def get_criteria_query_fragment(self) -> str:
        """
        Returns a query fragment that applies the composite criteria almost eligible logic.
        """
        composite_criteria_query = "\n    UNION ALL\n".join(
            [
                indent(f"""({condition.get_criteria_query_fragment()})""", " " * 4)
                for condition in self.sub_conditions_list
            ]
        )
        return indent(
            f"""
WITH composite_criteria_condition AS (
{composite_criteria_query}
)
SELECT
    state_code, person_id, start_date, end_date,
    -- Use ANY_VALUE for these span attributes since they are the same across every span with the same start/end date
    ANY_VALUE(is_eligible) AS is_eligible,
    ANY_VALUE(reasons) AS reasons,
    ANY_VALUE(reasons_v2) AS reasons_v2,
    ANY_VALUE(ineligible_criteria) AS ineligible_criteria,
    -- Almost eligible if number of almost eligible criteria count is in the set range
    -- and the almost eligible criteria not-met does not exceed the number allowed
    COUNTIF(is_almost_eligible) BETWEEN {self.at_least_n_conditions_true} AND {self.at_most_n_conditions_true}
        AND COUNTIF(NOT is_almost_eligible) <= {self.at_most_n_conditions_true - self.at_least_n_conditions_true} AS is_almost_eligible,
FROM composite_criteria_condition
GROUP BY 1, 2, 3, 4
""",
            " " * 4,
        )

    def get_critical_date_parsing_fragments_by_criteria(
        self,
    ) -> Optional[Dict[str, List[str]]]:
        """Return all critical date parsing queries for the sub conditions"""

        # Iteratively collect the critical dates from the sub conditions
        critical_date_fragments_map: Dict[str, List[str]] = defaultdict(list)
        for condition in self.sub_conditions_list:
            sub_condition_fragments_map = (
                condition.get_critical_date_parsing_fragments_by_criteria()
            )
            if not sub_condition_fragments_map:
                continue
            for criteria_name, fragments in sub_condition_fragments_map.items():
                critical_date_fragments_map[criteria_name].extend(fragments)

        if not critical_date_fragments_map:
            return None

        return critical_date_fragments_map

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
