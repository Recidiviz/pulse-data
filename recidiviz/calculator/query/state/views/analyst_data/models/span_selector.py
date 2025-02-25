# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Defines SpanSelector object used to filter rows from a spans table"""

from typing import Dict, List, Sequence, Union

import attr

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
    list_to_query_string,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    METRIC_UNITS_OF_OBSERVATION_BY_TYPE,
    MetricUnitOfObservation,
    MetricUnitOfObservationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.span_type import (
    SpanType,
)


@attr.define(frozen=True, kw_only=True)
class SpanSelector:
    """
    Class that stores information about conditions on a spans table, in order to
    generate query fragments for filtering spans.
    """

    # The SpanType used to filter to a subset of spans
    span_type: SpanType

    # Dictionary mapping span attributes to their associated conditions
    span_conditions_dict: Dict[str, Union[List[str], str]] = attr.ib()

    @property
    def unit_of_observation_type(self) -> MetricUnitOfObservationType:
        """Returns the MetricUnitOfObservationType associated with the span type"""
        return self.span_type.unit_of_observation_type

    @property
    def unit_of_observation(self) -> MetricUnitOfObservation:
        """Returns the MetricUnitOfObservation object associated with the inputted type"""
        return METRIC_UNITS_OF_OBSERVATION_BY_TYPE[self.unit_of_observation_type]

    def generate_span_conditions_query_fragment(self) -> str:
        """Returns a query fragment that filters a query based on configured span conditions"""
        condition_strings = [f'span = "{self.span_type.value}"']
        for attribute, conditions in self.span_conditions_dict.items():
            if isinstance(conditions, str):
                attribute_condition_string = conditions
            elif isinstance(conditions, Sequence):
                attribute_condition_string = (
                    f"IN ({list_to_query_string(conditions, quoted=True)})"
                )
            else:
                raise TypeError(
                    "All attribute filters must have type str or Sequence[str]"
                )
            condition_strings.append(
                f"""JSON_EXTRACT_SCALAR(span_attributes, "$.{attribute}") {attribute_condition_string}"""
            )
        # Apply all conditions via AND to a single span type
        condition_strings_query_fragment = "\n        AND ".join(condition_strings)
        return condition_strings_query_fragment

    def generate_span_selector_query(self) -> str:
        """Returns a standalone query that filters the appropriate spans table based on configured span conditions"""
        return f"""
WITH filtered_spans AS (
    SELECT *, end_date AS end_date_exclusive
    FROM `{{project_id}}.analyst_data.{self.unit_of_observation_type.short_name}_spans_materialized`
    WHERE {self.generate_span_conditions_query_fragment()}
)
,
{create_sub_sessions_with_attributes("filtered_spans", end_date_field_name="end_date_exclusive",index_columns=sorted(self.unit_of_observation.primary_key_columns))}
,
sub_sessions_dedup AS (
    SELECT DISTINCT
        {self.unit_of_observation.get_primary_key_columns_query_string()},
        start_date,
        end_date_exclusive,
    FROM
        sub_sessions_with_attributes
)
{aggregate_adjacent_spans(
    table_name='sub_sessions_dedup',
    index_columns=sorted(self.unit_of_observation.primary_key_columns),
    attribute=[],
    end_date_field_name='end_date_exclusive')
}
"""
