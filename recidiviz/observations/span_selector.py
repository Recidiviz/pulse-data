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

from typing import Dict, List, Union

import attr

from recidiviz.calculator.query.sessions_query_fragments import (
    aggregate_adjacent_spans,
    create_sub_sessions_with_attributes,
)
from recidiviz.observations.observation_selector import ObservationSelector
from recidiviz.observations.span_observation_big_query_view_builder import (
    SpanObservationBigQueryViewBuilder,
)
from recidiviz.observations.span_type import SpanType


# TODO(#23055): Add state_code and person_id filters to support selecting custom
#  populations
@attr.define(frozen=True, kw_only=True)
class SpanSelector(ObservationSelector[SpanType]):
    """
    Class that stores information about conditions on a spans table, in order to
    generate query fragments for filtering spans.
    """

    # The SpanType to select
    span_type: SpanType

    # Dictionary mapping span attributes to their associated conditions. Only spans
    # with these attribute values will be selected.
    span_conditions_dict: Dict[str, Union[List[str], str]] = attr.ib()

    @property
    def observation_type(self) -> SpanType:
        return self.span_type

    @property
    def observation_conditions_dict(self) -> dict[str, list[str] | str]:
        return self.span_conditions_dict

    # TODO(#29291): Move this logic off the SpanSelector. This is specifically relevant
    #  to creating population spans and only used (outside of docstrings) in the
    #  assignment view queries.
    def generate_span_selector_query(self) -> str:
        """Returns a standalone query that filters the appropriate spans table based on
        configured span conditions
        """
        span_observation_address = (
            SpanObservationBigQueryViewBuilder.materialized_view_address_for_span(
                self.span_type
            )
        )

        conditions_fragment = self.generate_observation_conditions_query_fragment(
            filter_by_observation_type=False,
            read_attributes_from_json=False,
            strip_newlines=False,
        )
        filter_clause = (
            f"WHERE {conditions_fragment}" if self.span_conditions_dict else ""
        )
        return f"""
WITH filtered_spans AS (
    SELECT *, end_date AS end_date_exclusive
    FROM `{span_observation_address.format_address_for_query_template()}`
    {filter_clause}
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
