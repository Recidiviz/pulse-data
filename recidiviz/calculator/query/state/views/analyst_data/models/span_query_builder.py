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
"""Defines SpanQueryBuilder object used to construct a metric-agnostic span query"""

from typing import List, Union

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.sessions_query_fragments import (
    convert_cols_to_json_string,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    METRIC_UNITS_OF_OBSERVATION_BY_TYPE,
    MetricUnitOfObservationType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.span_type import (
    SpanType,
)
from recidiviz.common import attr_validators
from recidiviz.common.str_field_utils import snake_to_title


@attr.define(frozen=True, kw_only=True)
class SpanQueryBuilder:
    """
    Class that stores information required to produce a SQL query for span-shaped data
    defined by specified index columns and span start date and end date, and with the
    option to store additional attributes in a JSON blob.
    """

    # Type of span
    span_type: SpanType = attr.ib()

    # Description of the span
    description: str = attr.field(validator=attr_validators.is_str)

    # Source for generating metric entity: requires either a standalone SQL query
    # string, or a BigQueryAddress if referencing an existing table
    sql_source: Union[str, BigQueryAddress] = attr.ib()

    # List of column names from source query to include in the attributes JSON blob
    attribute_cols: List[str] = attr.field(validator=attr_validators.is_list)

    # Name of the column from source table that should be used as the span start date
    span_start_date_col: str = attr.field(validator=attr_validators.is_str)

    # Name of the column from source table that should be used as the span end date
    span_end_date_col: str = attr.field(validator=attr_validators.is_str)

    @property
    def query_builder_label(self) -> str:
        return "span"

    @property
    def pretty_name(self) -> str:
        return snake_to_title(self.span_type.name)

    @property
    def unit_of_observation_type(self) -> MetricUnitOfObservationType:
        """Returns the unit of observation type at which the span is observed"""
        return self.span_type.unit_of_observation_type

    @property
    def source_query_fragment(self) -> str:
        """Returns the properly formatted SQL query string of the inputted source table"""
        if isinstance(self.sql_source, BigQueryAddress):
            return f"`{{project_id}}.{self.sql_source.to_str()}`"
        return f"({self.sql_source})"

    def generate_subquery(self) -> str:
        unit_of_observation = METRIC_UNITS_OF_OBSERVATION_BY_TYPE[
            self.unit_of_observation_type
        ]
        return f"""
/* {self.description} */
SELECT DISTINCT
    {unit_of_observation.get_primary_key_columns_query_string()},
    "{self.span_type.value}" AS span,
    {self.span_start_date_col} AS start_date,
    {self.span_end_date_col} AS end_date,
    {convert_cols_to_json_string(self.attribute_cols)} AS span_attributes,
FROM
    {self.source_query_fragment}
"""
