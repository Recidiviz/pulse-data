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
"""Defines EventQueryBuilder object used to construct a metric-agnostic event query"""

from typing import List, Union

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.calculator.query.sessions_query_fragments import (
    convert_cols_to_json_string,
)
from recidiviz.calculator.query.state.views.analyst_data.models.event_type import (
    EventType,
)
from recidiviz.calculator.query.state.views.analyst_data.models.metric_unit_of_analysis_type import (
    METRIC_UNITS_OF_OBSERVATION_BY_TYPE,
    MetricUnitOfObservationType,
)
from recidiviz.common import attr_validators
from recidiviz.common.str_field_utils import snake_to_title


@attr.define(frozen=True, kw_only=True)
class EventQueryBuilder:
    """
    Class that stores information required to produce a SQL query for event-shaped data
    defined by specified index columns and an event date, and with the option to store additional
    attributes in a JSON blob.
    """

    # Type of event
    event_type: EventType = attr.ib()

    # Description of the event
    description: str = attr.field(validator=attr_validators.is_str)

    # Source for generating metric entity: requires either a standalone SQL query string, or a BigQueryAddress
    # if referencing an existing table
    sql_source: Union[str, BigQueryAddress] = attr.ib()

    # List of column names from source query to include in the attributes JSON blob
    attribute_cols: List[str] = attr.field(validator=attr_validators.is_list)

    # Name of the column from source table that should be used as the event date
    event_date_col: str = attr.field(validator=attr_validators.is_str)

    @property
    def query_builder_label(self) -> str:
        return "event"

    @property
    def pretty_name(self) -> str:
        return snake_to_title(self.event_type.name)

    @property
    def unit_of_observation_type(self) -> MetricUnitOfObservationType:
        """Returns the unit of observation type at which the event is observed"""
        return self.event_type.unit_of_observation_type

    @property
    def source_query_fragment(self) -> str:
        """Returns the properly formatted SQL query string of the inputted source table"""
        if isinstance(self.sql_source, BigQueryAddress):
            return f"`{{project_id}}.{self.sql_source.to_str()}`"
        return f"""(
{self.sql_source}
)"""

    def generate_subquery(self) -> str:
        unit_of_observation = METRIC_UNITS_OF_OBSERVATION_BY_TYPE[
            self.unit_of_observation_type
        ]
        return f"""
/* {self.description} */
SELECT DISTINCT
    {unit_of_observation.get_primary_key_columns_query_string()},
    "{self.event_type.value}" AS event,
    {self.event_date_col} AS event_date,
    {convert_cols_to_json_string(self.attribute_cols)} AS event_attributes,
FROM
    {self.source_query_fragment}
"""
