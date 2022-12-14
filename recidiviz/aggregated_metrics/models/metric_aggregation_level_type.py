# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Constants related to a MetricAggregationLevelType."""
from enum import Enum
from typing import List, Optional

import attr

from recidiviz.common import attr_validators

# Mapping between clean index column names and their source column names
COLUMN_SOURCE_DICT = {
    "district": "supervision_district",
    "office": "supervision_office",
    "officer_id": "supervising_officer_external_id",
}


class MetricAggregationLevelType(Enum):
    """The type of metric aggregation level."""

    STATE_CODE = "STATE"
    FACILITY = "FACILITY"
    SUPERVISION_DISTRICT = "DISTRICT"
    SUPERVISION_OFFICE = "OFFICE"
    SUPERVISION_OFFICER = "OFFICER"


@attr.define(frozen=True, kw_only=True)
class MetricAggregationLevel:
    """Class that stores information about a unit of aggregation, along with functions to help generate SQL fragments"""

    # The level type object enum
    level_type: MetricAggregationLevelType

    # The name of the BigQuery table in the `sessions` dataset from which to derive assignments
    client_assignment_sessions_view_name: str = attr.field(
        validator=attr_validators.is_str
    )

    # List of attribute columns present in the assignment table by which to disaggregate
    index_columns: List[str]

    @property
    def level_name_short(self) -> str:
        return self.level_type.value.lower()

    def get_index_columns_query_string(self, prefix: Optional[str] = None) -> str:
        if prefix:
            return ", ".join([prefix + "." + column for column in self.index_columns])
        return ", ".join(self.index_columns)

    def get_index_column_rename_query_string(self, prefix: Optional[str] = None) -> str:
        full_prefix = f"{prefix}." if prefix else ""
        renamed_columns = [
            f"{full_prefix}{COLUMN_SOURCE_DICT[column]} AS {column}"
            if column in COLUMN_SOURCE_DICT
            else f"{full_prefix}{column}"
            for column in self.index_columns
        ]
        return ", ".join(renamed_columns)


METRIC_AGGREGATION_LEVELS_BY_TYPE = {
    MetricAggregationLevelType.FACILITY: MetricAggregationLevel(
        level_type=MetricAggregationLevelType.FACILITY,
        client_assignment_sessions_view_name="location_sessions_materialized",
        index_columns=["state_code", "facility"],
    ),
    MetricAggregationLevelType.STATE_CODE: MetricAggregationLevel(
        level_type=MetricAggregationLevelType.STATE_CODE,
        client_assignment_sessions_view_name="compartment_sessions_materialized",
        index_columns=["state_code"],
    ),
    MetricAggregationLevelType.SUPERVISION_DISTRICT: MetricAggregationLevel(
        level_type=MetricAggregationLevelType.SUPERVISION_DISTRICT,
        client_assignment_sessions_view_name="location_sessions_materialized",
        index_columns=["state_code", "district"],
    ),
    MetricAggregationLevelType.SUPERVISION_OFFICE: MetricAggregationLevel(
        level_type=MetricAggregationLevelType.SUPERVISION_OFFICE,
        client_assignment_sessions_view_name="location_sessions_materialized",
        index_columns=["state_code", "district", "office"],
    ),
    MetricAggregationLevelType.SUPERVISION_OFFICER: MetricAggregationLevel(
        level_type=MetricAggregationLevelType.SUPERVISION_OFFICER,
        client_assignment_sessions_view_name="supervision_officer_sessions_materialized",
        index_columns=["state_code", "officer_id"],
    ),
}
