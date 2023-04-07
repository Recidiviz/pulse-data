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
"""Constants related to a MetricAggregationLevelType."""
from enum import Enum
from typing import Dict, List, Optional

import attr

from recidiviz.calculator.query.state.dataset_config import (
    REFERENCE_VIEWS_DATASET,
    SESSIONS_DATASET,
)
from recidiviz.common import attr_validators

# This object contains a mapping from original column names in Recidiviz's
# schema to abbreviated column names. Note that we do not rename state_code
# or facility, so those columns are omitted from this object.
COLUMN_SOURCE_DICT = {
    "district": "supervision_district",
    "district_name": "supervision_district_name",
    "office": "supervision_office",
    "office_name": "supervision_office_name",
    "officer_id": "supervising_officer_external_id",
    "unit_name": "supervision_unit_name",
    "unit": "supervision_unit",
}


class MetricAggregationLevelType(Enum):
    """The type of metric aggregation level."""

    STATE_CODE = "STATE"
    FACILITY = "FACILITY"
    SUPERVISION_DISTRICT = "DISTRICT"
    SUPERVISION_OFFICE = "OFFICE"
    SUPERVISION_OFFICER = "OFFICER"
    SUPERVISION_UNIT = "UNIT"


@attr.define(frozen=True, kw_only=True)
class MetricAggregationLevel:
    """Class that stores information about a unit of aggregation, along with functions to help generate SQL fragments"""

    # The level type object enum
    level_type: MetricAggregationLevelType

    # The source query containing client assignments to the aggregation level
    client_assignment_query: str = attr.field(validator=attr_validators.is_str)

    # List of columns present in the assignment table that serve as the primary keys of the table
    primary_key_columns: List[str]

    # List of attribute columns in the assignment table that provide additional information about the aggregation level
    attribute_columns: List[str]

    # Dictionary of additional bigquery dataset args for assembly of a bigquery view builder
    dataset_kwargs: Dict[str, str]

    @property
    def level_name_short(self) -> str:
        """Returns lowercase enum name"""
        return self.level_type.value.lower()

    @property
    def pretty_name(self) -> str:
        return self.level_name_short.replace("_", " ").title()

    @property
    def index_columns(self) -> List[str]:
        """Returns concatenated list of primary key and attribute columns"""
        return self.primary_key_columns + self.attribute_columns

    def get_primary_key_columns_query_string(self, prefix: Optional[str] = None) -> str:
        """Returns string containing comma separated primary key column names with optional prefix"""
        prefix_str = f"{prefix}." if prefix else ""
        return ", ".join(f"{prefix_str}{column}" for column in self.primary_key_columns)

    def get_attribute_columns_query_string(self, prefix: Optional[str] = None) -> str:
        """Returns string containing comma separated attribute column names with optional prefix"""
        prefix_str = f"{prefix}." if prefix else ""
        return ", ".join(f"{prefix_str}{column}" for column in self.attribute_columns)

    def get_index_columns_query_string(self, prefix: Optional[str] = None) -> str:
        """Returns string containing comma separated index column names with optional prefix"""
        prefix_str = f"{prefix}." if prefix else ""
        return ", ".join(f"{prefix_str}{column}" for column in self.index_columns)

    def get_original_columns_query_string(self, prefix: Optional[str] = None) -> str:
        """Returns string containing comma separated column names from the source table with optional prefix"""
        prefix_str = f"{prefix}." if prefix else ""
        return ", ".join(
            f"{prefix_str}{COLUMN_SOURCE_DICT.get(column, column)}"
            for column in self.index_columns
        )

    def get_index_column_rename_query_string(self, prefix: Optional[str] = None) -> str:
        """
        Returns string containing comma separated column names from the source table aliased with new index columns
        where present in COLUMN_SOURCE_DICT, with optional prefix
        """
        prefix_str = f"{prefix}." if prefix else ""
        renamed_columns = [
            f"{prefix_str}{COLUMN_SOURCE_DICT.get(column, column)} AS {column}"
            for column in self.index_columns
        ]
        return ", ".join(renamed_columns)


METRIC_AGGREGATION_LEVELS_BY_TYPE = {
    MetricAggregationLevelType.FACILITY: MetricAggregationLevel(
        level_type=MetricAggregationLevelType.FACILITY,
        client_assignment_query="SELECT * FROM `{project_id}.{sessions_dataset}.location_sessions_materialized`",
        primary_key_columns=["state_code", "facility"],
        attribute_columns=["facility_name"],
        dataset_kwargs={"sessions_dataset": SESSIONS_DATASET},
    ),
    MetricAggregationLevelType.STATE_CODE: MetricAggregationLevel(
        level_type=MetricAggregationLevelType.STATE_CODE,
        client_assignment_query="SELECT * "
        "FROM `{project_id}.{sessions_dataset}.compartment_sessions_materialized`",
        primary_key_columns=["state_code"],
        attribute_columns=[],
        dataset_kwargs={"sessions_dataset": SESSIONS_DATASET},
    ),
    MetricAggregationLevelType.SUPERVISION_DISTRICT: MetricAggregationLevel(
        level_type=MetricAggregationLevelType.SUPERVISION_DISTRICT,
        client_assignment_query="SELECT * "
        "FROM `{project_id}.{sessions_dataset}.location_sessions_materialized`",
        primary_key_columns=["state_code", "district"],
        attribute_columns=["district_name"],
        dataset_kwargs={"sessions_dataset": SESSIONS_DATASET},
    ),
    MetricAggregationLevelType.SUPERVISION_OFFICE: MetricAggregationLevel(
        level_type=MetricAggregationLevelType.SUPERVISION_OFFICE,
        client_assignment_query="SELECT * "
        "FROM `{project_id}.{sessions_dataset}.location_sessions_materialized`",
        primary_key_columns=["state_code", "district", "office"],
        attribute_columns=["district_name", "office_name"],
        dataset_kwargs={"sessions_dataset": SESSIONS_DATASET},
    ),
    MetricAggregationLevelType.SUPERVISION_UNIT: MetricAggregationLevel(
        level_type=MetricAggregationLevelType.SUPERVISION_UNIT,
        client_assignment_query="SELECT * "
        "FROM `{project_id}.{sessions_dataset}.supervision_unit_sessions_materialized`",
        primary_key_columns=["state_code", "district", "unit"],
        attribute_columns=["unit_name"],
        dataset_kwargs={"sessions_dataset": SESSIONS_DATASET},
    ),
    MetricAggregationLevelType.SUPERVISION_OFFICER: MetricAggregationLevel(
        level_type=MetricAggregationLevelType.SUPERVISION_OFFICER,
        client_assignment_query="""
SELECT a.*, b.full_name_clean AS officer_name
FROM 
    `{project_id}.{sessions_dataset}.supervision_officer_sessions_materialized` a
LEFT JOIN 
    `{project_id}.{reference_views_dataset}.agent_external_id_to_full_name` b
ON 
    a.state_code = b.state_code
    AND a.supervising_officer_external_id = b.external_id
""",
        primary_key_columns=["state_code", "officer_id"],
        attribute_columns=["officer_name"],
        dataset_kwargs={
            "reference_views_dataset": REFERENCE_VIEWS_DATASET,
            "sessions_dataset": SESSIONS_DATASET,
        },
    ),
}
