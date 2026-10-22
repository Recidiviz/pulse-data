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
"""Utility functions for working with BQ schema and LookML fields."""
from typing import List, Optional

from google.cloud import bigquery

from recidiviz.looker.lookml_view_field import (
    DimensionLookMLViewField,
    TimeDimensionGroupLookMLViewField,
)
from recidiviz.looker.lookml_view_field_parameter import (
    LookMLFieldDatatype,
    LookMLFieldParameter,
    LookMLFieldType,
    LookMLTimeframesOption,
)

DATETIME_TIMEFRAMES = [
    LookMLTimeframesOption.RAW,
    LookMLTimeframesOption.TIME,
    LookMLTimeframesOption.DATE,
    LookMLTimeframesOption.WEEK,
    LookMLTimeframesOption.MONTH,
    LookMLTimeframesOption.QUARTER,
    LookMLTimeframesOption.YEAR,
]


def lookml_field_type_for_bq_type(
    field_type: bigquery.enums.SqlTypeNames,
) -> LookMLFieldType:
    """Converts a BigQuery field type into a LookMLFieldType."""
    if field_type in (
        bigquery.enums.SqlTypeNames.DATE,
        bigquery.enums.SqlTypeNames.DATETIME,
        bigquery.enums.SqlTypeNames.TIMESTAMP,
    ):
        return LookMLFieldType.TIME
    if field_type == bigquery.enums.SqlTypeNames.STRING:
        return LookMLFieldType.STRING
    if field_type == bigquery.enums.SqlTypeNames.BOOLEAN:
        return LookMLFieldType.YESNO
    if field_type in (
        bigquery.enums.SqlTypeNames.INTEGER,
        bigquery.enums.SqlTypeNames.FLOAT,
    ):
        return LookMLFieldType.NUMBER

    raise NotImplementedError(
        f"Could not convert {field_type.value} into Looker: LookML data type equivalent to "
        f"{field_type.value} has not been implemented"
    )


def lookml_view_field_for_schema_field(
    schema_field: bigquery.SchemaField,
    custom_params: Optional[List[LookMLFieldParameter]] = None,
) -> DimensionLookMLViewField | TimeDimensionGroupLookMLViewField:
    """Converts a BigQuery schema field into a LookML view field.

    Returns a TimeDimensionGroupLookMLViewField for DATE, DATETIME, and TIMESTAMP
    fields (producing a set of time-based dimensions at different granularities), and
    a DimensionLookMLViewField for all other types.

    Args:
        schema_field: The BigQuery schema field to convert.
        custom_params: Additional LookML parameters to include in the generated field.
    """
    field_type = bigquery.enums.SqlTypeNames(schema_field.field_type)
    if field_type == bigquery.enums.SqlTypeNames.DATETIME:
        return TimeDimensionGroupLookMLViewField.for_datetime_column(
            schema_field.name, custom_params
        )
    if field_type == bigquery.enums.SqlTypeNames.DATE:
        return TimeDimensionGroupLookMLViewField.for_date_column(
            schema_field.name, custom_params
        )
    if field_type == bigquery.enums.SqlTypeNames.TIMESTAMP:
        return TimeDimensionGroupLookMLViewField.for_column(
            column_name=schema_field.name,
            datatype=LookMLFieldDatatype.TIMESTAMP,
            timeframe_options=DATETIME_TIMEFRAMES,
            custom_params=custom_params,
        )
    return DimensionLookMLViewField.for_column(
        schema_field.name,
        lookml_field_type_for_bq_type(field_type),
        custom_params,
    )
