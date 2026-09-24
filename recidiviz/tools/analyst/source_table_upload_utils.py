# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Shared helpers for validating a DataFrame against a registered BigQuery
source-table schema before appending it via to_gbq."""

import pandas as pd
from google.cloud.bigquery.enums import StandardSqlTypeNames as BigQueryFieldType

from recidiviz.source_tables.source_table_config import SourceTableConfig


def convert_df_column_to_type_if_necessary(
    df: pd.DataFrame, *, column_name: str, field_type: BigQueryFieldType
) -> None:
    """Converts df[column_name] in place to a dtype compatible with
    |field_type|, raising a ValueError if the values can't be converted.
    """
    try:
        if field_type == BigQueryFieldType.STRING:
            if pd.api.types.is_string_dtype(df[column_name]):
                return
            df[column_name] = df[column_name].astype(str)
        elif field_type == BigQueryFieldType.INT64:
            if pd.api.types.is_integer_dtype(df[column_name]):
                return
            df[column_name] = pd.to_numeric(df[column_name], downcast="integer")
        elif field_type in (BigQueryFieldType.DATE, BigQueryFieldType.TIMESTAMP):
            if pd.api.types.is_datetime64_any_dtype(df[column_name]):
                return
            df[column_name] = pd.to_datetime(df[column_name])
        else:
            raise ValueError(
                f"Unsupported column type for column [{column_name}]: {field_type}"
            )
    except Exception as e:
        raise ValueError(
            f"Column [{column_name}] cannot be converted to type [{field_type}]: {e}"
        ) from e


def validate_and_convert_df_columns_to_schema(
    df: pd.DataFrame, table_config: SourceTableConfig
) -> None:
    """Raises a ValueError if |df|'s columns don't exactly match
    |table_config|'s schema. Converts each column to the schema's declared
    type where possible.
    """
    required_columns = {column.name for column in table_config.schema_fields}
    df_columns = set(df.columns)

    extra_columns = df_columns - required_columns
    if extra_columns:
        raise ValueError(f"Extra columns present: {extra_columns}")

    missing_columns = required_columns - df_columns
    if missing_columns:
        raise ValueError(f"Missing required columns: {missing_columns}")

    for col in table_config.schema_fields:
        convert_df_column_to_type_if_necessary(
            df, column_name=col.name, field_type=BigQueryFieldType(col.field_type)
        )
