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
source-table schema and appending it via to_gbq."""

import pandas as pd
from google.cloud.bigquery.enums import StandardSqlTypeNames as BigQueryFieldType

from recidiviz.source_tables.source_table_config import SourceTableConfig
from recidiviz.utils.environment import DATA_PLATFORM_GCP_PROJECTS


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
            # astype(str) would write missing values as the literal strings
            # "None", "nan", or "<NA>"; the pandas string dtype keeps them null.
            df[column_name] = df[column_name].astype("string")
        elif field_type == BigQueryFieldType.INT64:
            if pd.api.types.is_integer_dtype(df[column_name]):
                return
            df[column_name] = pd.to_numeric(df[column_name], downcast="integer")
        elif field_type in (
            BigQueryFieldType.DATE,
            BigQueryFieldType.DATETIME,
            BigQueryFieldType.TIMESTAMP,
        ):
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
    |table_config|'s schema. Converts each non-REPEATED column to the schema's
    declared type where possible, and checks that every value of a REPEATED
    column is a list.
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
        if col.mode == "REPEATED":
            non_list_values = [v for v in df[col.name] if not isinstance(v, list)]
            if non_list_values:
                raise ValueError(
                    f"Column [{col.name}] is REPEATED, so every value must be a "
                    f"list, but found: {non_list_values[:5]}"
                )
            continue
        convert_df_column_to_type_if_necessary(
            df, column_name=col.name, field_type=BigQueryFieldType(col.field_type)
        )


def append_df_to_source_table_in_all_projects(
    df: pd.DataFrame, *, table_config: SourceTableConfig
) -> None:
    """Appends df to table_config's table in every data platform project (staging
    and production). Call validate_and_convert_df_columns_to_schema() on df
    first.
    """
    num_rows = len(df)
    for project_id in DATA_PLATFORM_GCP_PROJECTS:
        project_specific_address = table_config.address.to_project_specific_address(
            project_id
        )
        print(
            f"Uploading [{num_rows}] rows to `{project_specific_address.to_str()}`..."
        )
        df.to_gbq(
            destination_table=table_config.address.to_str(),
            project_id=project_id,
            table_schema=[col.to_api_repr() for col in table_config.schema_fields],
            if_exists="append",
        )
        print(
            f"Done uploading [{num_rows}] rows to `{project_specific_address.to_str()}`."
        )
