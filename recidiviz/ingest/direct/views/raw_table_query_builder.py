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
"""Helper class for building views over state raw data tables."""
import datetime
from typing import Optional

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_query_builder import BigQueryQueryBuilder
from recidiviz.big_query.big_query_utils import datetime_clause
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.raw_data.dataset_config import (
    raw_tables_dataset_for_region,
)
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_file_import_manager import (
    DirectIngestRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils.string import StrictStringFormatter

LATEST_INCREMENTAL_FILE_FILTER_CLAUSE = """filtered_rows AS (
    SELECT
        * EXCEPT (recency_rank)
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY {raw_table_primary_key_str}
                               ORDER BY update_datetime DESC{supplemental_order_by_clause}) AS recency_rank
        FROM
            `{{project_id}}.{raw_table_dataset_id}.{raw_table_name}`
        {date_filter_clause}
    ) a
    WHERE
        recency_rank = 1
)"""

# When querying raw data we receive as full historical dumps every transfer, we don't
# need to collapse rows down to one per primary key, because we believe that the
# contents of a single file are the correct, non-duplicated contents of the source table.
LATEST_HISTORICAL_FILE_FILTER_CLAUSE = """max_update_datetime AS (
    SELECT
        MAX(update_datetime) AS update_datetime
    FROM
        `{{project_id}}.{raw_table_dataset_id}.{raw_table_name}`
    {date_filter_clause}
),
max_file_id AS (
    SELECT
        MAX(file_id) AS file_id
    FROM
        `{{project_id}}.{raw_table_dataset_id}.{raw_table_name}`
    WHERE
        update_datetime = (SELECT update_datetime FROM max_update_datetime)
),
filtered_rows AS (
    SELECT *
    FROM
        `{{project_id}}.{raw_table_dataset_id}.{raw_table_name}`
    WHERE
        file_id = (SELECT file_id FROM max_file_id)
)"""
RAW_DATA_VIEW_TEMPLATE = """
WITH {filtered_rows_cte}
SELECT {columns_clause}
FROM filtered_rows
"""
DEFAULT_DATETIME_COL_NORMALIZATION_TEMPLATE = """
        COALESCE(
            CAST(SAFE_CAST({col_name} AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%y', {col_name}) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%Y', {col_name}) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M', {col_name}) AS DATETIME) AS STRING),
            CAST(SAFE_CAST(SAFE.PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S', {col_name}) AS DATETIME) AS STRING),
            {col_name}
        ) AS {col_name}"""
DATETIME_SQL_CAST_TEMPLATE = """
            CAST(SAFE_CAST({col_sql} AS DATETIME) AS STRING),"""
DATETIME_COL_NORMALIZATION_TEMPLATE = """
        COALESCE({datetime_casts}
            {col_name}
        ) AS {col_name}"""


class RawTableQueryBuilder:
    """Helper class for building views over state raw data tables."""

    def __init__(
        self,
        *,
        project_id: str,
        region_code: str,
        raw_data_source_instance: DirectIngestInstance,
    ):
        self.project_id = project_id
        self.state_code = StateCode(region_code.upper())
        self.raw_data_source_instance = raw_data_source_instance

    def build_query(
        self,
        raw_file_config: DirectIngestRawFileConfig,
        address_overrides: Optional[BigQueryAddressOverrides],
        normalized_column_values: bool,
        raw_data_datetime_upper_bound: Optional[datetime.datetime],
    ) -> str:
        """Returns a query against data in a state raw data table.

        Args:
            raw_file_config: The config for the raw table to query
            address_overrides: If provided, tables in the query will be replaced with
                these overrides, if applicable.
            normalized_column_values: If true, columns values will be normalized
                according to their config specification (e.g. datetime columns
                normalized).
            raw_data_datetime_upper_bound: If set, this raw data query will only return
                rows received on or before this datetime.
        """
        if normalized_column_values:
            columns_clause = self.normalized_columns_for_config(raw_file_config)
        else:
            columns_clause = self._columns_clause_for_config(raw_file_config)

        if raw_data_datetime_upper_bound:
            date_filter_clause = f"WHERE update_datetime <= {datetime_clause(raw_data_datetime_upper_bound)}"
        else:
            date_filter_clause = ""

        raw_table_dataset_id = raw_tables_dataset_for_region(
            state_code=self.state_code,
            instance=self.raw_data_source_instance,
            sandbox_dataset_prefix=None,
        )
        if not raw_file_config.always_historical_export:
            filtered_rows_cte = StrictStringFormatter().format(
                LATEST_INCREMENTAL_FILE_FILTER_CLAUSE,
                raw_table_dataset_id=raw_table_dataset_id,
                raw_table_name=raw_file_config.file_tag,
                raw_table_primary_key_str=raw_file_config.primary_key_str,
                supplemental_order_by_clause=self._supplemental_order_by_clause_for_config(
                    raw_file_config
                ),
                date_filter_clause=date_filter_clause,
            )
        else:
            filtered_rows_cte = StrictStringFormatter().format(
                LATEST_HISTORICAL_FILE_FILTER_CLAUSE,
                raw_table_dataset_id=raw_table_dataset_id,
                raw_table_name=raw_file_config.file_tag,
                date_filter_clause=date_filter_clause,
            )

        query_kwargs = {
            "columns_clause": columns_clause,
            "filtered_rows_cte": filtered_rows_cte,
        }
        return BigQueryQueryBuilder(address_overrides=address_overrides).build_query(
            project_id=self.project_id,
            query_template=RAW_DATA_VIEW_TEMPLATE,
            query_format_kwargs=query_kwargs,
        )

    @staticmethod
    def _supplemental_order_by_clause_for_config(
        raw_file_config: DirectIngestRawFileConfig,
    ) -> str:
        if not raw_file_config.supplemental_order_by_clause:
            return ""

        supplemental_order_by_clause = (
            raw_file_config.supplemental_order_by_clause.strip()
        )
        if not supplemental_order_by_clause.startswith(","):
            return ", " + supplemental_order_by_clause

        return supplemental_order_by_clause

    @staticmethod
    def _columns_clause_for_config(raw_file_config: DirectIngestRawFileConfig) -> str:
        if not raw_file_config.available_columns:
            raise ValueError(
                f"Found no available (documented) columns for file [{raw_file_config.file_tag}]"
            )

        columns_str = ", ".join(
            [column.name for column in raw_file_config.available_columns]
        )
        return columns_str

    @staticmethod
    def normalized_columns_for_config(
        raw_file_config: DirectIngestRawFileConfig,
    ) -> str:
        if not raw_file_config.available_columns:
            raise ValueError(
                f"Found no available (documented) columns for file [{raw_file_config.file_tag}]"
            )

        non_datetime_cols_to_format = raw_file_config.available_non_datetime_cols
        non_datetime_col_str = ", ".join(non_datetime_cols_to_format)
        datetime_cols_to_format = raw_file_config.available_datetime_cols

        if not datetime_cols_to_format:
            return non_datetime_col_str

        # Right now this only performs normalization for datetime columns, but in the future
        # this method can be expanded to normalize other values.
        return f"{non_datetime_col_str}, " + (
            ", ".join(
                [
                    StrictStringFormatter().format(
                        DEFAULT_DATETIME_COL_NORMALIZATION_TEMPLATE, col_name=col_name
                    )
                    if not col_sql_opt
                    else StrictStringFormatter().format(
                        DATETIME_COL_NORMALIZATION_TEMPLATE,
                        col_name=col_name,
                        datetime_casts="".join(
                            [
                                StrictStringFormatter().format(
                                    DATETIME_SQL_CAST_TEMPLATE,
                                    col_sql=StrictStringFormatter().format(
                                        col_sql, col_name=col_name
                                    ),
                                )
                                for col_sql in col_sql_opt
                            ]
                        ),
                    )
                    for col_name, col_sql_opt in datetime_cols_to_format
                ]
            )
        )
