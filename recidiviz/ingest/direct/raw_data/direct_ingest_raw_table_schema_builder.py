# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""Class responsible for building schemas for raw data tables."""
from typing import Dict, List

from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud.bigquery.enums import SqlTypeNames

from recidiviz.big_query.big_query_utils import format_description_for_big_query
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    FILE_ID_COL_DESCRIPTION,
    FILE_ID_COL_NAME,
    IS_DELETED_COL_DESCRIPTION,
    IS_DELETED_COL_NAME,
    UPDATE_DATETIME_COL_DESCRIPTION,
    UPDATE_DATETIME_COL_NAME,
)

FILE_ID_COLUMN = SchemaField(
    name=FILE_ID_COL_NAME,
    field_type=SqlTypeNames.INTEGER.value,
    mode="REQUIRED",
    description=FILE_ID_COL_DESCRIPTION,
)

IS_DELETED_COLUMN = SchemaField(
    name=IS_DELETED_COL_NAME,
    field_type=SqlTypeNames.BOOLEAN.value,
    mode="REQUIRED",
    description=IS_DELETED_COL_DESCRIPTION,
)

UPDATE_DATETIME_COLUMN = SchemaField(
    name=UPDATE_DATETIME_COL_NAME,
    field_type=SqlTypeNames.DATETIME.value,
    mode="REQUIRED",
    description=UPDATE_DATETIME_COL_DESCRIPTION,
)


class RawDataTableBigQuerySchemaBuilder:
    """Builds big query schemas for raw data tables."""

    RECIDIVIZ_MANAGED_FIELDS: Dict[str, bigquery.SchemaField] = {
        FILE_ID_COL_NAME: FILE_ID_COLUMN,
        UPDATE_DATETIME_COL_NAME: UPDATE_DATETIME_COLUMN,
        IS_DELETED_COL_NAME: IS_DELETED_COLUMN,
    }

    @staticmethod
    def raw_file_column_as_bq_field(
        column: str, description: str
    ) -> bigquery.SchemaField:
        return bigquery.SchemaField(
            name=column,
            field_type=bigquery.enums.SqlTypeNames.STRING.value,
            mode="NULLABLE",
            description=format_description_for_big_query(description),
        )

    @staticmethod
    def _columns_to_descriptions_for_config(
        raw_file_config: DirectIngestRawFileConfig, columns: List[str]
    ) -> Dict[str, str]:
        config_columns_to_descr = {
            column.name: column.description or ""
            for column in raw_file_config.current_columns
        }

        return {col: config_columns_to_descr.get(col, "") for col in columns}

    # TODO(#28239) deprecate this approach in favor of build_schmea_for_big_query or
    # build_schema_for_big_query_from_columns
    @classmethod
    def build_bq_schema_from_columns(
        cls,
        *,
        raw_file_config: DirectIngestRawFileConfig,
        columns: List[str],
    ) -> List[bigquery.SchemaField]:
        """Returns a list of BigQuery Schema fields based on the provided |columns|
        for a raw data table.
        """

        schema_fields = []
        cols_to_descriptions = cls._columns_to_descriptions_for_config(
            raw_file_config, columns
        )

        for column, description in cols_to_descriptions.items():
            if column in cls.RECIDIVIZ_MANAGED_FIELDS:
                schema_fields.append(cls.RECIDIVIZ_MANAGED_FIELDS[column])
            else:
                schema_fields.append(
                    cls.raw_file_column_as_bq_field(column, description)
                )

        return schema_fields

    @classmethod
    def _raw_file_columns_as_bq_fields(
        cls, raw_file_config: DirectIngestRawFileConfig, columns: List[str]
    ) -> List[bigquery.SchemaField]:
        return [
            cls.raw_file_column_as_bq_field(column, description)
            for column, description in cls._columns_to_descriptions_for_config(
                raw_file_config, columns
            ).items()
        ]

    @classmethod
    def build_bq_schema_for_config_from_columns(
        cls,
        *,
        raw_file_config: DirectIngestRawFileConfig,
        columns: List[str],
        include_recidiviz_managed_fields: bool = True,
    ) -> List[bigquery.SchemaField]:
        """Returns a list of BigQuery Schema fields provided in |columns|, optionally
        adding the Recidiviz-managed fields.

        This method is provided as a helper for cases where we are handling importing
        incomplete configs; if that is not your use case, consider using
        build_schmea_for_big_query instead.
        """
        schema_columns = cls._raw_file_columns_as_bq_fields(raw_file_config, columns)

        if include_recidiviz_managed_fields:
            schema_columns.extend(cls.RECIDIVIZ_MANAGED_FIELDS.values())

        return schema_columns

    @classmethod
    def build_bq_schema_for_config(
        cls,
        *,
        raw_file_config: DirectIngestRawFileConfig,
        include_recidiviz_managed_fields: bool = True,
    ) -> List[bigquery.SchemaField]:
        """Returns a list of BigQuery Schema fields listed in the raw data config,
        optionally adding the Recidiviz-managed fields.
        """
        return cls.build_bq_schema_for_config_from_columns(
            raw_file_config=raw_file_config,
            columns=[col.name for col in raw_file_config.current_columns],
            include_recidiviz_managed_fields=include_recidiviz_managed_fields,
        )
