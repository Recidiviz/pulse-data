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
"""File responsible for generating SQL queries that lightly cleans (trims
whitespace, removes all NULL rows) raw data that has been newly loaded from a CSV
into a temp table and adds appropriate metadata columns so that it matches the
expected format of our main raw data tables.
"""
import datetime

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_query_builder import BigQueryQueryBuilder
from recidiviz.common.constants.non_standard_characters import (
    NON_STANDARD_ASCII_CONTROL_CHARS_HEX_CODES,
    NON_STANDARD_UNICODE_SPACE_CHARACTERS,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRegionRawFileConfig,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance

PRE_MIGRATION_TRANSFORMATION_QUERY = """
SELECT {trimmed_cols}, {recidiviz_metadata_columns} \n FROM `{project_id}.{dataset_id}.{table_id}` \n WHERE {all_nulls_where}
"""


class DirectIngestTempRawTablePreMigrationTransformationQueryBuilder:
    """Class responsible for generating SQL queries that lightly cleans (trims
    whitespace, removes all NULL rows) raw data that has been newly loaded from a CSV
    into a temp table and adds appropriate metadata columns so that it matches the
    expected format of our main raw data tables.
    """

    def __init__(
        self,
        region_raw_file_config: DirectIngestRegionRawFileConfig,
        raw_data_instance: DirectIngestInstance,
    ) -> None:
        self._region_raw_file_config = region_raw_file_config
        self._raw_data_instance = raw_data_instance
        self._query_builder = BigQueryQueryBuilder(
            parent_address_overrides=None, parent_address_formatter_provider=None
        )

    @staticmethod
    def _build_replace_non_standard_characters_query_str(col_name: str) -> str:
        """Builds a SQL query str that replaces any non-standard ascii control characters (ASCII control characters sans
        horizontal tab, line feed, and carriage return) and unicode non-standard space characters with a standard space character.
        """
        non_standard_ascii_control_chars_str = "".join(
            NON_STANDARD_ASCII_CONTROL_CHARS_HEX_CODES
        )
        non_standard_unicode_space_chars_str = "".join(
            NON_STANDARD_UNICODE_SPACE_CHARACTERS
        )
        return rf"REGEXP_REPLACE({col_name}, r'[{non_standard_ascii_control_chars_str}{non_standard_unicode_space_chars_str}]', ' ')"

    def build_pre_migration_transformations_query(
        self,
        *,
        project_id: str,
        file_tag: str,
        source_table: BigQueryAddress,
        file_id: int,
        update_datetime: datetime.datetime,
        is_deleted: bool,
    ) -> str:
        """Builds a SQL query that converts non-standard whitespace and ascii control characters to a space character, trims
        whitespace from all columns, converts any value containing only whitespace to null, removes all NULL rows,
        and adds file_id, update_datetime, and is_deleted metadata columns to a raw data
        table that has been newly loaded from a CSV.
        """

        raw_table_config = self._region_raw_file_config.raw_file_configs[file_tag]

        trimmed_cols = ", ".join(
            rf"NULLIF(TRIM({self._build_replace_non_standard_characters_query_str(col_name)}), '')  as {col_name}"
            for col_name in raw_table_config.column_names_at_datetime(update_datetime)
        )

        filter_all_nulls_where = "\n  OR ".join(
            f"{col_name} IS NOT NULL"
            for col_name in raw_table_config.column_names_at_datetime(update_datetime)
        )

        # TODO(#30325) we are only including microseconds if they are non-zero to match legacy ingest
        # but we should migrate all existing update_datetime values the same format
        update_datetime_str = update_datetime.replace(tzinfo=None).isoformat(
            timespec="microseconds" if update_datetime.microsecond else "seconds"
        )
        parse_datetime_str = f"PARSE_DATETIME('%FT%H:%M:%E*S', '{update_datetime_str}')"

        recidiviz_metadata_columns = f"{file_id} as file_id, {parse_datetime_str} as update_datetime, {is_deleted} as is_deleted"

        return self._query_builder.build_query(
            project_id=project_id,
            query_template=PRE_MIGRATION_TRANSFORMATION_QUERY,
            query_format_kwargs={
                "trimmed_cols": trimmed_cols,
                "all_nulls_where": filter_all_nulls_where,
                "recidiviz_metadata_columns": recidiviz_metadata_columns,
                "dataset_id": source_table.dataset_id,
                "table_id": source_table.table_id,
            },
        )
