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
from typing import Optional

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_query_builder import BigQueryQueryBuilder
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
        parent_address_overrides: Optional[BigQueryAddressOverrides] = None,
    ) -> None:
        self._region_raw_file_config = region_raw_file_config
        self._raw_data_instance = raw_data_instance
        self._query_builder = BigQueryQueryBuilder(
            parent_address_overrides=parent_address_overrides
        )

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
        """Builds a SQL query that trims whitespace from all columns, removes all NULL rows,
        and adds file_id, update_datetime, and is_deleted metadata columns to a raw data
        table that has been newly loaded from a CSV.
        """

        raw_table_config = self._region_raw_file_config.raw_file_configs[file_tag]

        trimmed_cols = ", ".join(
            f"TRIM({col.name}) as {col.name}" for col in raw_table_config.columns
        )

        filter_all_nulls_where = "\n  OR ".join(
            f"{col.name} IS NOT NULL" for col in raw_table_config.columns
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
