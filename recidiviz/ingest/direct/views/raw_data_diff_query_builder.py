#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2023 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""Defines a class for building SQL queries to compute the diffs between existing data in a raw data table and
updated data for that table."""
import datetime
from types import ModuleType

from recidiviz.big_query.big_query_query_builder import BigQueryQueryBuilder
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct import regions
from recidiviz.ingest.direct.direct_ingest_regions import (
    raw_data_pruning_enabled_in_state_and_instance,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import DirectIngestRawFileConfig
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.raw_table_query_builder import RawTableQueryBuilder
from recidiviz.utils.string import StrictStringFormatter

DELETED_DIFF_JOIN_CLAUSE_TEMPLATE = """
    (
        (current_table_rows.{column_name} IS NULL AND new_table_rows.{column_name} IS NULL) OR 
        (current_table_rows.{column_name} = new_table_rows.{column_name})
    )"""

DELETED_DIFF_WHERE_CLAUSE_TEMPLATE = """
new_table_rows.{column_name} IS NULL
"""

QUERY_TEMPLATE = """
WITH current_table_rows AS 
( 
 {latest_current_raw_data_query}
), 
new_table_rows AS ( 
    SELECT 
      * 
    FROM `{project_id}.{new_raw_data_dataset}.{new_raw_data_table_id}`
),
added_or_updated_diff AS ( 
  SELECT * FROM new_table_rows 
  EXCEPT DISTINCT 
  SELECT * FROM current_table_rows
), 
deleted_diff AS ( 
  SELECT 
    current_table_rows.* 
  FROM 
    current_table_rows
  LEFT OUTER JOIN
    new_table_rows
  ON 
    {deleted_diff_join_clause} 
  WHERE 
    {deleted_diff_where_clause}
) 
SELECT 
  *, 
  {file_id} AS file_id,
  CAST('{update_datetime}' AS DATETIME) AS update_datetime,
  false AS is_deleted 
FROM added_or_updated_diff 

UNION ALL 

SELECT 
  *, 
  {file_id} AS file_id,
  CAST('{update_datetime}' AS DATETIME) AS update_datetime,
  true AS is_deleted 
FROM deleted_diff
"""


class RawDataDiffQueryBuilder:
    """Class for building SQL queries to compute the diffs between existing data in a raw data table and updated
    data for that table."""

    def __init__(
        self,
        project_id: str,
        state_code: StateCode,
        file_id: int,
        update_datetime: datetime.datetime,
        raw_data_instance: DirectIngestInstance,
        new_raw_data_table_id: str,
        raw_file_config: DirectIngestRawFileConfig,
        new_raw_data_dataset: str,
        region_module: ModuleType = regions,
    ):
        """Builds a query for computing the diffs between existing data in a raw data table and updated data for
        that table."""
        self._query_builder = BigQueryQueryBuilder(address_overrides=None)
        self.project_id = project_id
        self.state_code = state_code
        self.file_id = file_id
        self.update_datetime = update_datetime
        self.raw_data_instance = raw_data_instance
        self.raw_file_config = raw_file_config
        self.new_raw_data_table_id = new_raw_data_table_id

        self.latest_current_raw_data_query = RawTableQueryBuilder(
            project_id=self.project_id,
            region_code=self.state_code.value,
            raw_data_source_instance=self.raw_data_instance,
        ).build_query(
            raw_file_config=raw_file_config,
            address_overrides=None,
            normalized_column_values=False,
            raw_data_datetime_upper_bound=None,
            filter_to_latest=True,
            filter_to_only_documented_columns=False,
        )
        self.primary_key_cols = self.raw_file_config.primary_key_cols

        self.new_raw_data_dataset = new_raw_data_dataset
        self._region_module = region_module

    def build_query(self) -> str:
        if not raw_data_pruning_enabled_in_state_and_instance(
            self.state_code, self.raw_data_instance
        ):
            raise ValueError(
                f"Raw data pruning is not yet enabled for state_code={self.state_code.value}, "
                f"instance={self.raw_data_instance.value}"
            )

        query_kwargs = {
            "project_id": self.project_id,
            "file_id": str(self.file_id),
            "update_datetime": self.update_datetime.strftime("%Y-%m-%dT%H:%M:%S.%f"),
            "latest_current_raw_data_query": self.latest_current_raw_data_query,
            "new_raw_data_table_id": self.new_raw_data_table_id,
            "new_raw_data_dataset": self.new_raw_data_dataset,
            "deleted_diff_join_clause": self._build_is_deleted_join_clause(),
            "deleted_diff_where_clause": self._build_is_deleted_where_clause(),
        }
        return self._query_builder.build_query(
            project_id=self.project_id,
            query_template=QUERY_TEMPLATE,
            query_format_kwargs=query_kwargs,
        )

    def build_and_print(self) -> None:
        print(self.build_query())

    def _build_is_deleted_join_clause(self) -> str:
        join_statements = []
        for col in self.primary_key_cols:
            join_statements.append(
                StrictStringFormatter().format(
                    DELETED_DIFF_JOIN_CLAUSE_TEMPLATE, column_name=col
                )
            )
        return " AND".join(join_statements)

    def _build_is_deleted_where_clause(self) -> str:
        where_statements = []
        for col in self.primary_key_cols:
            where_statements.append(
                StrictStringFormatter().format(
                    DELETED_DIFF_WHERE_CLAUSE_TEMPLATE, column_name=col
                )
            )
        return " AND".join(where_statements)
