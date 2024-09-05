# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Generates a query to check that for each update_datetime, there exists the same
number of rows and the same number of distinct file_ids, and returns any update_datetimes
in which there is a mismatch"""

import attr

from recidiviz.tools.ingest.operations.raw_table_diff_query_generator import (
    RawTableDiffQueryGenerator,
)
from recidiviz.utils.string import StrictStringFormatter

QUERY_TEMPLATE = """
WITH src AS (
    SELECT
      {datetime_column},
      COUNT(DISTINCT file_id) AS src_file_id_count,
      COUNT(*) AS src_row_count
    FROM
      `{src_project_id}.{src_raw_data_dataset_id}.{raw_data_table_id}`
    GROUP BY
      update_datetime
),
cmp AS (
    SELECT
      {datetime_column},
      COUNT(DISTINCT file_id) AS cmp_file_id_count,
      COUNT(*) AS cmp_row_count
    FROM
      `{cmp_project_id}.{cmp_raw_data_dataset_id}.{raw_data_table_id}`
    GROUP BY
      update_datetime
)

SELECT
  COALESCE(src.update_datetime, cmp.update_datetime) AS update_datetime,
  src.src_file_id_count,
  cmp.cmp_file_id_count,
  src.src_row_count,
  cmp.cmp_row_count
FROM
  src
FULL OUTER JOIN
  cmp
ON
  src.update_datetime = cmp.update_datetime
WHERE
  src.src_file_id_count != cmp.cmp_file_id_count OR
  src.src_file_id_count IS NULL OR
  cmp.cmp_file_id_count IS NULL OR
  src.src_row_count != cmp.cmp_row_count OR
  src.src_row_count IS NULL OR
  cmp.cmp_row_count IS NULL
ORDER BY
  update_datetime
"""


@attr.define
class RawTableFileCountsDiffQueryGenerator(RawTableDiffQueryGenerator):
    """Generates a query to check that for each update_datetime, there exists the same
    number of rows and the same number of distinct file_ids, and returns any update_datetimes
    in which there is a mismatch"""

    def generate_query(
        self,
        file_tag: str,
    ) -> str:
        return StrictStringFormatter().format(
            QUERY_TEMPLATE,
            src_project_id=self.src_project_id,
            src_raw_data_dataset_id=self.src_dataset_id,
            cmp_project_id=self.cmp_project_id,
            cmp_raw_data_dataset_id=self.cmp_dataset_id,
            raw_data_table_id=file_tag,
            datetime_column=self._get_truncated_datetime_column_name(),
        )
