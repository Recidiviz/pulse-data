# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Query containing supervision violation information.

Supervision violations and their responses both come from `dbo_SanctionTracking`, a table which models violations and
their responses as a set of related entities. Each set contains one to many "violation" rows and one to many "sanction"
rows.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
base_violations AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY parole_number, parole_count_id, set_id, sequence_id 
      ORDER BY
        -- A row that has been moved to the history table is the most "current" version of this row. We want to ignore
        -- the old row in dbo_SanctionTracking.
        is_history_row DESC,
        -- This should never be different within a partition, but adding this for determinism
        violation_code,
        violation_timestamp
    ) AS row_rank
  FROM (
    SELECT
      ParoleNumber as parole_number,
      ParoleCountID as parole_count_id,
      SetID as set_id,
      SequenceID as sequence_id,
      SanctionCode as violation_code,
      ViolationDate AS violation_timestamp,
      0 AS is_history_row
    FROM {dbo_SanctionTracking}
    WHERE Type = 'V'

    UNION ALL

    SELECT
      ParoleNumber as parole_number,
      ParoleCountID as parole_count_id,
      SetID as set_id,
      SequenceID as sequence_id,
      SanctionCode as violation_code,
      ViolationDate AS violation_timestamp,
      1 AS is_history_row
    FROM {dbo_Hist_SanctionTracking}
    WHERE Type = 'V'
  )
)
SELECT
  parole_number,
  parole_count_id,
  set_id,
  EXTRACT(
    DATE FROM MIN(
        PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E3S", violation_timestamp)
    )
  ) as violation_date,
  TO_JSON_STRING(
    ARRAY_AGG(STRUCT(sequence_id, violation_code) ORDER BY sequence_id, violation_code, violation_timestamp)
  ) AS violation_types
FROM base_violations
WHERE row_rank = 1
GROUP BY parole_number, parole_count_id, set_id;
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="supervision_violation",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
