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
"""Ingest view for ageid_StaffSupervisor information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH critical_dates AS (
    SELECT
      StaffId,
      Supervisor,
      LAG(Supervisor) OVER (PARTITION BY StaffId ORDER BY update_datetime) AS prev_supervisor,
      update_datetime
    FROM {ageid_StaffRecord@ALL}
    WHERE TRUE
    QUALIFY (prev_supervisor IS NULL AND Supervisor IS NOT NULL) OR prev_supervisor != Supervisor
)
SELECT 
  StaffId,
  ROW_NUMBER() OVER (PARTITION BY StaffId ORDER BY update_datetime) AS period_seq_num,
  Supervisor,
  update_datetime AS start_date,
  LEAD(update_datetime) OVER (PARTITION BY StaffId ORDER BY update_datetime) AS end_date
FROM critical_dates
WHERE Supervisor IS NOT NULL;
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_oz",
    ingest_view_name="ageid_staff_supervisor",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
