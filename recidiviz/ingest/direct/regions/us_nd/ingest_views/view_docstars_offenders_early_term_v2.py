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
"""Query containing information on current supervision early termination dates.
Built to run in IID. """

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT 
    SID, 
    EARLY_TERMINATION_DATE, 
    RecDate
FROM (
  SELECT
    SID,
    EARLY_TERMINATION_DATE,
    LAG(EARLY_TERMINATION_DATE) OVER (PARTITION BY SID ORDER BY RecDate) AS PREV_EARLY_TERMINATION_DATE,
    RecDate
  FROM
    {docstars_offenders@ALL}
) sub
WHERE 
  (PREV_EARLY_TERMINATION_DATE IS NULL AND EARLY_TERMINATION_DATE IS NOT NULL)
  OR (PREV_EARLY_TERMINATION_DATE IS NOT NULL AND EARLY_TERMINATION_DATE IS NULL)
  OR (
    PREV_EARLY_TERMINATION_DATE IS NOT NULL
    AND EARLY_TERMINATION_DATE IS NOT NULL
    AND PREV_EARLY_TERMINATION_DATE != EARLY_TERMINATION_DATE
  )
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="docstars_offenders_early_term_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
