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

"""Query containing assessment information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
  ot.ofndr_num,
  ot.ofndr_tst_id,
  atc.assess_cat_desc,
  atl.tst_title,
  -- We null out any dates not between 1900 and 2040 because they probably are
  -- meaningless and they confuse bigquery
  IF(DATE(tst_dt) BETWEEN DATE('1900-01-01') AND DATE('2040-01-01'), CAST(LEFT(tst_dt, 10) AS DATE), NULL) tst_dt,
  ote.tot_score,
  UPPER(ote.eval_desc) as eval_desc
FROM {ofndr_tst} ot

-- Get score information
LEFT JOIN {ofndr_tst_eval} ote USING (ofndr_tst_id)

-- Get information about the assessment type
LEFT JOIN {assess_tst} atl USING (assess_tst_id)
LEFT JOIN {assess_tst_cat_cd} atc USING (assess_cat_cd)

WHERE 
  -- Only removes 23 rows.
  DATE(tst_dt) <= @update_timestamp
  
  -- There are several tests that have NOTE in the title. These don't seem to be actual
  -- tests, but rather a note about a test. I'm not sure how to retrieve that note yet,
  -- but they aren't actually tests so they can be filtered out. TODO(#37453) for more
  -- filters that could be applied this way
  AND tst_title NOT LIKE '%NOTE'
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="US_UT",
    ingest_view_name="assessment",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
