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

"""Query containing housing status period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
  ofndr_num,
  ah.ofndr_addr_hist_id,
  addr_typ_cd,
  cmt,
  end_cmt,
  -- If the address is associated with a "homeless" attribute, or there is a comment
  -- contains the word "homeless" or "shelter" we consider the person to be homeless.
  (cmt IS NOT NULL AND (LOWER(cmt) LIKE '%homeless%' OR LOWER(cmt) like '%shelter%')) OR
  (aac.title = 'HOMELESS / TRANSIENT') AS unhoused,
  DATE(ah.strt_dt) AS strt_dt,
  DATE(ah.end_dt) AS end_dt,
  FROM {ofndr_addr_hist} ah
  LEFT JOIN {ofndr_addr_attr} aa USING (ofndr_addr_hist_id)
  LEFT JOIN {addr_attr_cd} aac USING (addr_attr_id)
  WHERE
    (
        ah.strt_dt IS NOT NULL 
        AND 
        DATE(ah.strt_dt) > DATE('1925-01-01')
        AND 
        DATE(ah.strt_dt) <= @update_timestamp
        AND 
        (ah.end_dt IS NULL OR DATE(ah.end_dt) <= @update_timestamp)
    )
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="US_UT",
    ingest_view_name="person_housing_status_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        # Use date_bounded to get raw data config formatters and null values
        VIEW_BUILDER.build_and_print(date_bounded=True)
