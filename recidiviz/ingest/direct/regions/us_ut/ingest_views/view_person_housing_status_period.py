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

# TODO(#37219) Still need to do homeless periods by joining with addr hist -- though not
# sure how accurate that will be.
VIEW_QUERY_TEMPLATE = """
SELECT
  ofndr_num,
  addr_typ_cd,
  DATE(addr_strt_dt) AS addr_strt_dt,
  DATE(addr_end_dt) AS addr_end_dt,
  -- These are concatted to make address_line_1
  psda_pre_dir,
  psda_street,
  psda_sfx_cd,
  psda_post_dir,
  -- These are concatted to make address_line_2
  pssa_unit_cd,
  pssa_range,
  -- These are optional address info
  city, 
  -- st,TODO(#37217)
  zip,
  vrfy_rslt_flg,
  -- Placed in 'address_metadata'
  DATE(vrfy_dt) AS vrfy_dt,
  ofndr_addr_id
  FROM {ofndr_addr_arch}
  WHERE
    (
        addr_strt_dt IS NOT NULL 
        AND 
        DATE(addr_strt_dt) > DATE('1925-01-01')
        AND 
        DATE(addr_strt_dt) <= @update_timestamp
        AND 
        DATE(addr_end_dt) <= @update_timestamp
    )
  AND
    (
        city IS NOT NULL 
        AND 
        city NOT IN ('UNKNOWN', 'UNKOWN', 'MEXICO')
    )
  AND
        vrfy_rslt_flg IS NOT NULL
  AND
    (
        psda_street NOT LIKE '%DEPORTED%'
        AND
        psda_street NOT LIKE '%UNKNOWN%'
    )
  AND 
    (
      -- These are concatted to make address_line_1
      psda_pre_dir IS NOT NULL
      OR
      psda_street IS NOT NULL
      OR
      psda_sfx_cd IS NOT NULL
      OR
      psda_post_dir IS NOT NULL
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
