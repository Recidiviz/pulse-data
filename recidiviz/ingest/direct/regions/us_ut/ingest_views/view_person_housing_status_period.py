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
WITH 
-- We null out any dates between 1900 and 2040 because they probably are meaningless and
-- they confuse bigquery
cleaned_dates as (
  SELECT * except (addr_strt_dt, addr_end_dt, vrfy_dt),
    IF(DATE(addr_strt_dt) BETWEEN DATE('1900-01-01') AND DATE('2040-01-01'), CAST(LEFT(addr_strt_dt, 10) AS DATE), NULL) addr_strt_dt,
    IF(DATE(addr_end_dt) BETWEEN DATE('1900-01-01') AND DATE('2040-01-01'), CAST(LEFT(addr_end_dt, 10) AS DATE), NULL) addr_end_dt,
    IF(DATE(vrfy_dt) BETWEEN DATE('1900-01-01') AND DATE('2040-01-01'), CAST(LEFT(vrfy_dt, 10) AS DATE), NULL) vrfy_dt
  FROM {ofndr_addr_arch}
)
SELECT 
  ofndr_num, 
  ofndr_addr_id,
  addr_strt_dt,
  addr_end_dt,
  
  addr_typ_cd, 

  NULLIF(
    ARRAY_TO_STRING(
      ARRAY(
        SELECT * 
        FROM UNNEST([psda_num, psda_pre_dir, psda_street, psda_sfx_cd, psda_post_dir]) addr_bit 
        WHERE addr_bit IS NOT NULL
      ), ' '
    ), ''
  ) as concatted_address_1,
  NULLIF(
    ARRAY_TO_STRING(
      ARRAY(
        SELECT * 
        FROM UNNEST([pssa_unit_cd, pssa_range]) addr_bit 
        WHERE addr_bit IS NOT NULL
      ), ' '
    ), ''
  ) as concatted_address_2,
  city, 
  zip, 

  vrfy_dt,
  vrfy_rslt_flg
FROM cleaned_dates
WHERE addr_strt_dt <= @update_timestamp
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="US_UT",
    ingest_view_name="person_housing_status_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
