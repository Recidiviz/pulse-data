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
"""Query containing person information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
op_cleaned AS (
  SELECT * 
  FROM {OFFENDERPROFILE}
  WHERE REGEXP_CONTAINS(OFFENDERID, r'^[[:alnum:]]+$')
),
aliases AS (
  SELECT 
    OFFENDERID,
    TO_JSON_STRING(ARRAY_AGG(STRUCT<alias_type string,first string,middle string,last string,suffix string>(OFFNNAMETYPE,OFFNFIRSTNAME,OFFNMIDDLENAME,OFFNLASTNAME,OFFNNAMESUFFIX) ORDER BY OFFNNAMETYPE,OFFNFIRSTNAME,OFFNMIDDLENAME,OFFNLASTNAME,OFFNNAMESUFFIX)) AS alias_list,
  FROM (
    SELECT DISTINCT 
      OFFENDERID,
      OFFNNAMETYPE,
      OFFNFIRSTNAME,
      OFFNMIDDLENAME,
      OFFNLASTNAME,
      OFFNNAMESUFFIX
    FROM {OFFENDERNAMEALIAS}
  ) unique_aliases
  GROUP BY OFFENDERID
),
ora_deduped AS (
  SELECT * 
  FROM (
    SELECT DISTINCT
      OFFENDERID,
      ADDRESSID,
      ADDRENDDATE,
      ADDREFFECTIVEDATE,
      OFFNISHOMELESS,
      DATELASTUPDATE,
      ROW_NUMBER() OVER (
        PARTITION BY OFFENDERID 
        ORDER BY 
          CASE ADDRESSTYPE
            WHEN 'P' THEN 0
            WHEN 'B' THEN 1
            WHEN 'S' THEN 2
            WHEN 'T' THEN 3
            WHEN 'D' THEN 4
          END,
          DATELASTUPDATE DESC,
          TIMELASTUPDATE DESC,
          ADDREFFECTIVEDATE
      ) AS ora_recency
    FROM (
      SELECT * 
      FROM {OFNRELATEDADDRESS}
      WHERE ADDRESSTYPE IN ('P','B','S','T','D') 
      AND ADDRENDDATE LIKE '%9999-12-31%'
    ) current_addresses_only
  ) ora
  WHERE ora_recency = 1
)
SELECT 
  op.OFFENDERID,
  op.OFFNBIRTHDATE,
  op.OFFNRACE,
  op.OFFNSEX,
  op.OFFNETHNICGROUP,
  op.OFFNEMAILADDR,

  al.alias_list,

  ora.OFFNISHOMELESS,

  ad.STREETNUMBER,
  ad.STREETNAME,
  ad.STREETTYPE,
  ad.SUITENUMBER,
  ad.APARTMENTNUM,
  ad.POBOX,
  ad.CITY,
  ad.STATE,
  ad.ZIPCODE
FROM op_cleaned op
LEFT JOIN aliases al
USING(OFFENDERID)
LEFT JOIN ora_deduped ora 
USING(OFFENDERID)
LEFT JOIN {ADDRESS} ad
USING(ADDRESSID)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar",
    ingest_view_name="person",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OFFENDERID",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
