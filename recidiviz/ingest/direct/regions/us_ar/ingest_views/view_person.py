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
-- We pick the most recent commited name (indicated by OFFNNAMETYPE = '1') to use as the primary name in StatePerson
primary_name_type_1 AS(
  SELECT 
      OFFENDERID,
      OFFNFIRSTNAME,
      OFFNMIDDLENAME,
      OFFNLASTNAME,
      OFFNNAMESUFFIX 
  FROM (SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY OFFENDERID ORDER BY DATELASTUPDATE DESC) as seq_num
  FROM {OFFENDERNAMEALIAS}
  WHERE OFFNNAMETYPE = '1')
  WHERE seq_num = 1
),
-- For a small number of users, they do not have a commited name so we pick the most recent updated alias to hydrate the primary name in StatePerson
primary_name_type_special AS(
  SELECT 
      OFFENDERID,
      OFFNFIRSTNAME,
      OFFNMIDDLENAME,
      OFFNLASTNAME,
      OFFNNAMESUFFIX 
  FROM (SELECT *,
    ROW_NUMBER() OVER (
        PARTITION BY OFFENDERID ORDER BY DATELASTUPDATE DESC) as seq_num
  FROM {OFFENDERNAMEALIAS}
  WHERE OFFNNAMETYPE != '1' AND OFFNLASTNAME IS NOT NULL)
  WHERE seq_num = 1
),
-- Several OFFNNAMETYPE values map to StatePersonAliasType.INTERNAL_UNKNOWN, unlike the
-- other enum values which have one-to-one mappings. We will run into entity merge errors
-- if a person has an alias with more than one of these types, since it'll result in the 
-- same alias occurring with the same alias type (INTERNAL_UNKNOWN) more than once.
deduped_aliases_unknown_type AS (
  SELECT *
  FROM (
    SELECT *
    FROM {OFFENDERNAMEALIAS}
    WHERE OFFNNAMETYPE IN (
      -- These values all map to StatePersonAliasType.INTERNAL_UNKNOWN.
      '5','6','7','9','0'
    )
  )
  QUALIFY ROW_NUMBER() OVER (
    -- For each name for a given person that shows up multiple times with different OFFNNAMETYPE 
    -- values that would be mapped to INTERNAL_UNKNOWN, pick the current one if possible,
    -- and otherwise use the most recently updated.
    PARTITION BY 
      OFFENDERID,
      OFFNFIRSTNAME,
      OFFNMIDDLENAME,
      OFFNLASTNAME,
      OFFNNAMESUFFIX
    ORDER BY 
      CURRENTCOMMNAME = 'Y' DESC,
      DATELASTUPDATE DESC,
      TIMELASTUPDATE DESC
    ) = 1
),
-- We then identify all other potential aliases from the table for a given person and then populate that info in a JSON to hydrate StatePersonAlias
aliases AS (
  SELECT
    OFFENDERID,
    TO_JSON_STRING(ARRAY_AGG(STRUCT<alias_type string,first string,middle string,last string,suffix string>(OFFNNAMETYPE,OFFNFIRSTNAME,OFFNMIDDLENAME,OFFNLASTNAME,OFFNNAMESUFFIX) ORDER BY OFFNNAMETYPE,OFFNFIRSTNAME,OFFNMIDDLENAME,OFFNLASTNAME,OFFNNAMESUFFIX)) AS alias_list,
  FROM (
    SELECT DISTINCT 
      OFFENDERID,
      -- Name types 5, 6, 7, and 9 are all mapped to INTERNAL_UNKNOWN as they don't fit into our schema.
      -- Here, these name types are all grouped into a single category, which avoids duplicate aliases
      -- from being ingested when 2 aliases in this category have the same information but different OFFNAMETYPE.
      CASE WHEN OFFNNAMETYPE IN ('5','6','7','9') THEN 'SPECIAL_TYPE' ELSE OFFNNAMETYPE END AS OFFNNAMETYPE,
      OFFNFIRSTNAME,
      OFFNMIDDLENAME,
      OFFNLASTNAME,
      OFFNNAMESUFFIX
    FROM (
      -- Union the deduped aliases of unknown type back with the rest of the aliases, which don't need any deduping.
      SELECT *
      FROM {OFFENDERNAMEALIAS}
      WHERE OFFNNAMETYPE NOT IN ('5','6','7','9','0')
      UNION ALL
      SELECT *
      FROM deduped_aliases_unknown_type
    )
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
  
  COALESCE(pn1.OFFNFIRSTNAME,pns.OFFNFIRSTNAME) AS OFFNFIRSTNAME,
  COALESCE(pn1.OFFNMIDDLENAME,pns.OFFNMIDDLENAME) AS OFFNMIDDLENAME,
  COALESCE(pn1.OFFNLASTNAME,pns.OFFNLASTNAME) AS OFFNLASTNAME,
  COALESCE(pn1.OFFNNAMESUFFIX,pns.OFFNNAMESUFFIX) AS OFFNNAMESUFFIX,

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
  ad.ZIPCODE,

  ip.ADCNUMBER
FROM op_cleaned op
LEFT JOIN primary_name_type_1 pn1
USING(OFFENDERID)
LEFT JOIN primary_name_type_special pns
USING(OFFENDERID)
LEFT JOIN aliases al
USING(OFFENDERID)
LEFT JOIN ora_deduped ora 
USING(OFFENDERID)
LEFT JOIN {ADDRESS} ad
USING(ADDRESSID)
LEFT JOIN {INMATEPROFILE} ip
USING(OFFENDERID)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ar", ingest_view_name="person", view_query_template=VIEW_QUERY_TEMPLATE
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
