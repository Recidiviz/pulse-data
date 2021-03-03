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
"""Query containing person demographic and identifier information from the DOC."""

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.ingest.direct.regions.us_pa.ingest_views.templates_person_external_ids import (
    MASTER_STATE_IDS_FRAGMENT,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""WITH
{MASTER_STATE_IDS_FRAGMENT},
search_inmate_info_with_master_ids AS (
    SELECT recidiviz_master_person_id, info.*
    FROM 
      {{dbo_tblSearchInmateInfo}} info
    LEFT OUTER JOIN
        (SELECT recidiviz_master_person_id, control_number 
         FROM recidiviz_master_person_ids
         GROUP BY recidiviz_master_person_id, control_number) ids
    USING (control_number)
),
bad_address_field_values AS (
  SELECT bad_value
  FROM UNNEST([
    'UNK', 'UNKNOWN', 'UNKOWN', 'N/A', 'NA', 'NONE', 'SAME', 'NONE PROVIDED', 'NONE AT THIS TIME',
    'NO ADDRESS GIVEN', 'NONE GIVEN @ RELEASE', 'NO ADDRESS PROVIDED', 'SAME AS ABOVE',
    'DECEASED', 'REFUSED', 'REFUSED TO GIVE', 'REFUSED TO PROVIDE', 'NOT KNOWN', 'NO ADDRESS', 
    'RECEIVED IN ERROR', 'ADDRESS UNKNOWN', 'NO FORWARDING ADDRES']
  ) bad_value
),
info_ranked_by_recency AS (
  SELECT
    recidiviz_master_person_id,
    info.control_number,
    inmate_number,
    ROW_NUMBER() OVER (
        PARTITION BY recidiviz_master_person_id 
        -- The delete_date column has the format YYYYDDD, where the YYYY term is the year and DDD is the # of
        -- days since the last day of the previous year (i.e. Jan 1 == 001). This column indicates 
        --- recency ordering, and will be set to 9999999 if the inmate number is active.
        ORDER BY CAST(delete_date AS INT64) DESC,
        inmate_number DESC
    ) AS recency_index,
    info.Frst_Nm,
    info.Mid_Nm,
    info.Lst_Nm,
    info.Nm_Suff,
    info.race,
    info.sex,
    NULLIF(info.date_of_birth, '00000000') AS date_of_birth,
    IF(
      -- Check for bogus addresses - set whole address to NULL if any are bad
      perrec.legal_address_1 IN (SELECT * FROM bad_address_field_values)
      OR perrec.legal_address_2 IN (SELECT * FROM bad_address_field_values)
      OR perrec.legal_city IN (SELECT * FROM bad_address_field_values),
      NULL,
      STRUCT(
        perrec.legal_address_1 AS legal_address_1,
        perrec.legal_address_2 AS legal_address_2,
        perrec.legal_city AS legal_city,
        perrec.legal_state,
        NULLIF(perrec.legal_zip_code, '00000') AS legal_zip_code
      )
    ) AS full_address
  FROM
    search_inmate_info_with_master_ids info
  LEFT OUTER JOIN
    {{dbo_Perrec}} perrec
  -- NOTE: As of 2020-06-10, there are 588 people returned by this query that didn't match a row in dbo_Perrec,
  -- so they will have null values from that table. Only 113 of those people are actively incarcerated.
  ON info.control_number = perrec.control_number AND info.inmate_number = perrec.inmate_num
),
most_recent_info AS (
  SELECT *
  FROM info_ranked_by_recency
  WHERE recency_index = 1
),
people AS (
  SELECT 
    recidiviz_master_person_id,
    control_number,
    Frst_Nm,
    Mid_Nm,
    Lst_Nm,
    Nm_Suff,
    race,
    sex,
    date_of_birth,
    full_address.legal_address_1 AS legal_address_1,
    full_address.legal_address_2 AS legal_address_2,
    full_address.legal_city AS legal_city,
    full_address.legal_state AS legal_state,
    full_address.legal_zip_code AS legal_zip_code,
    inmate_numbers
  FROM
    most_recent_info
  LEFT OUTER JOIN (
    SELECT recidiviz_master_person_id, STRING_AGG(DISTINCT inmate_number, ',' ORDER BY inmate_number) AS inmate_numbers
    FROM search_inmate_info_with_master_ids
    GROUP BY recidiviz_master_person_id
  ) AS inmate_numbers_grouping
  USING (recidiviz_master_person_id)
)
SELECT 
  *
FROM people
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_pa",
    ingest_view_name="doc_person_info_v2",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="recidiviz_master_person_id",
    materialize_raw_data_table_views=True,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
