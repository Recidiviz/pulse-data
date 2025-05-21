# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Query containing CDCR client information. Currently only looks at folks who are on 
supervision.

Tickets to improve this view:

Things to consider making tickets for:
1. Parsing the first few characters of the CDCNO to add information.
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
  WITH 
  -- Pulls all necessary person info. This CTE may return multiple rows per real-world
  -- person, but should return exactly one row per OffenderId.
  base_info AS (
      SELECT
        OffenderId,
        Cdcno,
        FirstName,
        MiddleName,
        LastName,
        NameSuffix,
        Birthday,
        Sex,
        Race,
        Ethnic,
        AddressType,
        LastParoleDate
      FROM {PersonParole}
  ),
  -- Returns one row per Cdcno, with a JSON array containing info about each OffenderId
  -- associated with that Cdcno. Generally, Cdcno to OffenderId is a 1:1 relationship,
  -- but there are a handful (3 as of 5/16/2025) of people with multiple different 
  -- OffenderId.
  offender_ids_by_cdcno AS (
    SELECT 
      Cdcno,
      TO_JSON_STRING(
        ARRAY_AGG(STRUCT(OffenderId, LastParoleDate) ORDER BY LastParoleDate DESC, OffenderId)
      ) AS OffenderIds
    FROM base_info
    WHERE Cdcno IS NOT NULL
    GROUP BY Cdcno
  )

  SELECT 
    TO_JSON_STRING([STRUCT(OffenderId, LastParoleDate)]) AS OffenderIds, 
    base_info.* EXCEPT(Cdcno, LastParoleDate, OffenderId) 
  FROM base_info
  WHERE Cdcno IS NULL
  
  UNION ALL
  
  SELECT OffenderIds, base_info.* EXCEPT(Cdcno, LastParoleDate, OffenderId) 
  FROM base_info
  JOIN offender_ids_by_cdcno
    USING (Cdcno)
  # Choose the most recent row for each Cdcno as this will contain the most up-to-date
  # name / birthdate info. As of 5/14/2025 there are 3 Cdcno that have multiple
  # OffenderId values.
  QUALIFY ROW_NUMBER() OVER (PARTITION BY Cdcno ORDER BY LastParoleDate) = 1
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ca", ingest_view_name="person", view_query_template=VIEW_QUERY_TEMPLATE
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
