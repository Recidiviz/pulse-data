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
"""Query that generates the state staff entity using the following tables: Staff"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
-- Extract and format staff names (last, first, middle) from the full name field, also removes placeholder emails
    name_parts_cte AS (
    SELECT DISTINCT
        UPPER(CASE 
                WHEN UPPER(Staff_Email) NOT LIKE '%TDCJ.TEST@TDCJ.TEXAS.GOV%'  -- Exclude test emails
                AND (UPPER(Staff_Email) LIKE UPPER(CONCAT('%', REGEXP_EXTRACT(full_name, r'^(\\S+)\\s'), '%'))  -- Include emails matching LastName
                     OR Staff_Email LIKE UPPER(CONCAT('%', REGEXP_EXTRACT(full_name, r'^\\S+\\s(\\S+)'), '%')))  -- Or matching FirstName
                THEN Staff_Email 
                ELSE NULL 
            END) AS Staff_Email,  
        UPPER(Staff_ID_Number) AS Staff_ID_Number,  
        -- Extract Last Name: Get the first part before any space (assuming single-word last names)
        UPPER(REGEXP_EXTRACT(full_name, r'^(\\S+)\\s')) AS FirstName,
        -- Extract First Name: Get the word following the last name, before any middle name
        UPPER(REGEXP_EXTRACT(full_name, r'^\\S+\\s(\\S+)')) AS LastName,
        -- Extract Middle Name (if present): The part after the first name, if it exists
        CASE 
            WHEN REGEXP_CONTAINS(full_name, r'^\\S+\\s\\S+\\s\\S') THEN
                UPPER(REGEXP_EXTRACT(full_name, r'^\\S+\\s\\S+\\s(.+)$'))
            ELSE 
                NULL  
        END AS MiddleName,
        Creation_Date
    FROM {Staff}
),
--Filters down to a single name per unique email 
choose_one_name_per_email as (
  SELECT
    FirstName,
    MiddleName,
    LastName,
    staff_email
  FROM name_parts_cte
  QUALIFY ROW_NUMBER() OVER (PARTITION BY staff_email ORDER BY Creation_Date DESC) = 1
),
-- Aggregates emails by name
agg_email_by_name AS (
  SELECT
    FirstName,
    MiddleName,
    LastName,
    STRING_AGG(Staff_Email, 
            ',' ORDER BY Staff_Email
        ) AS email_list
  FROM choose_one_name_per_email
  GROUP BY FirstName,MiddleName,LastName
),
-- Filters down to a single name per unique staff id
choose_one_name_per_id AS (
  SELECT
    FirstName,
    MiddleName,
    LastName,
    Staff_ID_Number
  FROM name_parts_cte
  QUALIFY ROW_NUMBER() OVER (PARTITION BY Staff_ID_Number ORDER BY Creation_Date DESC) = 1
),
-- Aggregates ids by name
agg_staff_id_by_name AS (
  SELECT
    FirstName,
    MiddleName,
    LastName,
    STRING_AGG(Staff_ID_Number, 
            ',' ORDER BY Staff_ID_Number
        ) AS staff_id_list
  FROM choose_one_name_per_id
  GROUP BY FirstName,MiddleName,LastName
),
-- Chooses the latest email associated with a given name
latest_email_by_name AS (
  SELECT
    FirstName,
    MiddleName,
    LastName,
    staff_email AS latest_staff_email
  FROM name_parts_cte
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY FirstName, MiddleName, LastName
    ORDER BY Creation_Date DESC
  ) = 1
)

SELECT
    COALESCE(a.FirstName,e.FirstName) AS FirstName,
    COALESCE(a.MiddleName,e.MiddleName) AS MiddleName,
    COALESCE(a.LastName,e.LastName) AS LastName,
    STRING_AGG(DISTINCT email_list, ',' ORDER BY email_list) AS email_list,
    STRING_AGG(DISTINCT staff_id_list, ',' ORDER BY staff_id_list) AS staff_id_list,
    l.latest_staff_email
FROM agg_staff_id_by_name a
FULL OUTER JOIN agg_email_by_name e
  ON a.FirstName = e.FirstName
  AND a.LastName = e.LastName
  AND (a.MiddleName = e.MiddleName OR (a.MiddleName IS NULL AND e.MiddleName IS NULL))
LEFT JOIN latest_email_by_name l
  ON COALESCE(a.FirstName, e.FirstName) = l.FirstName
  AND COALESCE(a.LastName, e.LastName) = l.LastName
  AND COALESCE(a.MiddleName, e.MiddleName) IS NOT DISTINCT FROM l.MiddleName
GROUP BY FirstName,MiddleName,LastName, l.latest_staff_email;
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_tx",
    ingest_view_name="state_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
