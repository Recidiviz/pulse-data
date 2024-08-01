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
"""Query containing state staff information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH
corrected_adh_shuser AS (
  SELECT 
    RecId,
    FirstName,
    MiddleInitial,
    LastName,
    DateUpdated,
    DateCreated,
    CASE WHEN LOWER(Email) = 'no_email@michigan.gov' THEN NULL
         WHEN REGEXP_CONTAINS(Email, r'@[a-zA-Z0-9\\-]+\\.[a-zA-Z0-9\\-.]+') THEN Email
         ELSE NULL
        END AS Email
  FROM {ADH_SHUSER}
),
corrected_adh_employee_additional_info AS (
  SELECT
    employee_id,
    CASE WHEN LOWER(email_address) = 'no_email@michigan.gov' THEN NULL
         WHEN REGEXP_CONTAINS(email_address, r'@[a-zA-Z0-9\\-]+\\.[a-zA-Z0-9\\-.]+') THEN email_address
         ELSE NULL
        END AS email_address
  FROM {ADH_EMPLOYEE_ADDITIONAL_INFO}
),
compas AS (
SELECT * FROM (
  SELECT
    COALESCE(RecIds, s.RecId) as RecIds,
    FirstName,
    MiddleInitial,
    LastName,
    LOWER(Email) AS lower_Email,
    ROW_NUMBER() OVER (PARTITION BY LOWER(Email), COALESCE(RecIds, s.RecId) ORDER BY IF(DateUpdated IS NOT NULL, DateUpdated, DateCreated) DESC) AS row_num
  FROM (
    -- Group all RecIds together by email
    SELECT DISTINCT
      STRING_AGG(DISTINCT RecId, ',') as RecIds,
      LOWER(Email) AS lower_Email
    FROM corrected_adh_shuser s
    WHERE Email is not NULL
    GROUP BY LOWER(Email)
  ) compas_ids
  RIGHT JOIN corrected_adh_shuser s
  ON compas_ids.lower_Email = LOWER(s.Email)
  -- Pick the most recent row
) compas_row
WHERE row_num = 1
),

omni_base AS (
  SELECT DISTINCT
    e.employee_id,
    first_name,
    middle_name,
    last_name,
    LOWER(email_address) AS email_address,
    e.last_update_date,
  FROM {ADH_EMPLOYEE} e
  LEFT JOIN corrected_adh_employee_additional_info ea
  ON e.employee_id = ea.employee_id 
),

omni AS (
SELECT * FROM (
SELECT 
  COALESCE(employee_ids, employee_id) as employee_ids,
  first_name,
  middle_name,
  last_name,
  LOWER(omni_base.email_address) as email_address_lower,
  ROW_NUMBER() OVER (PARTITION BY LOWER(omni_base.email_address), COALESCE(employee_ids, employee_id) ORDER BY last_update_date DESC) AS row_num
FROM (
  SELECT DISTINCT
    STRING_AGG(DISTINCT employee_id, ',') AS employee_ids,
    email_address
  FROM omni_base
  WHERE email_address is not NULL
  GROUP BY email_address
  ) omni_email
RIGHT JOIN omni_base 
ON omni_base.email_address = LOWER(omni_email.email_address)
) omni_row
WHERE row_num = 1
), 
-- Getting staff information from compas if omni is null.
final_cte AS ( 
SELECT 
  omni.employee_ids AS employee_ids_omni,
  compas.RecIds AS employee_ids_compas,
  COALESCE(omni.first_name, compas.FirstName) AS first_name,
  COALESCE(omni.middle_name, compas.MiddleInitial) AS middle_name,
  COALESCE(omni.last_name, compas.LastName) AS last_name,
  COALESCE(omni.email_address_lower, compas.lower_Email) AS email_address,
FROM omni
FULL JOIN compas
ON omni.email_address_lower = compas.lower_Email)
-- Nulling out invalid emails.
SELECT 
  employee_ids_omni,
  employee_ids_compas,
  first_name,
  middle_name, 
  last_name, 
  IF(email_address LIKE '%zz_locked%','',email_address) AS email_address
FROM final_cte
"""


VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="state_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
