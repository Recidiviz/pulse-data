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
WITH compas AS (
SELECT * FROM (
  SELECT
    RecIds,
    FirstName,
    MiddleInitial,
    LastName,
    LOWER(Email) AS lower_Email,
    ROW_NUMBER() OVER (PARTITION BY LOWER(Email) ORDER BY IF(DateUpdated IS NOT NULL, DateUpdated, DateCreated) DESC) AS row_num
  FROM (
    -- Group all RecIds together by email
    SELECT DISTINCT
      STRING_AGG(DISTINCT RecId, ',') as RecIds,
      LOWER(Email) AS lower_Email
    FROM {ADH_SHUSER} s
    GROUP BY LOWER(Email)
  ) compas_ids
  JOIN {ADH_SHUSER} s
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
  LEFT JOIN {ADH_EMPLOYEE_ADDITIONAL_INFO} ea
  ON e.employee_id = ea.employee_id 
),

omni AS (
SELECT * FROM (
SELECT 
  employee_ids,
  first_name,
  middle_name,
  last_name,
  LOWER(omni_base.email_address) as email_address_lower,
  ROW_NUMBER() OVER (PARTITION BY LOWER(omni_base.email_address) ORDER BY last_update_date DESC) AS row_num
FROM (
  SELECT DISTINCT
    STRING_AGG(DISTINCT employee_id, ',') AS employee_ids,
    email_address
  FROM omni_base
  GROUP BY email_address
  ) omni_email
JOIN omni_base 
ON omni_base.email_address = LOWER(omni_email.email_address)
) omni_row
WHERE row_num = 1
)

SELECT 
  omni.employee_ids AS employee_ids_omni,
  compas.RecIds AS employee_ids_compas,
  COALESCE(omni.first_name, compas.FirstName) AS first_name,
  COALESCE(omni.middle_name, compas.MiddleInitial) AS middle_name,
  COALESCE(omni.last_name, compas.LastName) AS last_name,
  COALESCE(omni.email_address_lower, compas.lower_Email) AS email_address,
FROM omni
FULL JOIN compas
ON omni.email_address_lower = compas.lower_Email
"""


VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="state_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="employee_ids_omni",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
