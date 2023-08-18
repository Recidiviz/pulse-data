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

"""Query containing state staff role period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH omni AS (
  SELECT DISTINCT
    FIRST_VALUE(e.employee_id) OVER (PARTITION BY LOWER(email_address) ORDER BY CAST(e.last_update_date AS DATETIME) DESC ) AS employee_id,
    LOWER(email_address) AS lower_email_address,
    FIRST_VALUE(position) OVER (PARTITION BY LOWER(email_address) ORDER BY CAST(e.last_update_date AS DATETIME) DESC) AS position,
  FROM {ADH_EMPLOYEE} e
  LEFT JOIN {ADH_EMPLOYEE_ADDITIONAL_INFO} ea
  ON e.employee_id = ea.employee_id
  WHERE email_address IS NOT NULL 
  AND LOWER(email_address) != 'no_email@michigan.gov'
),
compas AS (
  SELECT DISTINCT
    FIRST_VALUE(RecId) OVER (PARTITION BY lower_Email ORDER BY LastUpdateDate DESC) AS RecId, 
    lower_Email, 
  FROM (
    SELECT 
      RecId, 
      LOWER(Email) AS lower_Email, 
      CASE 
        WHEN COALESCE(CAST(DateUpdated AS DATETIME), DATE(1900,1,1)) > COALESCE(CAST(DateCreated AS DATETIME), DATE(1900,1,1)) 
            THEN CAST(DateUpdated AS DATETIME) ELSE CAST(DateCreated AS DATETIME)
      END AS LastUpdateDate
    FROM {ADH_SHUSER}
    WHERE Email IS NOT NULL 
    AND LOWER(Email) != 'no_email@michigan.gov'
  ) compas_base
)

SELECT 
  omni.employee_id AS employee_id_omni,
  compas.RecId AS employee_id_compas,
  -- the compas dataset does not include data about position or job id
  omni.position AS position_omni,
  1 AS period_seq_num,
  -- TODO(#21395):We should be building these periods based on actual start and end dates of employment; these are arbitrary, temporary placeholders.
  DATE(1900, 1, 1) AS start_date,
  NULL AS end_date
FROM omni
FULL JOIN compas
ON omni.lower_email_address = compas.lower_Email


UNION ALL 

-- pull in all rows for people with null emails or no_email@michigan.gov
-- both of these cases only exist in the omni system
SELECT DISTINCT
  e.employee_id AS employee_id_omni,
  CAST(NULL AS STRING) AS employee_id_compas,
  position,
  1 as period_seq_num,
  DATE(1900, 1, 1) AS start_date,
  NULL AS end_date
FROM {ADH_EMPLOYEE} e
  LEFT JOIN {ADH_EMPLOYEE_ADDITIONAL_INFO} ea
  ON e.employee_id = ea.employee_id
WHERE email_address IS NULL
OR email_address = 'no_email@michigan.gov'
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="state_staff_role_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="employee_id_omni",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
