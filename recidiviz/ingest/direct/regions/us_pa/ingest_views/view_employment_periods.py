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
"""Query containing incarceration sentence information from the dbo_Senrec table."""

from recidiviz.calculator.query.sessions_query_fragments import aggregate_adjacent_spans
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH 
-- This CTE nulls out end dates in EmploymentAvailability if it's in the future
EmploymentAvailability_cleaned AS (
    SELECT Parole_No,
           CAST(Effective_Start_Date AS DATETIME) AS Effective_Start_Date,
           CASE WHEN CAST(Effective_End_Date AS DATETIME) < CURRENT_DATE THEN CAST(Effective_End_Date AS DATETIME)
                ELSE NULL
                END AS Effective_End_Date,
            Offender_Attribute_Value
    FROM {{EmploymentAvailability}}
),
-- This CTE nulls out end dates in EmploymentPeriod if it's in the future
EmploymentPeriod_cleaned AS (
    SELECT Parole_No,
           CAST(Effective_Start_Date AS DATETIME) AS Effective_Start_Date,
           CASE WHEN CAST(Effective_End_Date AS DATETIME) < CURRENT_DATE THEN CAST(Effective_End_Date AS DATETIME)
                ELSE NULL
                END AS Effective_End_Date,
            Org_Name
    FROM {{EmploymentPeriod}}
),
-- This CTE identifies all relevant dates (so that we can make periods with them later)
all_dates AS (
  SELECT 
    Parole_No, 
    Effective_Start_Date AS start_date
  FROM EmploymentAvailability_cleaned

  UNION DISTINCT

  SELECT 
    Parole_No, 
    Effective_End_Date AS start_date
  FROM EmploymentAvailability_cleaned

  UNION DISTINCT

  SELECT 
    Parole_No, 
    Effective_Start_Date AS start_date
  FROM EmploymentPeriod_cleaned

  UNION DISTINCT

  SELECT 
    Parole_No, 
    Effective_End_Date AS start_date
  FROM EmploymentPeriod_cleaned
),
-- This CTE makes tiny spans based on the relevant dates
tiny_spans AS (
  SELECT *, 
    LEAD(start_date) OVER(PARTITION BY Parole_No ORDER BY start_date) AS end_date
  FROM all_dates
  WHERE start_date IS NOT NULL
),

-- This CTE joins on all attribute values relevant to each tiny span
initial_periods AS (
    SELECT 
    tiny_spans.*, 
    status.Offender_Attribute_Value, 
    employers.Org_Name,
    ROW_NUMBER() OVER(PARTITION BY tiny_spans.Parole_No ORDER BY tiny_spans.start_date, Org_Name, Offender_Attribute_Value) AS employment_seq_no
    FROM tiny_spans 
    LEFT JOIN EmploymentAvailability_cleaned status
    ON status.Effective_Start_Date <= tiny_spans.start_date 
        AND COALESCE(tiny_spans.end_date, DATE(9999,9,9)) <= COALESCE(status.Effective_End_Date, DATE(9999,9,9)) 
        AND tiny_spans.Parole_No = status.Parole_No
    LEFT JOIN EmploymentPeriod_cleaned employers
    ON employers.Effective_Start_Date <= tiny_spans.start_date 
        AND COALESCE(tiny_spans.end_date, DATE(9999,9,9)) <= COALESCE(employers.Effective_End_Date, DATE(9999,9,9)) 
        AND tiny_spans.Parole_No = employers.Parole_No
    WHERE Org_Name IS NOT NULL OR Offender_Attribute_Value IS NOT NULL
),

-- This CTE aggregates adjacent tiny spans together
final_periods AS (
    {aggregate_adjacent_spans(
        table_name="initial_periods",
        attribute=["Offender_Attribute_Value", "Org_Name"],
        index_columns=["Parole_No"])}
)

SELECT 
  Parole_No,
  Offender_Attribute_Value,
  Org_Name,
  start_date,
  end_date,
  employment_seq_no
FROM initial_periods
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="employment_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
