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
"""Query containing program assignment information using treatments data from Vantage.
"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
WITH 
-- This CTE infers referral date, start date, and discharge data based on Status and StatusDate.
-- In addition, because a client could be assigned to a program multiple times, we construct a
-- variable called program_attempt_count to track distinct assignments to the same program by 
-- assuming that each time a program has a status of "Assigned" represents a new attempt of the program.
data_with_inferred_dates AS (
  SELECT 
    Parole_No,
    Inmate_No,
    ProgramName,
    LocationID,
    Status,
    CAST(DateStatusChanged AS DATETIME) AS DateStatusChanged,
    CASE WHEN Status = 'Assigned' THEN CAST(DateStatusChanged AS DATETIME) ELSE NULL END AS referral_date,
    CASE WHEN Status = 'In Progress' THEN CAST(DateStatusChanged AS DATETIME) ELSE NULL END AS start_date,
    CASE WHEN Status in ('Discharged Prior to Completion', 
                        'Removed - Assigned in Error', 
                        'Completed', 
                        'Failed to Complete', 
                        'Failed to Complete - Refused',
                        'Removed with Credit') THEN CAST(DateStatusChanged AS DATETIME) ELSE NULL END AS discharge_date,
    SUM(CAST((Status = "Assigned") AS INT64)) 
      OVER (PARTITION BY COALESCE(Parole_No, Inmate_No), ProgramName 
            ORDER BY CAST(DateStatusChanged AS DATETIME)
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS program_attempt_count
  FROM {SubjectProgram}
)

-- Because this table is ledger style but doesn't come with primary keys for each program assignment, for each program and 
-- and program_attempt_count, we infer:
--    - referal date as the earliest date the program had a status of Assigned for this program_attempt_count 
--      (there should just be one date due to the way we construct program_attempt_count)
--    - start date as the earliest date the program had a status of In Progress for this program_attempt_count
--    - discharge date as the latest date the program had a completion related status for this program_attempt_count
-- We also create a treatment_seq_no based on earliest_status_date for each program assignment stint in order to create a
-- unique and deterministic external id

SELECT * EXCEPT(program_attempt_count, earliest_status_date), 
    ROW_NUMBER() OVER(PARTITION BY COALESCE(Parole_No, Inmate_No) ORDER BY earliest_status_date, ProgramName, program_attempt_count) AS treatment_seq_no
FROM (
  SELECT 
    Parole_No,
    Inmate_No,
    ProgramName,
    program_attempt_count,
    FIRST_VALUE(LocationID) OVER(w ORDER BY DateStatusChanged DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as most_recent_LocationId,
    FIRST_VALUE(Status) OVER(w ORDER BY DateStatusChanged DESC ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as most_recent_Status,
    MIN(referral_date) OVER(w) AS referral_date,
    MIN(start_date) OVER(w) AS start_date,
    MAX(discharge_date) OVER(w) AS discharge_date,
    MIN(DateStatusChanged) OVER(w) AS earliest_status_date
  FROM data_with_inferred_dates
  QUALIFY ROW_NUMBER() OVER(w ORDER BY DateStatusChanged DESC) = 1
  WINDOW w AS (PARTITION BY COALESCE(Parole_No, Inmate_No), ProgramName, program_attempt_count)
)
WHERE most_recent_status <> 'Removed - Assigned in Error'
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="program_assignment_vantage",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
