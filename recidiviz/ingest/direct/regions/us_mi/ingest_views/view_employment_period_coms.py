# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Query containing MDOC employment period information using data from COMS."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT DISTINCT
  Employment_Id,
  LTRIM(emp.offender_number, '0') as Offender_Number,
  Job_Status,
  Job_Title,
  -- Let's start all employment periods on 8-14-2023 since that's when employment periods data got moved to COMS
  CASE 
    WHEN DATE(Employment_Start_Date) < DATE(2023,8,14) THEN DATE(2023,8,14)
    ELSE DATE(Employment_Start_Date)
    END AS Employment_Start_Date,
  DATE(Employment_End_Date) as Employment_End_Date,
  Reason_For_Leaving
FROM {COMS_Employment} emp
LEFT JOIN {ADH_OFFENDER} off on LTRIM(emp.offender_number, '0') = off.Offender_Number
INNER JOIN {ADH_OFFENDER_BOOKING} book on off.offender_id = book.offender_id
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="employment_period_coms",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
