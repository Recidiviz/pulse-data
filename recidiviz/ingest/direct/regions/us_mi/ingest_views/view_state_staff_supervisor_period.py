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

# This currently returns supervisor periods for all staff, not just supervision related staff

VIEW_QUERY_TEMPLATE = """
SELECT 
  Staff_Omni_Employee_Id, 
  Supervisor_Omni_Employee_Id, 
  Start_Date, 
  End_Date,
  ROW_NUMBER() OVER(PARTITION BY Staff_Omni_Employee_Id ORDER BY Start_Date, End_Date NULLS LAST, Entered_Date) AS period_id
FROM {COMS_dm_Staff_Supervisors} coms
"""


VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_mi",
    ingest_view_name="state_staff_supervisor_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="Staff_Omni_Employee_Id, period_id",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()