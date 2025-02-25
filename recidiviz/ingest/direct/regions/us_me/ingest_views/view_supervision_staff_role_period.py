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

"""Query containing supervision staff role period information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT 
  Employee_Id,
  Cis_9001_Employee_Type_Cd,
  1 AS period_seq_num,
  -- TODO(#21219):We should be building these periods based on actual start and end dates of employment; these are arbitrary, temporary placeholders.
  -- Since this is a static roster, there will be exactly one row per P.O., and we will assume they are all actively employed.
  DATE(1900,1,1) AS Start_Date, 
  NULL AS End_Date
FROM {CIS_900_EMPLOYEE}
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_me",
    ingest_view_name="supervision_staff_role_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
