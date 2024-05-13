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
"""Query for employment verification supervision contacts (as determined by updates entered into the employment history modules)"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
-- Let's count every update to a record in the Atlas employment history module with an active employer
-- as an employment verification supervision contact
SELECT *,
       ROW_NUMBER() OVER(PARTITION BY EmploymentHistoryId ORDER BY UpdateDate) as contact_num
FROM (
    SELECT DISTINCT
        EmploymentHistoryId,
        OffenderId,
        UpdateUserId,
        DATE(hist.UpdateDate) as UpdateDate,
    FROM {ind_EmploymentHistory@ALL} hist
    WHERE 
        -- There is an employer
        EmployerId IS NOT NULL AND
        -- This is still an active employer
        EndDate = '0'
)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="employment_contacts",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
