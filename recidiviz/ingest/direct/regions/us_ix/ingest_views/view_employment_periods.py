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
"""Query that generates the employment_periods entity"""
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT
    EmploymentHistoryId,
    OffenderId,
    EmploymentStatusDesc,
    PARSE_DATE('%Y%m%d', StartDate) as StartDate,
    CASE WHEN EndDate = '0' THEN NULL
        ELSE PARSE_DATE('%Y%m%d', EndDate)
        END AS EndDate,
    EmployerDesc,
    JobTitle
    -- NOTE: employment end reason can only be found for migrated data in the comments section. There is no place to enter end reason in the Atlas frontend.
    -- SPLIT(REGEXP_EXTRACT(Comments, r"[Emp\\_Quit\\_Reason]=[A-Z ]+"), '=')[OFFSET(1)] AS legacy_end_reason,
FROM {ind_EmploymentHistory}
LEFT JOIN {ref_Employer}
    USING (EmployerId)
LEFT JOIN {ref_EmploymentStatus}
    USING (EmploymentStatusId)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="employment_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
