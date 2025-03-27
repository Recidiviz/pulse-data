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
"""Query containing staff information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
  WITH 
    -- This CTE pulls all staff data from the case managers table
    staff_from_case_managers_data AS (
        SELECT DISTINCT CaseManagerStaffId, CaseManagerFirstNm, CaseManagerLastNm
        FROM {IA_DOC_CaseManagers}
        -- in case there are mismatches in names, we take the most recently entered one.  To split ties further, sort by name
        QUALIFY ROW_NUMBER() OVER(PARTITION BY CaseManagerStaffId ORDER BY CAST(EnteredDt AS DATETIME) DESC, CaseManagerLastNm DESC NULLS LAST, CaseManagerFirstNm DESC NULLS LAST) = 1
    ),

    -- This CTE pulls all staff data from the intervention tables
    staff_from_interventions_data AS (
        SELECT DISTINCT ReferringStaffId
        FROM {IA_DOC_Interventions}

        UNION DISTINCT

        SELECT DISTINCT ReferringStaffId
        FROM {IA_DOC_InterventionPrograms} 
    )

    SELECT *
    FROM staff_from_case_managers_data

    UNION ALL 

    SELECT 
        ReferringStaffId,
        CAST(NULL AS STRING) AS CaseManagerFirstNm,
        CAST(NULL AS STRING) AS CaseManagerLastNm
    FROM staff_from_interventions_data
    LEFT JOIN staff_from_case_managers_data
        ON ReferringStaffId = CaseManagerStaffId
    -- only include staff information from interventions data if it doesn't already exist in the case managers data
    WHERE CaseManagerStaffId IS NULL

"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ia",
    ingest_view_name="state_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
