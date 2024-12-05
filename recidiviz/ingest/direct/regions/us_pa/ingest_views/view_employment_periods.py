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

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    SELECT *, ROW_NUMBER() OVER(PARTITION BY Parole_No ORDER BY DATE(Effective_Start_Date), Org_Name) AS employment_seq_no
    FROM (
        SELECT DISTINCT
            Parole_No, 
            Offender_Attribute_Value, 
            Org_Name, 
            DATE(Effective_Start_Date) as Effective_Start_Date,
            CAST(NULL AS Date) AS Effective_End_Date
            -- TODO(#35515) Add in actual end date once PA removes the current employment filter
            -- in dbo_EmploymentPeriod, Effective_End_Date is typically set to 12-31-9999 when the employment period is still ongoing, 
            -- but there are a few cases where Effective_End_Date is not 12-31-9999 but in the future.  For those cases, we'll also 
            -- assume the employment period is ongoing
            -- CASE WHEN DATE(Effective_End_Date) > CURRENT_DATE THEN NULL
            --     ELSE DATE(Effective_End_Date) 
            --     END AS Effective_End_Date,
        FROM {dbo_EmploymentPeriod}
    )
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_pa",
    ingest_view_name="employment_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
