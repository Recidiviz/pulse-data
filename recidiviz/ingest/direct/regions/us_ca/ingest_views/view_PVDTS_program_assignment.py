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
"""Query containing CDCR program assignment information. For more information,
including to-do's and things we could UXR, see us_ca.md.
"""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
    WITH indeterminate_view as (
        SELECT
            OffenderId,
            PROVIDER_NAME,
            Actual_StartDate,
            Actual_EndDate,
            -- When there is no end date, the period is open
            Actual_EndDate is null as currently_assigned,
            -- TODO(#27200) This only helps resolve indeterminacy. We chose to keep the
            -- program they are currently active in (which we identify by it having a
            -- null end date) or the program were a part of the longest
            ROW_NUMBER() OVER (PARTITION BY OffenderId, PROVIDER_NAME, Actual_StartDate ORDER BY Actual_EndDate DESC NULLS FIRST, EmployReferredBy) as row_number
        FROM {PVDTS_TreatmentReferrals}
        WHERE 
            -- Filter out referrals in which we don't know the start date. Likely, lots
            -- of these aren't actually program starts, though we should ask about this
            -- in UXR.  
            Actual_StartDate is not null
    )
    SELECT             
        OffenderId,
        PROVIDER_NAME,
        Actual_StartDate,
        Actual_EndDate,
        currently_assigned,
    FROM indeterminate_view 
    WHERE row_number = 1
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ca",
    ingest_view_name="PVDTS_program_assignment",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="OffenderId",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
