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
    SELECT
        StaffId,
        FirstNm,
        MiddleNm,
        LastNm,
        Suffix,
        -- As of 4/16/2025, there are 43 staff records (mostly inactive) with an invalid email
        -- that would get NULLed out here.  We can fix with migrations on a case by case basis as needed.
        CASE 
            WHEN Email NOT LIKE '%@%' OR LOWER(Email) LIKE 'none@%'
                THEN NULL 
            ELSE REGEXP_REPLACE(Email, r'\\s+', '')
            END AS Email
    FROM {IA_DOC_MAINT_Staff}
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ia",
    ingest_view_name="state_staff",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
