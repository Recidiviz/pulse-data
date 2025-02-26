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
"""Query that generates state staff supervisor periods information."""

from recidiviz.ingest.direct.regions.us_ix.ingest_views.query_fragments import (
    STATE_STAFF_SUPERVISOR_PERIODS_CTES,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
    WITH {STATE_STAFF_SUPERVISOR_PERIODS_CTES}
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY officer_EmployeeId ORDER BY start_date, end_date NULLS LAST) as period_id
    FROM final_supervisor_periods
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ix",
    ingest_view_name="state_staff_supervisor_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="officer_EmployeeId, period_id",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
