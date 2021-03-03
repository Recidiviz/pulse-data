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
"""Query that generates incarceration periods."""
from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.ingest.direct.regions.us_id.ingest_views.templates_periods import (
    get_all_periods_query_fragment,
    PeriodType,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH 
{get_all_periods_query_fragment(period_type=PeriodType.INCARCERATION)}

# Filter to just incarceration periods

SELECT
  # Living unit codes are not yet needed, so ignore them here. If
  * EXCEPT(lu_cd, lu_ldesc, wrkld_cat_title, empl_cd, empl_sdesc, empl_ldesc, empl_title, prev_loc_ldesc),
  ROW_NUMBER() 
    OVER (PARTITION BY docno ORDER BY start_date, end_date) AS period_id
FROM
  periods_with_previous_and_next_info
WHERE 
  fac_typ = 'I'                             # Facility type incarceration
# TODO(#3509): Consider tracking escape incarceration periods in the same way we track absconscion.
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_id",
    ingest_view_name="movement_facility_location_offstat_incarceration_periods",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="docno, incrno, start_date, end_date",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
