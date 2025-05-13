# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Query that collects information about periods of assignment between case managers
and residents in UT facilities."""

from recidiviz.common.constants.reasonable_dates import (
    STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
)
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
SELECT
    case_wrkr_usr_id,
    ofndr_num,
    CAST(beg_dt AS DATETIME) AS beg_dt,
    CAST(end_dt AS DATETIME) as end_dt
FROM {{case_mgr_ofndr}}
WHERE beg_dt IS DISTINCT FROM end_dt
AND CAST(beg_dt AS DATETIME) BETWEEN '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}' and @update_timestamp
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_ut",
    ingest_view_name="incarceration_person_staff_relationship_period",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
