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
"""Query containing offense in custody and POS report information."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = """
SELECT 
    ROOT_OFFENDER_ID,
    AGY_LOC_ID,
    OIC_INCIDENT_ID,
    INCIDENT_DATE,
    EFFECTIVE_DATE,
    INCIDENT_DETAILS,
    INCIDENT_TYPE,
    RESULT_OIC_OFFENCE_CATEGORY,
    OIC_SANCTION_CODE,
    OIC_SANCTION_DESC,
    AGENCY_INCIDENT_ID,
    OMS_OWNER_V_OIC_INCIDENTS___INT_LOC_DESCRIPTION,
    FINDING_DESCRIPTION,
    SANCTION_MONTHS,
    SANCTION_DAYS,
    SANCTION_SEQ
FROM {elite_offense_in_custody_and_pos_report_data}
-- Exclude entries with malformed dates that make them appear to have occurred before 1900.
WHERE CAST(INCIDENT_DATE AS DATETIME) > CAST('1900-01-01' AS DATETIME)
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="elite_offense_in_custody_and_pos_report_data",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
