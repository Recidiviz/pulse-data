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

from recidiviz.ingest.direct.views.direct_ingest_big_query_view_types import (
    DirectIngestPreProcessedIngestViewBuilder,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

# TODO(#2399): Update this query to query from raw data table once we can ingest
#  elite_offense_in_custody_and_pos_report_data.csv
VIEW_QUERY_TEMPLATE = """
SELECT 
    '' AS INCIDENT_TYPE,
    '' AS INCIDENT_DATE,
    '' AS INCIDENT_DETAILS,
    '' AS AGY_LOC_ID,
    '' AS ROOT_OFFENDER_ID,
    '' AS OFFENDER_BOOK_ID,
    '' AS OIC_SANCTION_CODE,
    '' AS OIC_SANCTION_DESC,
    '' AS EFFECTIVE_DATE,
    '' AS LAST_NAME,
    '' AS FIRST_NAME,
    '' AS OIC_INCIDENT_ID,
    '' AS AGENCY_INCIDENT_ID,
    '' AS INCIDENT_TYPE_DESC,
    '' AS OMS_OWNER_V_OIC_INCIDENTS_INT_LOC_DESCRIPTION,
    '' AS REPORT_DATE,
    '' AS OIC_HEARING_ID,
    '' AS OIC_HEARING_TYPE,
    '' AS OIC_HEARING_TYPE_DESC,
    '' AS HEARING_DATE,
    '' AS HEARING_STAFF_NAME,
    '' AS OMS_OWNER_V_OIC_HEARINGS_COMMENT_TEXT,
    '' AS OMS_OWNER_V_OIC_HEARINGS_INT_LOC_DESCRIPTION,
    '' AS OMS_OWNER_V_OIC_HEARING_RESULTS_RESULT_SEQ,
    '' AS OIC_OFFENCE_CATEGORY,
    '' AS OIC_OFFENCE_CODE,
    '' AS OIC_OFFENCE_DESCRIPTION,
    '' AS PLEA_DESCRIPTION,
    '' AS FINDING_DESCRIPTION,
    '' AS RESULT_OIC_OFFENCE_CATEGORY,
    '' AS RESULT_OIC_OFFENCE_CODE,
    '' AS RESULT_OIC_OFFENCE_DESCRIPTION,
    '' AS Expr1030,
    '' AS SANCTION_SEQ,
    '' AS COMPENSATION_AMOUNT,
    '' AS SANCTION_MONTHS,
    '' AS SANCTION_DAYS,
    '' AS OMS_OWNER_V_OFFENDER_OIC_SANCTIONS_COMMENT_TEXT,
    '' AS OMS_OWNER_V_OFFENDER_OIC_SANCTIONS_RESULT_SEQ,
    '' AS ALIAS_NAME_TYPE,    
"""

VIEW_BUILDER = DirectIngestPreProcessedIngestViewBuilder(
    region="us_nd",
    ingest_view_name="elite_offense_in_custody_and_pos_report_data",
    view_query_template=VIEW_QUERY_TEMPLATE,
    order_by_cols="ROOT_OFFENDER_ID",
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
