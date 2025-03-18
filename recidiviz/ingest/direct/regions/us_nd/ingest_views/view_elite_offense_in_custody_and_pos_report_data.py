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
"""Query containing offense in custody and positive report information.

Incident severity is assigned at the sanction level in ND rather than the incident level,
so this view associates the most high-level sanction given as a result of an incident
with the incident itself."""

from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.persistence.entity.state.entities import (
    STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override

VIEW_QUERY_TEMPLATE = f"""
WITH 
-- Collect all information available in raw data about each offense and sanction.
base AS (
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
FROM {{elite_offense_in_custody_and_pos_report_data}}
-- Exclude entries with malformed dates that make them appear to have occurred before 1900.
WHERE CAST(INCIDENT_DATE AS DATETIME) > '{STANDARD_DATE_FIELD_REASONABLE_LOWER_BOUND}'
)

SELECT DISTINCT 
    * EXCEPT(RESULT_OIC_OFFENCE_CATEGORY), 
    -- Some offenses have multiple sanction severities listed. Associate the offense
    -- with the highest severity sanction received as a result.
    FIRST_VALUE(RESULT_OIC_OFFENCE_CATEGORY) OVER (
        PARTITION BY AGENCY_INCIDENT_ID, OIC_INCIDENT_ID 
        ORDER BY CASE 
            WHEN RESULT_OIC_OFFENCE_CATEGORY IN ('MAJ','LVL3','LVL2E') THEN 1
            WHEN RESULT_OIC_OFFENCE_CATEGORY IN ('LVL3R','LVL1E','LVL2') THEN 3
            WHEN RESULT_OIC_OFFENCE_CATEGORY IN ('LVL1','LVL2R','MIN') THEN 4
            ELSE 5
        END,
        -- Choose the level with the highest numerical value within each group
        RESULT_OIC_OFFENCE_CATEGORY DESC) AS RESULT_OIC_OFFENCE_CATEGORY
FROM base
"""

VIEW_BUILDER = DirectIngestViewQueryBuilder(
    region="us_nd",
    ingest_view_name="elite_offense_in_custody_and_pos_report_data",
    view_query_template=VIEW_QUERY_TEMPLATE,
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        VIEW_BUILDER.build_and_print()
